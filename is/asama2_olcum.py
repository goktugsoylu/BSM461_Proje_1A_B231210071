# -*- coding: utf-8 -*-
"""
BSM 461 — Aşama 2 (Depolama) Ölçüm Betiği · Grup B231210071 · Şablon 4 (haber)
Kapsam:
  - Veri sözlüğü (kaynaklar.csv) ve Z1 kesin JSON kayıt boyutu
  - Z2: JSON -> Parquet (snappy, zstd, gzip) boyut, yazma ve okuma süresi
  - Z4: S3 dosya düzeni (bölümsüz vs tarih=.../ vs tarih=.../ + repartition)
  - Z3: Bölüm anahtarı adayları, sıcak nokta (en dolu / ortalama) ve tuzlama
  - Z10: Kirli kayıt ve tekrar sayımı -> temiz & karantina tabloları
Çıktılar: kanit/olcumler.csv ve kanit/asama2-ozet.txt
"""
import csv
import json
import os
import re
import time
import urllib.request
from pyspark.sql import SparkSession, functions as F, types as T

HAM_KOK = "s3a://ham/haber/"
HAM_DESEN = "s3a://ham/haber/tarih=*/*.jsonl"
BOYUT_CSV = "s3a://ham/boyut/haber/kaynaklar.csv"
ISLENMIS_Z2 = "s3a://islenmis/haber_z2/"
ISLENMIS_Z4 = "s3a://islenmis/haber_z4/"
TEMIZ_YOL = "s3a://islenmis/haber_temiz/"
KARANTINA_YOL = "s3a://islenmis/haber_karantina/"
KANIT_DIZIN = "/kit/kanit"

SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType()),
    T.StructField("haber_id", T.StringType()),
    T.StructField("kaynak", T.StringType()),
    T.StructField("kategori", T.StringType()),
    T.StructField("baslik", T.StringType()),
    T.StructField("metin", T.StringType()),
])


def s3_boyut(spark, yol):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    it = fs.listFiles(p, True)
    adet = boyut = 0
    while it.hasNext():
        f = it.next()
        ad = f.getPath().getName()
        if not ad.startswith(("_", ".")) and "/_temporary/" not in f.getPath().toString():
            adet += 1
            boyut += f.getLen()
    return adet, boyut


def olc(fn, tekrar=1):
    sureler = []
    sonuc = None
    for _ in range(tekrar):
        t0 = time.perf_counter()
        sonuc = fn()
        sureler.append(time.perf_counter() - t0)
    return sonuc, sorted(sureler)[len(sureler) // 2], sureler


def son_tarama(spark):
    try:
        sc = spark.sparkContext
        time.sleep(0.8)
        url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/sql?details=true&planDescription=false&length=1000"
        veriler = json.load(urllib.request.urlopen(url))
        son = max(veriler, key=lambda e: e["id"])
        dosya = boyut = 0
        for dugum in son.get("nodes", []):
            if dugum["nodeName"].startswith("Scan"):
                for m in dugum.get("metrics", []):
                    deger = m["value"].split("\n")[0].replace(",", "")
                    if m["name"] == "number of files read":
                        dosya += int(float(deger.split()[0]))
                    elif m["name"] == "size of files read":
                        sayi, birim = deger.split()[:2]
                        boyut += float(sayi) * {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30}.get(birim, 1)
        return dosya, boyut
    except Exception:
        return -1, -1


def mb(b):
    return round(b / (2**20), 2)


def main():
    spark = SparkSession.builder.appName("bsm461-asama2-olcum").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    os.makedirs(KANIT_DIZIN, exist_ok=True)

    olcumler = []
    satirlar = []

    def ekle(bilesen, deney, metrik, deger, birim, notlar=""):
        olcumler.append({
            "asama": "Asama2_Depolama",
            "bilesen": bilesen,
            "deney": deney,
            "metrik": metrik,
            "deger": deger,
            "birim": birim,
            "notlar": notlar,
        })

    # ── 0. Boyut tablosu ve Z1 kesin JSON boyutu ─────────────────────────────
    satirlar.append("=== 0. BOYUT TABLOSU (kaynaklar.csv) VE HAM VERI ===")
    boyut_df = spark.read.option("header", True).csv(BOYUT_CSV)
    for r in boyut_df.collect():
        satirlar.append("  " + str(r.asDict()))

    n_json, b_json = s3_boyut(spark, HAM_KOK)
    ham_df = spark.read.schema(SEMA).json(HAM_DESEN)
    toplam_kayit = ham_df.count()
    kayit_bayt = round(b_json / toplam_kayit, 2)
    satirlar.append(
        f"Ham JSON: {n_json} dosya | {mb(b_json)} MB | {toplam_kayit:,} kayit | ortalama {kayit_bayt} bayt/kayit"
    )
    ekle("Z1", "ham_veri_olcumu", "ortalama_kayit_boyutu", kayit_bayt, "bayt", f"{toplam_kayit} kayit uzerinden")
    ekle("Z2", "dosya_bicimi", "json_toplam_boyut", mb(b_json), "MB", f"{n_json} dosya")

    # ── 1. Z2: JSON -> Parquet ve Kodek Karsilastirmasi ──────────────────────
    satirlar.append("\n=== 1. Z2: JSON -> PARQUET VE KODEK KARSILASTIRMASI ===")
    _, t_cikar, _ = olc(lambda: spark.read.json(HAM_DESEN).schema, 1)
    _, t_elle, _ = olc(lambda: spark.read.schema(SEMA).json(HAM_DESEN).schema, 1)
    satirlar.append(f"Sema cikarimi: {t_cikar:.2f} s  vs  Elle sema: {t_elle:.2f} s")
    ekle("Z2", "sema_okuma", "sema_cikarimi_suresi", round(t_cikar, 2), "s")
    ekle("Z2", "sema_okuma", "elle_sema_suresi", round(t_elle, 2), "s")

    # Ayni sorgu: kaynak basina haber sayisi (3 kez ortanca)
    sorgu_z2 = lambda df: df.groupBy("kaynak").count().collect()
    _, t_json_sorgu, _ = olc(lambda: sorgu_z2(spark.read.schema(SEMA).json(HAM_DESEN)), 3)
    satirlar.append(f"Sorgu suresi (JSON, 3 tur ortanca): {t_json_sorgu:.2f} s")
    ekle("Z2", "sorgu_suresi", "json_sorgu_ortanca", round(t_json_sorgu, 2), "s", "groupBy(kaynak).count(), 3 tur")

    satirlar.append(f"{'Bicim/Kodek':<16}{'Dosya':>7}{'Boyut(MB)':>12}{'JSON/P':>9}{'Yazma(s)':>10}{'Sorgu(s)':>10}")
    satirlar.append(f"{'JSON (ham)':<16}{n_json:>7}{mb(b_json):>12.2f}{'1.00':>9}{'-':>10}{t_json_sorgu:>10.2f}")

    for kodek in ["snappy", "zstd", "gzip"]:
        yol = f"{ISLENMIS_Z2}kodek={kodek}/"
        _, t_yaz, _ = olc(
            lambda k=kodek, y=yol: ham_df.coalesce(8).write.mode("overwrite").option("compression", k).parquet(y),
            1,
        )
        n_k, b_k = s3_boyut(spark, yol)
        oran = round(b_json / b_k, 2)
        _, t_oku, _ = olc(lambda y=yol: sorgu_z2(spark.read.parquet(y)), 3)
        satirlar.append(f"{'Parquet-' + kodek:<16}{n_k:>7}{mb(b_k):>12.2f}{oran:>9.2f}{t_yaz:>10.2f}{t_oku:>10.2f}")
        ekle("Z2", f"parquet_{kodek}", "toplam_boyut", mb(b_k), "MB", f"{n_k} dosya, oran {oran}x")
        ekle("Z2", f"parquet_{kodek}", "yazma_suresi", round(t_yaz, 2), "s")
        ekle("Z2", f"parquet_{kodek}", "sorgu_ortanca", round(t_oku, 2), "s", "groupBy(kaynak).count(), 3 tur")

    # ── 2. Z4: S3 Dosya Duzeni ve Kucuk Dosya Analizi ────────────────────────
    satirlar.append("\n=== 2. Z4: S3 DOSYA DUZENI VE KUCUK DOSYA ANALIZI ===")
    snappy_df = spark.read.parquet(f"{ISLENMIS_Z2}kodek=snappy/").withColumn(
        "tarih", F.to_date(F.to_timestamp("olay_zamani"))
    )
    GUN = "2026-11-18"
    duzenler = {
        "a_bolumsuz": f"{ISLENMIS_Z2}kodek=snappy/",
        "b_tarih_birlestirmesiz": f"{ISLENMIS_Z4}duzen_b/",
        "c_tarih_repartition": f"{ISLENMIS_Z4}duzen_c/",
    }
    snappy_df.coalesce(6).write.mode("overwrite").partitionBy("tarih").parquet(duzenler["b_tarih_birlestirmesiz"])
    snappy_df.repartition("tarih").write.mode("overwrite").partitionBy("tarih").parquet(duzenler["c_tarih_repartition"])

    satirlar.append(
        f"{'Duzen':<25}{'Dosya':>7}{'Boyut(MB)':>11}{'Ort(MB)':>9}{'OkunanDosya':>13}{'Okunan(MB)':>12}{'Sure(s)':>9}"
    )
    for ad, yol in duzenler.items():
        n_d, b_d = s3_boyut(spark, yol)
        ort_mb = round((b_d / n_d) / (2**20), 2) if n_d else 0

        def tek_gun_sorgu(y=yol):
            t = spark.read.parquet(y)
            if "tarih" not in t.columns:
                t = t.withColumn("tarih", F.to_date(F.to_timestamp("olay_zamani")))
            return t.filter(F.col("tarih") == GUN).groupBy("kaynak").count()

        _, t_gun, _ = olc(lambda: tek_gun_sorgu().collect(), 3)
        n_oku, b_oku = son_tarama(spark)
        plan_txt = tek_gun_sorgu()._jdf.queryExecution().executedPlan().toString()
        pf = re.search(r"PartitionFilters: \[([^\]]*)\]", plan_txt)
        pf_str = re.sub(r"#\d+", "", pf.group(1)) if pf and pf.group(1) else "yok"

        satirlar.append(
            f"{ad:<25}{n_d:>7}{mb(b_d):>11.2f}{ort_mb:>9.2f}{n_oku:>13}{mb(b_oku):>12.2f}{t_gun:>9.2f}  | PF: {pf_str}"
        )
        ekle("Z4", ad, "dosya_sayisi", n_d, "adet", f"ort {ort_mb} MB")
        ekle("Z4", ad, "tek_gun_sorgu_suresi", round(t_gun, 2), "s", f"okunan {n_oku} dosya, {mb(b_oku)} MB")

    # ── 3. Z3: Bolum Anahtari ve Sicak Nokta Olcumu ──────────────────────────
    satirlar.append("\n=== 3. Z3: BOLUM ANAHTARI VE SICAK NOKTA (K=8 BOLUM) ===")
    K = 8
    z3_df = snappy_df
    satirlar.append(f"{'Anahtar':<18}{'Farkli':>10}{'EnBuyukDeger':<20}{'Pay(%)':>8}{'Max/Ort':>10}")
    kaynak_oran = 0.0
    sicak_kaynak = None

    for anahtar in ["kaynak", "kategori", "tarih", "haber_id"]:
        sayim = z3_df.groupBy(anahtar).count()
        farkli = sayim.count()
        ust = sayim.orderBy(F.desc("count")).first()
        pay = round(ust["count"] / toplam_kayit * 100, 2)
        bolum = (
            z3_df.groupBy(F.pmod(F.hash(F.col(anahtar)), F.lit(K)).alias("b"))
            .count()
            .agg(F.max("count"), F.avg("count"))
            .first()
        )
        oran = round(bolum[0] / bolum[1], 2)
        if anahtar == "kaynak":
            kaynak_oran = oran
            sicak_kaynak = ust[0]
        satirlar.append(f"{anahtar:<18}{farkli:>10,}{str(ust[0]):<20}{pay:>8.2f}{oran:>10.2f}")
        ekle("Z3", f"anahtar_{anahtar}", "en_dolu_bolu_ortalama", oran, "oran", f"en buyuk {ust[0]} (%{pay})")

    # Cozum 1: Tuzlama (sicak kaynak 16 alt anahtara)
    tuzlu_df = z3_df.withColumn(
        "kaynak_tuzlu",
        F.when(
            F.col("kaynak") == sicak_kaynak,
            F.concat(F.col("kaynak"), F.lit("#"), (F.rand(461) * 16).cast("int").cast("string")),
        ).otherwise(F.col("kaynak")),
    )
    b_tuz = (
        tuzlu_df.groupBy(F.pmod(F.hash("kaynak_tuzlu"), F.lit(K)))
        .count()
        .agg(F.max("count"), F.avg("count"))
        .first()
    )
    oran_tuz = round(b_tuz[0] / b_tuz[1], 2)

    # Cozum 2: Bilesik anahtar (kaynak + tarih)
    bilesik_df = z3_df.withColumn("kaynak_tarih", F.concat_ws("|", F.col("kaynak"), F.col("tarih").cast("string")))
    b_bil = (
        bilesik_df.groupBy(F.pmod(F.hash("kaynak_tarih"), F.lit(K)))
        .count()
        .agg(F.max("count"), F.avg("count"))
        .first()
    )
    oran_bil = round(b_bil[0] / b_bil[1], 2)

    satirlar.append(f"Tuzlama ({sicak_kaynak}#0..15): kaynak Max/Ort {kaynak_oran:.2f} -> {oran_tuz:.2f}")
    satirlar.append(f"Bilesik anahtar (kaynak|tarih): Max/Ort {kaynak_oran:.2f} -> {oran_bil:.2f}")
    ekle("Z3", "sicak_nokta_cozum_tuzlama", "en_dolu_bolu_ortalama", oran_tuz, "oran", f"once {kaynak_oran}")
    ekle("Z3", "sicak_nokta_cozum_bilesik", "en_dolu_bolu_ortalama", oran_bil, "oran", "kaynak|tarih")

    # ── 4. Z10: Kirli Veri ve Tekrar Sayimi + Temiz Tablo ────────────────────
    satirlar.append("\n=== 4. Z10: KIRLI VERI VE TEKRAR SAYIMI ===")
    ham_p = spark.read.parquet(f"{ISLENMIS_Z2}kodek=snappy/")
    zaman_col = F.to_timestamp("olay_zamani")

    bos_kosul = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")
    kusurlar = {
        "tekrar_eden_haber_id": toplam_kayit - ham_p.dropDuplicates(["haber_id"]).count(),
        "null_veya_bozuk_olay_zamani": ham_p.filter(zaman_col.isNull()).count(),
        "null_veya_bos_haber_id": ham_p.filter(bos_kosul("haber_id")).count(),
        "null_veya_bos_kaynak": ham_p.filter(bos_kosul("kaynak")).count(),
        "null_veya_bos_kategori": ham_p.filter(bos_kosul("kategori")).count(),
        "null_veya_bos_baslik": ham_p.filter(bos_kosul("baslik")).count(),
        "null_veya_bos_metin": ham_p.filter(bos_kosul("metin")).count(),
    }
    satirlar.append(f"{'Kusur Turu':<32}{'Sayi':>10}{'Oran(%)':>10}")
    for k, v in kusurlar.items():
        yuzde = round(v / toplam_kayit * 100, 3)
        satirlar.append(f"{k:<32}{v:>10,}{yuzde:>10.3f}")
        ekle("Z10", "kirli_veri", k, v, "kayit", f"%{yuzde}")

    # Ornek kirli ve tekrar kayitlar
    kirli_filtre = (
        zaman_col.isNull()
        | bos_kosul("haber_id")
        | bos_kosul("kaynak")
        | bos_kosul("kategori")
        | bos_kosul("baslik")
        | bos_kosul("metin")
    )
    satirlar.append("\nOrnek 3 kirli kayit:")
    for r in ham_p.filter(kirli_filtre).limit(3).collect():
        satirlar.append("  " + str(r.asDict()))

    tekrar_id = (
        ham_p.filter(F.col("haber_id").isNotNull())
        .groupBy("haber_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(2)
        .collect()
    )
    satirlar.append("Ornek tekrar eden haber_id'ler: " + str([(r["haber_id"], r["count"]) for r in tekrar_id]))

    # Temizleme karari:
    # 1) Tekrar eden haber_id'leri tekillestir (idempotentlik)
    # 2) Zorunlu alanlari eksik/bozuk olanlari karantinaya ayir
    # 3) Temiz veriyi tarih=.../ + repartition ile yaz
    tekil = ham_p.dropDuplicates(["haber_id"]).withColumn("olay_ts", zaman_col)
    karantina_filtre = (
        F.col("olay_ts").isNull()
        | bos_kosul("haber_id")
        | bos_kosul("kaynak")
        | bos_kosul("kategori")
        | bos_kosul("baslik")
        | bos_kosul("metin")
    )
    karantina = tekil.filter(karantina_filtre)
    temiz = (
        tekil.filter(~karantina_filtre)
        .withColumn("tarih", F.to_date("olay_ts"))
        .withColumn("saat", F.hour("olay_ts"))
    )

    karantina.coalesce(1).write.mode("overwrite").parquet(KARANTINA_YOL)
    temiz.repartition("tarih").write.mode("overwrite").option("compression", "zstd").partitionBy("tarih").parquet(TEMIZ_YOL)

    n_temiz_satir = spark.read.parquet(TEMIZ_YOL).count()
    n_kar_satir = spark.read.parquet(KARANTINA_YOL).count()
    n_t_dosya, b_t_boyut = s3_boyut(spark, TEMIZ_YOL)

    satirlar.append(
        f"\nTemiz tablo  : {n_temiz_satir:,} kayit | {n_t_dosya} dosya | {mb(b_t_boyut)} MB (zstd) -> {TEMIZ_YOL}"
    )
    satirlar.append(f"Karantina    : {n_kar_satir:,} kayit -> {KARANTINA_YOL}")
    ekle("Z10", "temizleme_sonucu", "temiz_kayit_sayisi", n_temiz_satir, "kayit", f"{n_t_dosya} dosya, {mb(b_t_boyut)} MB")
    ekle("Z10", "temizleme_sonucu", "karantina_kayit_sayisi", n_kar_satir, "kayit", "eksik/bozuk alanlar")

    # ── Kanit dosyalarini yaz ────────────────────────────────────────────────
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    with open(csv_yol, "w", newline="", encoding="utf-8") as f:
        yazici = csv.DictWriter(f, fieldnames=["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"])
        yazici.writeheader()
        yazici.writerows(olcumler)

    ozet_metin = "\n".join(satirlar)
    ozet_yol = os.path.join(KANIT_DIZIN, "asama2-ozet.txt")
    with open(ozet_yol, "w", encoding="utf-8") as f:
        f.write(ozet_metin)

    print(ozet_metin)
    print(f"\n✓ Kaydedildi: {csv_yol} ve {ozet_yol}")
    spark.stop()


if __name__ == "__main__":
    main()