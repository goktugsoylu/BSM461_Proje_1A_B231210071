# -*- coding: utf-8 -*-
"""
BSM 461 — Aşama 3 (Toplu Hat · Z5) · Grup B231210071 · Şablon 4 (haber)
Çalıştırma:
  docker compose exec spark spark-submit is/toplu_is.py
  docker compose exec spark spark-submit is/toplu_is.py --bekle 300   # Spark UI: localhost:4040
"""
import argparse
import csv
import json
import os
import re
import time
import urllib.request
from pyspark.sql import SparkSession, functions as F, types as T

HAM_DESEN = "s3a://ham/haber/tarih=*/*.jsonl"
BOYUT_CSV = "s3a://ham/boyut/haber/kaynaklar.csv"
TEMIZ_YOL = "s3a://islenmis/haber_temiz/"
KARANTINA_YOL = "s3a://islenmis/haber_karantina/"
OZET_S1_YOL = "s3a://islenmis/haber_ozet/s1_kaynak/"
OZET_S2_GUN_YOL = "s3a://islenmis/haber_ozet/s2_gunluk/"
OZET_S2_SAAT_YOL = "s3a://islenmis/haber_ozet/s2_saatlik/"
KANIT_DIZIN = "/kit/kanit"

SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType()),
    T.StructField("haber_id", T.StringType()),
    T.StructField("kaynak", T.StringType()),
    T.StructField("kategori", T.StringType()),
    T.StructField("baslik", T.StringType()),
    T.StructField("metin", T.StringType()),
])


def s3_var_mi(spark, yol):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(p)


def temiz_veriyi_hazirla(spark):
    """Aşama 2'de üretilen temiz tablo varsa doğrudan okur; yoksa sıfırdan temizleyip yazar."""
    if not s3_var_mi(spark, TEMIZ_YOL):
        ham = spark.read.schema(SEMA).json(HAM_DESEN)
        bos = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")
        tekil = ham.dropDuplicates(["haber_id"]).withColumn("olay_ts", F.to_timestamp("olay_zamani"))
        kirli = (
            F.col("olay_ts").isNull()
            | bos("haber_id")
            | bos("kaynak")
            | bos("kategori")
            | bos("baslik")
            | bos("metin")
        )
        tekil.filter(kirli).coalesce(1).write.mode("overwrite").parquet(KARANTINA_YOL)
        (
            tekil.filter(~kirli)
            .withColumn("tarih", F.to_date("olay_ts"))
            .withColumn("saat", F.hour("olay_ts"))
            .repartition("tarih")
            .write.mode("overwrite")
            .option("compression", "zstd")
            .partitionBy("tarih")
            .parquet(TEMIZ_YOL)
        )
    return spark.read.parquet(TEMIZ_YOL)


def olc(fn, tekrar=3):
    sureler = []
    sonuc = None
    for _ in range(tekrar):
        t0 = time.perf_counter()
        sonuc = fn()
        sureler.append(time.perf_counter() - t0)
    ort = sum(sureler) / len(sureler)
    ortanca = sorted(sureler)[len(sureler) // 2]
    return sonuc, ort, ortanca, sureler


def son_tarama(spark):
    try:
        sc = spark.sparkContext
        time.sleep(0.6)
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
        return dosya, round(boyut / (2**20), 2)
    except Exception:
        return -1, -1.0


def csv_yaz(yol, alanlar, satirlar):
    with open(yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar)
        w.writeheader()
        w.writerows(satirlar)


def olcumler_csv_guncelle(yeni_satirlar):
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    alanlar = ["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"]
    mevcut = []
    if os.path.exists(csv_yol):
        with open(csv_yol, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("asama") != "Asama3_TopluHat":
                    mevcut.append(r)
    csv_yaz(csv_yol, alanlar, mevcut + yeni_satirlar)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bekle", type=int, default=0, help="Spark UI için sonda kaç saniye beklensin")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bsm461-asama3-toplu-hat").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    os.makedirs(KANIT_DIZIN, exist_ok=True)

    temiz = temiz_veriyi_hazirla(spark)
    boyut = spark.read.option("header", True).csv(BOYUT_CSV)

    rapor = []
    yeni_olcumler = []

    def ekle(bilesen, deney, metrik, deger, birim, notlar=""):
        yeni_olcumler.append({
            "asama": "Asama3_TopluHat",
            "bilesen": bilesen,
            "deney": deney,
            "metrik": metrik,
            "deger": deger,
            "birim": birim,
            "notlar": notlar,
        })

    # ── TOPLU SORU 1: Kaynak karşılaştırması (kaynaklar.csv ile JOIN) ────────
    gunluk_kaynak = temiz.groupBy("kaynak", "tarih").agg(
        F.count("*").alias("gunluk_haber"),
        F.countDistinct("kategori").alias("kategori_cesit"),
    )
    s1_df = (
        gunluk_kaynak.groupBy("kaynak")
        .agg(
            F.sum("gunluk_haber").alias("toplam_haber"),
            F.round(F.avg("gunluk_haber"), 1).alias("gunluk_ort_haber"),
            F.round(F.avg("kategori_cesit"), 2).alias("ort_kategori_cesidi"),
        )
        .join(boyut, "kaynak")
        .select("kaynak", "tur", "sehir", "kurulus_yili", "toplam_haber", "gunluk_ort_haber", "ort_kategori_cesidi")
        .orderBy(F.desc("toplam_haber"))
    )
    s1_satirlar = [r.asDict() for r in s1_df.collect()]
    s1_df.coalesce(1).write.mode("overwrite").parquet(OZET_S1_YOL)
    csv_yaz(
        os.path.join(KANIT_DIZIN, "veri_s1_kaynak.csv"),
        ["kaynak", "tur", "sehir", "kurulus_yili", "toplam_haber", "gunluk_ort_haber", "ort_kategori_cesidi"],
        s1_satirlar,
    )

    rapor.append("=== TOPLU SORU 1: KAYNAK KARSILASTIRMASI (kaynaklar.csv JOIN) ===")
    rapor.append(f"{'Kaynak':<16}{'Tur':<9}{'Sehir':<11}{'Toplam':>10}{'GunlukOrt':>12}{'KatCesit':>10}")
    for r in s1_satirlar:
        rapor.append(
            f"{r['kaynak']:<16}{r['tur']:<9}{r['sehir']:<11}{r['toplam_haber']:>10,}"
            f"{r['gunluk_ort_haber']:>12,.1f}{r['ort_kategori_cesidi']:>10.2f}"
        )

    # ── TOPLU SORU 2: Zaman içindeki seyir (Günlük kategori + Saatlik yoğunluk)
    s2_gun_df = (
        temiz.groupBy("tarih", "kategori")
        .agg(F.count("*").alias("haber_sayisi"))
        .orderBy("tarih", F.desc("haber_sayisi"))
    )
    s2_gun_satirlar = [
        {"tarih": str(r["tarih"]), "kategori": r["kategori"], "haber_sayisi": r["haber_sayisi"]}
        for r in s2_gun_df.collect()
    ]
    s2_gun_df.coalesce(1).write.mode("overwrite").parquet(OZET_S2_GUN_YOL)
    csv_yaz(os.path.join(KANIT_DIZIN, "veri_s2_gunluk.csv"), ["tarih", "kategori", "haber_sayisi"], s2_gun_satirlar)

    s2_saat_df = (
        temiz.groupBy("saat")
        .agg(F.count("*").alias("haber_sayisi"))
        .orderBy("saat")
    )
    s2_saat_satirlar = [r.asDict() for r in s2_saat_df.collect()]
    s2_saat_df.coalesce(1).write.mode("overwrite").parquet(OZET_S2_SAAT_YOL)
    csv_yaz(os.path.join(KANIT_DIZIN, "veri_s2_saatlik.csv"), ["saat", "haber_sayisi"], s2_saat_satirlar)

    rapor.append("\n=== TOPLU SORU 2A: GUNLUK KATEGORI SEYRI (EN YUKSEK 10 GUN-KATEGORI) ===")
    for r in sorted(s2_gun_satirlar, key=lambda x: x["haber_sayisi"], reverse=True)[:10]:
        rapor.append(f"  {r['tarih']}  {r['kategori']:<12}  {r['haber_sayisi']:>8,}")

    rapor.append("\n=== TOPLU SORU 2B: SAATLIK HABER DAGILIMI (00-23) ===")
    for r in s2_saat_satirlar:
        rapor.append(f"  Saat {r['saat']:02d}:00  ->  {r['haber_sayisi']:>8,}")

    # ── PERFORMANS KARARI + EXPLAIN (Son 7 gün, gündüz 08-18, JOIN + GROUPBY) ─
    def sorgu_perf():
        t = spark.read.parquet(TEMIZ_YOL)
        b = spark.read.option("header", True).csv(BOYUT_CSV)
        return (
            t.filter((F.col("tarih") >= "2026-11-18") & F.col("saat").between(8, 18))
            .join(b, "kaynak")
            .groupBy("tur", "sehir", "kategori")
            .agg(F.count("*").alias("haber_sayisi"))
            .orderBy(F.desc("haber_sayisi"))
        )

    # 1) explain(mode="formatted") — En iyi ayarda (Broadcast açık, 8 bölüm)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760")

    plan_str = spark._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.explainString(
        sorgu_perf()._jdf.queryExecution(), "formatted"
    )
    explain_yol = os.path.join(KANIT_DIZIN, "explain-1.txt")
    with open(explain_yol, "w", encoding="utf-8") as f:
        f.write(plan_str)

    rapor.append("\n=== EXPLAIN(mode='formatted') OZETI (kanit/explain-1.txt) ===")
    rapor.append(plan_str.split("\n\n")[0])
    for satir in plan_str.splitlines():
        if satir.startswith(("PartitionFilters", "PushedFilters", "ReadSchema")):
            rapor.append("  " + re.sub(r"#\d+", "", satir))

    # 2) Ölçümlü Performans Kararı (Aynı temiz veri üzerinde, her ayar 3 kez)
    rapor.append("\n=== Z5 PERFORMANS KARARI (AYNI VERI, 3 TUR ORTALAMA & ORTANCA) ===")
    rapor.append(f"{'Ayar':<42}{'Ort(s)':>8}{'Ortanca(s)':>12}{'JoinStratejisi':>19}{'Dosya':>7}{'Okunan(MB)':>12}")

    deneyler = [
        ("1_Once_SortMerge_200bolum", "200", "-1"),
        ("2_Ara_SortMerge_8bolum", "8", "-1"),
        ("3_Sonra_Broadcast_8bolum", "8", "10485760"),
    ]
    for ad, shuf_p, bc_esik in deneyler:
        spark.conf.set("spark.sql.shuffle.partitions", shuf_p)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", bc_esik)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", bc_esik)

        _, ort, ortanca, ham_sureler = olc(lambda: sorgu_perf().collect(), tekrar=3)
        n_oku, mb_oku = son_tarama(spark)
        plan_deney = sorgu_perf()._jdf.queryExecution().executedPlan().toString()
        join_es = re.findall(r"(BroadcastHashJoin|SortMergeJoin)", plan_deney)
        join_tur = join_es[0] if join_es else "?"
        sure_listesi = ", ".join(f"{x:.2f}" for x in ham_sureler)

        rapor.append(
            f"{ad:<42}{ort:>8.2f}{ortanca:>12.2f}{join_tur:>19}{n_oku:>7}{mb_oku:>12.2f}   [{sure_listesi}]"
        )
        ekle("Z5", ad, "ortalama_sure", round(ort, 2), "s", f"3 tur: [{sure_listesi}], {join_tur}")
        ekle("Z5", ad, "ortanca_sure", round(ortanca, 2), "s", f"{n_oku} dosya, {mb_oku} MB")

    # Ayarları en iyi hâline geri getir
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760")

    olcumler_csv_guncelle(yeni_olcumler)

    ozet_metin = "\n".join(rapor)
    ozet_yol = os.path.join(KANIT_DIZIN, "asama3-ozet.txt")
    with open(ozet_yol, "w", encoding="utf-8") as f:
        f.write(ozet_metin)

    print(ozet_metin)
    print(f"\n✓ Kaydedildi: {explain_yol}, {ozet_yol} ve /kit/kanit/olcumler.csv (güncellendi)")

    if args.bekle > 0:
        print(f"\nSpark UI açık: http://localhost:4040 — {args.bekle} saniye bekleniyor (bitirmek için Ctrl+C)")
        time.sleep(args.bekle)

    spark.stop()


if __name__ == "__main__":
    main()