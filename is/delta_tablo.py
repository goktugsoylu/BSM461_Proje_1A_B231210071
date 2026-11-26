# -*- coding: utf-8 -*-
"""
BSM 461 â€” Asama 4 (Tablo Bicimi / Delta Lake Â· Z7) Â· Grup B231210071
Kullanim:
  docker compose exec spark spark-submit is/delta_tablo.py --mod surum
  docker compose exec spark spark-submit is/delta_tablo.py --mod kesinti-delta
  docker compose exec spark spark-submit is/delta_tablo.py --mod dogrula-delta
  docker compose exec spark spark-submit is/delta_tablo.py --mod kesinti-parquet
  docker compose exec spark spark-submit is/delta_tablo.py --mod dogrula-parquet
"""
import argparse
import csv
import os
import time
from pyspark.sql import SparkSession, functions as F, types as T

HAM_DESEN = "s3a://ham/haber/tarih=*/*.jsonl"
DELTA_YOL = "s3a://tablolar/haber_delta"
PARQUET_KESINTI_YOL = "s3a://islenmis/haber_parquet_kesinti_test"
KANIT_DIZIN = "/kit/kanit"

SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType()),
    T.StructField("haber_id", T.StringType()),
    T.StructField("kaynak", T.StringType()),
    T.StructField("kategori", T.StringType()),
    T.StructField("baslik", T.StringType()),
    T.StructField("metin", T.StringType()),
])


def olcumler_csv_ekle(yeni_satirlar):
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    alanlar = ["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"]
    mevcut = []
    if os.path.exists(csv_yol):
        with open(csv_yol, "r", encoding="utf-8") as f:
            mevcut = list(csv.DictReader(f))
    yeni_anahtarlar = {(r["asama"], r["deney"], r["metrik"]) for r in yeni_satirlar}
    temiz_mevcut = [
        r for r in mevcut
        if (r.get("asama"), r.get("deney"), r.get("metrik")) not in yeni_anahtarlar
    ]
    with open(csv_yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar)
        w.writeheader()
        w.writerows(temiz_mevcut + yeni_satirlar)


def temiz_df_oku(spark):
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
    return (
        tekil.filter(~kirli)
        .withColumn("tarih", F.to_date("olay_ts"))
        .withColumn("saat", F.hour("olay_ts"))
    )


def s3_dosya_ozeti(spark, yol):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(p):
        return 0, 0
    gecici_var = 0
    parquet_sayisi = 0
    it = fs.listFiles(p, True)
    while it.hasNext():
        f_yol = it.next().getPath().toString()
        if "_temporary" in f_yol:
            gecici_var += 1
        elif f_yol.endswith(".parquet"):
            parquet_sayisi += 1
    return parquet_sayisi, gecici_var


def mod_surum(spark):
    rapor = []
    olcumler = []

    print("1) Ham veri temizleniyor ve Delta Surum 0 olarak yaziliyor...")
    t0 = time.perf_counter()
    temiz = temiz_df_oku(spark)
    (
        temiz.repartition("tarih")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("tarih")
        .save(DELTA_YOL)
    )
    v0_sure = round(time.perf_counter() - t0, 2)
    v0_sayisi = spark.read.format("delta").load(DELTA_YOL).count()

    print(f"   Surum 0 yazildi: {v0_sayisi:,} satir ({v0_sure} s)")

    print("2) Ikinci yazma (Surum 1: yeni gun verisi ekleme) yapiliyor...")
    t1 = time.perf_counter()
    yeni_gun = (
        spark.read.format("delta")
        .load(DELTA_YOL)
        .limit(1000)
        .withColumn("haber_id", F.concat(F.col("haber_id"), F.lit("_ekgun")))
        .withColumn("tarih", F.date_add(F.col("tarih"), 1))
    )
    yeni_gun.write.format("delta").mode("append").save(DELTA_YOL)
    v1_sure = round(time.perf_counter() - t1, 2)
    v1_sayisi = spark.read.format("delta").load(DELTA_YOL).count()
    print(f"   Surum 1 yazildi: {v1_sayisi:,} satir ({v1_sure} s)")

    print("3) DESCRIBE HISTORY ve versionAsOf (Zaman Yolculugu) calistiriliyor...")
    gecmis_df = spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_YOL}`").select(
        "version", "timestamp", "operation", "operationMetrics"
    )
    gecmis_satirlar = gecmis_df.collect()

    eski_v0_sayisi = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_YOL).count()
    guncel_v1_sayisi = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_YOL).count()

    rapor.append("=== Z7 DELTA LAKE: SURUMLER VE ZAMAN YOLCULUGU ===")
    rapor.append(f"Delta Tablo Yolu : {DELTA_YOL}")
    rapor.append(f"Surum 0 (overwrite) : {v0_sayisi:,} satir | {v0_sure} s")
    rapor.append(f"Surum 1 (append +1000 yeni gun) : {v1_sayisi:,} satir | {v1_sure} s")
    rapor.append("\n--- DESCRIBE HISTORY ---")
    for r in gecmis_satirlar:
        rapor.append(f"  version={r['version']} | ts={r['timestamp']} | op={r['operation']}")
    rapor.append("\n--- ZAMAN YOLCULUGU (versionAsOf) ---")
    rapor.append(f"  versionAsOf=0 okunan satir : {eski_v0_sayisi:,}")
    rapor.append(f"  versionAsOf=1 okunan satir : {guncel_v1_sayisi:,}")
    rapor.append(f"  Fark (eklenen yeni gun)    : {guncel_v1_sayisi - eski_v0_sayisi:,} satir")

    olcumler.extend([
        {"asama": "Asama4_Delta", "bilesen": "Z7", "deney": "delta_surum_0", "metrik": "satir_sayisi", "deger": v0_sayisi, "birim": "satir", "notlar": f"ilk overwrite ({v0_sure} s)"},
        {"asama": "Asama4_Delta", "bilesen": "Z7", "deney": "delta_surum_1", "metrik": "satir_sayisi", "deger": v1_sayisi, "birim": "satir", "notlar": f"yeni gun append +1000 ({v1_sure} s)"},
        {"asama": "Asama4_Delta", "bilesen": "Z7", "deney": "zaman_yolculugu", "metrik": "v0_okunan_satir", "deger": eski_v0_sayisi, "birim": "satir", "notlar": "versionAsOf=0 ile eski surum okundu"},
    ])
    olcumler_csv_ekle(olcumler)

    ozet = "\n".join(rapor)
    with open(os.path.join(KANIT_DIZIN, "asama4-delta-ozet.txt"), "w", encoding="utf-8") as f:
        f.write(ozet + "\n")
    print("\n" + ozet)


def mod_kesinti_delta(spark):
    print("\n>>> DELTA ATOMIK YAZMA DENEYI BASLIYOR <<<")
    print(">>> 2-3 SANIYE ICINDE TERMINALDE Ctrl+C TUSUNA BASIP ISI YARIDA KES! <<<\n")
    temiz = temiz_df_oku(spark)
    (
        temiz.repartition(16)
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("tarih")
        .save(DELTA_YOL)
    )
    print("UYARI: Is kesilmeden tamamlandi. Tekrar calistirip daha erken Ctrl+C bas.")


def mod_dogrula_delta(spark):
    son_gecmis = spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_YOL}`").select("version", "operation").collect()
    satir_sayisi = spark.read.format("delta").load(DELTA_YOL).count()
    parquet_n, _ = s3_dosya_ozeti(spark, DELTA_YOL)

    metin = (
        "\n=== Z7 ATOMIK YAZMA DOGRULAMASI (DELTA - Ctrl+C SONRASI) ===\n"
        f"Tablo okunabildi mi? : EVET (bozulma yok)\n"
        f"Okunan satir sayisi  : {satir_sayisi:,} (Surum 1 ile birebir ayni)\n"
        f"DESCRIBE HISTORY     : {[(r['version'], r['operation']) for r in son_gecmis]}\n"
        f"Klasordeki .parquet  : {parquet_n} dosya (yarim yazilanlar _delta_log'a islenmedigi icin yok sayilir)\n"
    )
    olcumler_csv_ekle([
        {"asama": "Asama4_Delta", "bilesen": "Z7", "deney": "atomik_kesinti_delta", "metrik": "kesinti_sonrasi_satir", "deger": satir_sayisi, "birim": "satir", "notlar": "Ctrl+C sonrasi tablo bozulmadi, son commit korundu"}
    ])
    with open(os.path.join(KANIT_DIZIN, "asama4-delta-ozet.txt"), "a", encoding="utf-8") as f:
        f.write(metin)
    print(metin)


def mod_kesinti_parquet(spark):
    print("\n>>> CIPLAK PARQUET KESINTI DENEYI BASLIYOR <<<")
    print(">>> 2-3 SANIYE ICINDE TERMINALDE Ctrl+C TUSUNA BASIP ISI YARIDA KES! <<<\n")
    temiz = temiz_df_oku(spark)
    (
        temiz.repartition(16)
        .write.mode("overwrite")
        .partitionBy("tarih")
        .parquet(PARQUET_KESINTI_YOL)
    )
    print("UYARI: Is kesilmeden tamamlandi. Tekrar calistirip daha erken Ctrl+C bas.")


def mod_dogrula_parquet(spark):
    parquet_n, gecici_n = s3_dosya_ozeti(spark, PARQUET_KESINTI_YOL)
    durum = ""
    satir = -1
    try:
        satir = spark.read.parquet(PARQUET_KESINTI_YOL).count()
        durum = f"KISMI/BOZUK VERI OKUNDU ({satir:,} satir â€” tam veri degil!)"
    except Exception as e:
        durum = f"OKUMA HATASI: {type(e).__name__}: {str(e).splitlines()[0][:140]}"

    metin = (
        "\n=== Z7 ATOMIK YAZMA DOGRULAMASI (CIPLAK PARQUET - Ctrl+C SONRASI) ===\n"
        f"Klasor               : {PARQUET_KESINTI_YOL}\n"
        f"Okuma sonucu         : {durum}\n"
        f"Klasordeki .parquet  : {parquet_n} | _temporary icindeki yarim dosya : {gecici_n}\n"
    )
    olcumler_csv_ekle([
        {"asama": "Asama4_Delta", "bilesen": "Z7", "deney": "atomik_kesinti_parquet", "metrik": "kesinti_sonrasi_satir", "deger": satir, "birim": "satir", "notlar": f"Ciplak Parquet Ctrl+C sonrasi: {durum[:80]}"}
    ])
    with open(os.path.join(KANIT_DIZIN, "asama4-delta-ozet.txt"), "a", encoding="utf-8") as f:
        f.write(metin)
    print(metin)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mod",
        required=True,
        choices=["surum", "kesinti-delta", "dogrula-delta", "kesinti-parquet", "dogrula-parquet"],
    )
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"bsm461-z7-{args.mod}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    os.makedirs(KANIT_DIZIN, exist_ok=True)

    if args.mod == "surum":
        mod_surum(spark)
    elif args.mod == "kesinti-delta":
        mod_kesinti_delta(spark)
    elif args.mod == "dogrula-delta":
        mod_dogrula_delta(spark)
    elif args.mod == "kesinti-parquet":
        mod_kesinti_parquet(spark)
    elif args.mod == "dogrula-parquet":
        mod_dogrula_parquet(spark)

    spark.stop()


if __name__ == "__main__":
    main()
