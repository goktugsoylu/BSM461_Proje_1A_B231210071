# -*- coding: utf-8 -*-
"""
BSM 461 — Asama 5 (Akis Hatti / Kafka -> Spark Structured Streaming · Z8) · Grup B231210071

Iki akis sorusu (Sablon 4 - Metin/Haber Akisi):
  - Akis Sorusu 1 (s1_kategori): 1 dakikalik sabit (tumbling) pencerede kategori basina
    benzersiz haber sayisi ve aktif kaynak sayisi.
  - Akis Sorusu 2 (s2_kaynak): 5 dakikalik kayan (sliding, 1 dk kayma) pencerede
    kaynak + kategori bazinda haber yogunlugu.

Calistirma (iki ayri terminalde):
  1) docker compose exec araclar python araclar/uretici.py --grup B231210071 --sablon haber --hiz 100 --sure 90
  2) docker compose exec spark spark-submit is/akis_iskelet.py --sablon haber --cikti s3 --watermark "2 minutes"
"""
import argparse
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", default="haber", choices=["log", "satis", "iot", "haber"])
ap.add_argument("--cikti", default="konsol", choices=["konsol", "s3"],
                help="konsol: ekrana yaz · s3: Parquet olarak s3a://islenmis/akis/<etiket>/")
ap.add_argument("--soru", default="s1", choices=["s1", "s2", "ikisi"],
                help="s1: 1dk sabit kategori hizi · s2: 5dk kayan kaynak-kategori · ikisi: her ikisi")
ap.add_argument("--watermark", default="2 minutes",
                help="Watermark gecikme esigi (ornek: '2 minutes', '10 minutes')")
ap.add_argument("--etiket", default="haber",
                help="S3 cikti ve checkpoint alt klasor adi (deneyleri ayirmak icin)")
ap.add_argument("--baslangic", default="earliest", choices=["earliest", "latest"],
                help="startingOffsets secimi (checkpoint yoksa nereden baslasin)")
ap.add_argument("--tekillestir-kapali", action="store_true",
                help="Verilirse dropDuplicates(haber_id) kapatilir (tekrar olay etkisini olcmek icin)")
a = ap.parse_args()

KONU = {"log": "web-loglari", "satis": "satislar", "iot": "sensorler", "haber": "haberler"}[a.sablon]

SEMA = {
    "log":   "olay_zamani STRING, ip STRING, yontem STRING, yol STRING, durum INT, bayt LONG, sure_ms DOUBLE, ajan STRING",
    "satis": "olay_zamani STRING, siparis_id STRING, magaza_id INT, urun_id INT, adet INT, birim_fiyat DOUBLE, tur STRING",
    "iot":   "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE",
    "haber": "olay_zamani STRING, haber_id STRING, kaynak STRING, kategori STRING, baslik STRING, metin STRING",
}[a.sablon]

spark = SparkSession.builder.appName(f"bsm461-akis-{a.etiket}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "6")

# ── 1. Kafka'dan oku ────────────────────────────────────────────────────────
# startingOffsets="earliest": Yeni bir deney/checkpoint baslatildiginda konuda bekleyen
# olaylarin kaybolmamasini saglar. Is yeniden basladiginda checkpoint varsa Spark ofsetleri
# checkpoint'ten okur ve startingOffsets'i yok sayar.
ham = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", KONU)
    .option("startingOffsets", a.baslangic)
    .option("failOnDataLoss", "false")
    .load()
)

olaylar = (
    ham.select(
        F.from_json(F.col("value").cast("string"), SEMA).alias("o"),
        F.col("timestamp").alias("islem_zamani"),
    )
    .select("o.*", "islem_zamani")
    .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
    .filter(F.col("olay_zamani").isNotNull() & F.col("haber_id").isNotNull())
)

# ── 2. Watermark ve Tekillestirme (Idempotentlik) ───────────────────────────
# Uretici olaylarin %5'ini 1-10 dk gec ve ~%0.2'sini tekrar gonderiyor.
# withWatermark + dropDuplicates(["haber_id", "olay_zamani"]) pencere siniri icindeki
# cift gonderilen olaylarin sayima iki kez girmesini engeller.
ak = olaylar.withWatermark("olay_zamani", a.watermark)
if not a.tekillestir_kapali:
    ak = ak.dropDuplicates(["haber_id", "olay_zamani"])

# Akis Sorusu 1: 1 dakikalik sabit pencerede kategori basina haber hizi ve kaynak cesitliligi
s1_pencereli = (
    ak.groupBy(F.window("olay_zamani", "1 minute"), F.col("kategori"))
    .agg(
        F.count("*").alias("haber_sayisi"),
        F.approx_count_distinct("kaynak").alias("aktif_kaynak"),
    )
    .select(
        F.col("window.start").alias("pencere_baslangic"),
        F.col("window.end").alias("pencere_bitis"),
        "kategori",
        "haber_sayisi",
        "aktif_kaynak",
    )
)

# Akis Sorusu 2: 5 dakikalik kayan pencerede (1 dk kayma) kaynak-kategori yogunlugu
s2_pencereli = (
    ak.groupBy(F.window("olay_zamani", "5 minutes", "1 minute"), F.col("kaynak"), F.col("kategori"))
    .agg(F.count("*").alias("haber_sayisi"))
    .select(
        F.col("window.start").alias("pencere_baslangic"),
        F.col("window.end").alias("pencere_bitis"),
        "kaynak",
        "kategori",
        "haber_sayisi",
    )
)

# ── 3. Cikti (Konsol veya S3 Parquet + Checkpoint) ──────────────────────────
def baslat(df, ad):
    if a.cikti == "konsol":
        return (
            df.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .option("numRows", 20)
            .trigger(processingTime="10 seconds")
            .queryName(ad)
            .start()
        )
    else:
        return (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"s3a://islenmis/akis/{a.etiket}_{ad}/")
            .option("checkpointLocation", f"s3a://islenmis/_checkpoint/{a.etiket}_{ad}/")
            .trigger(processingTime="15 seconds")
            .queryName(ad)
            .start()
        )

sorgular = []
if a.soru in ("s1", "ikisi"):
    sorgular.append(baslat(s1_pencereli, "s1_kategori"))
if a.soru in ("s2", "ikisi"):
    sorgular.append(baslat(s2_pencereli, "s2_kaynak"))

print(f"✓ Akis basladi | konu={KONU} | soru={a.soru} | watermark={a.watermark} | tekillestir={not a.tekillestir_kapali} | cikti={a.cikti}")
for q in sorgular:
    q.awaitTermination()