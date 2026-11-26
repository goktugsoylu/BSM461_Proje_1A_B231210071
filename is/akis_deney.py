# -*- coding: utf-8 -*-
"""
BSM 461 — Asama 5 (Watermark, Gec Veri ve Tekillestirme Olcumu · Z8) · Grup B231210071
Kullanim (iki ayri terminalde):
  Terminal 1: docker compose exec spark spark-submit is/akis_deney.py --deney gec_05 --watermark "2 minutes"
  Terminal 2: docker compose exec araclar python araclar/uretici.py --grup B231210071 --sablon haber --hiz 50 --sure 60 --gec-orani 0.05
"""
import argparse
import csv
import os
import time
from pyspark.sql import SparkSession, functions as F

KANIT_DIZIN = "/kit/kanit"
SEMA = "olay_zamani STRING, haber_id STRING, kaynak STRING, kategori STRING, baslik STRING, metin STRING"


def olcumler_csv_ekle(yeni_satirlar):
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    alanlar = ["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"]
    mevcut = []
    if os.path.exists(csv_yol):
        with open(csv_yol, "r", encoding="utf-8") as f:
            mevcut = list(csv.DictReader(f))
    yeni_anahtar = {(r["asama"], r["deney"], r["metrik"]) for r in yeni_satirlar}
    temiz = [
        r for r in mevcut
        if (r.get("asama"), r.get("deney"), r.get("metrik")) not in yeni_anahtar
    ]
    with open(csv_yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar)
        w.writeheader()
        w.writerows(temiz + yeni_satirlar)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deney", required=True, help="Ornek: gec_05 veya gec_30")
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--gec-etiket", default="%5", help="Raporda gorunecek gec veri orani")
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"bsm461-deney-{args.deney}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "6")
    os.makedirs(KANIT_DIZIN, exist_ok=True)

    ham = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "haberler")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    olaylar = (
        ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"))
        .select("o.*")
        .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
        .filter(F.col("olay_zamani").isNotNull() & F.col("haber_id").isNotNull())
    )

    pencereli = (
        olaylar.withWatermark("olay_zamani", args.watermark)
        .dropDuplicates(["haber_id", "olay_zamani"])
        .groupBy(F.window("olay_zamani", "1 minute"), F.col("kategori"))
        .agg(F.count("*").alias("haber_sayisi"))
    )

    pencere_tablosu = {}

    def batch_isle(df, batch_id):
        for r in df.collect():
            anahtar = (str(r["window"]["start"]), r["kategori"])
            pencere_tablosu[anahtar] = int(r["haber_sayisi"])

    sorgu = (
        pencereli.writeStream.outputMode("update")
        .foreachBatch(batch_isle)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"\n>>> HAZIR ({args.deney} | watermark={args.watermark}) <<<")
    print(">>> Simdi 2. terminalde uretici komutunu baslat! (Uretici bitince bu ekran otomatik kapanacak) <<<\n")

    gorulen_batchler = set()
    toplam_girdi = 0
    watermark_atilan = 0
    veri_geldi = False
    bos_tur = 0

    try:
        while sorgu.isActive:
            time.sleep(3)
            for p in sorgu.recentProgress:
                bid = p["batchId"]
                if bid in gorulen_batchler:
                    continue
                gorulen_batchler.add(bid)
                girdi = int(p.get("numInputRows", 0))
                atilan = sum(int(op.get("numRowsDroppedByWatermark", 0)) for op in p.get("stateOperators", []))
                toplam_girdi += girdi
                watermark_atilan += atilan

                if girdi > 0:
                    veri_geldi = True
                    bos_tur = 0
                    print(f"  [Batch {bid:02d}] gelen={girdi:>4} | watermark_atilan={atilan:>4} | kumulatif_girdi={toplam_girdi:>5}")
                elif veri_geldi:
                    bos_tur += 1

            if veri_geldi and bos_tur >= 3:
                break
    finally:
        sorgu.stop()
        spark.stop()

    sayima_giren = sum(pencere_tablosu.values())
    tekrar_ve_kirli = max(0, toplam_girdi - watermark_atilan - sayima_giren)
    kayip_yuzde = round((watermark_atilan / toplam_girdi) * 100, 2) if toplam_girdi else 0.0

    metin = (
        f"\n=== Z8 WATERMARK VE GEC VERI DENEYI ({args.deney} | gec_orani={args.gec_etiket} | wm={args.watermark}) ===\n"
        f"  Kafka'dan okunan toplam olay : {toplam_girdi:,}\n"
        f"  Watermark nedeniyle atilan   : {watermark_atilan:,} (%{kayip_yuzde})\n"
        f"  Tekrar/kirli elenen (dedup)  : {tekrar_ve_kirli:,}\n"
        f"  Akis sayimina giren benzersiz: {sayima_giren:,}\n"
    )
    print(metin)

    with open(os.path.join(KANIT_DIZIN, "asama5-akis-ozet.txt"), "a", encoding="utf-8") as f:
        f.write(metin)

    olcumler_csv_ekle([
        {"asama": "Asama5_Akis", "bilesen": "Z8", "deney": f"watermark_{args.deney}", "metrik": "kafka_girdi_olay", "deger": toplam_girdi, "birim": "olay", "notlar": f"gec_orani={args.gec_etiket}, wm={args.watermark}"},
        {"asama": "Asama5_Akis", "bilesen": "Z8", "deney": f"watermark_{args.deney}", "metrik": "watermark_atilan", "deger": watermark_atilan, "birim": "olay", "notlar": f"kayip=%{kayip_yuzde}"},
        {"asama": "Asama5_Akis", "bilesen": "Z8", "deney": f"watermark_{args.deney}", "metrik": "sayima_giren", "deger": sayima_giren, "birim": "olay", "notlar": f"tekrar_ve_kirli_elenen={tekrar_ve_kirli}"},
    ])
    print("✓ Kaydedildi: /kit/kanit/asama5-akis-ozet.txt ve /kit/kanit/olcumler.csv")


if __name__ == "__main__":
    main()