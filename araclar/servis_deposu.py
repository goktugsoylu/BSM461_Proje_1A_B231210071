# -*- coding: utf-8 -*-
"""
BSM 461 — Asama 4 (Servis Deposu / MongoDB · Z6) · Grup B231210071
Kullanim:
  docker compose --profile servis up -d
  docker compose exec araclar python araclar/servis_deposu.py
"""
import csv
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/kit/araclar")
from pymongo import ASCENDING, DESCENDING, MongoClient
from ortak import MONGO_ADRES
from uretici import uretec, zaman_damgasi

GRUP_KODU = "B231210071"
HABER_ADET = 20000
TEKRAR = 200
KANIT_DIZIN = "/kit/kanit"


def yuzdelik(degerler, p):
    sirali = sorted(degerler)
    idx = min(len(sirali) - 1, int(len(sirali) * p / 100.0))
    return round(sirali[idx], 2)


def sorgu_olc(fn, tekrar=TEKRAR):
    # Isinma (warm-up)
    for _ in range(5):
        fn()
    sureler_ms = []
    for _ in range(tekrar):
        t0 = time.perf_counter()
        fn()
        sureler_ms.append((time.perf_counter() - t0) * 1000.0)
    return {
        "ort": round(sum(sureler_ms) / len(sureler_ms), 2),
        "p50": yuzdelik(sureler_ms, 50),
        "p95": yuzdelik(sureler_ms, 95),
        "p99": yuzdelik(sureler_ms, 99),
    }


def olcumler_csv_ekle(yeni_satirlar):
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    alanlar = ["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"]
    mevcut = []
    if os.path.exists(csv_yol):
        with open(csv_yol, "r", encoding="utf-8") as f:
            mevcut = list(csv.DictReader(f))
    temiz_mevcut = [r for r in mevcut if r.get("asama") != "Asama4_Servis"]
    with open(csv_yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar)
        w.writeheader()
        w.writerows(temiz_mevcut + yeni_satirlar)


def csv_oku(dosya_adi):
    yol = os.path.join(KANIT_DIZIN, dosya_adi)
    with open(yol, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    os.makedirs(KANIT_DIZIN, exist_ok=True)
    client = MongoClient(MONGO_ADRES, serverSelectionTimeoutMS=10000)
    db = client["haber_servis"]

    # Temiz baslangic
    for ad in ["kaynak_ozetleri", "gunluk_seyir", "saatlik_dagilim", "son_haberler"]:
        db[ad].drop()

    rapor = []
    olcumler = []

    def ekle(deney, metrik, deger, birim, notlar=""):
        olcumler.append({
            "asama": "Asama4_Servis",
            "bilesen": "Z6",
            "deney": deney,
            "metrik": metrik,
            "deger": deger,
            "birim": birim,
            "notlar": notlar,
        })

    # 1) Toplu Soru 1 -> kaynak_ozetleri (Gomme modeli: boyut bilgileri + metrikler tek belgede)
    s1_satirlar = csv_oku("veri_s1_kaynak.csv")
    kaynak_belgeleri = [
        {
            "_id": r["kaynak"],
            "meta": {
                "tur": r["tur"],
                "sehir": r["sehir"],
                "kurulus_yili": int(r["kurulus_yili"]),
            },
            "istatistik": {
                "toplam_haber": int(r["toplam_haber"]),
                "gunluk_ort_haber": float(r["gunluk_ort_haber"]),
                "ort_kategori_cesidi": float(r["ort_kategori_cesidi"]),
            },
        }
        for r in s1_satirlar
    ]
    db["kaynak_ozetleri"].insert_many(kaynak_belgeleri)

    # 2) Toplu Soru 2A -> gunluk_seyir (Gomme modeli: bir gunun tum kategorileri tek belgede)
    s2_gun = csv_oku("veri_s2_gunluk.csv")
    gun_gruplari = defaultdict(list)
    for r in s2_gun:
        gun_gruplari[r["tarih"]].append(
            {"kategori": r["kategori"], "haber_sayisi": int(r["haber_sayisi"])}
        )
    gunluk_belgeler = [
        {
            "_id": tarih,
            "toplam_haber": sum(k["haber_sayisi"] for k in kats),
            "kategoriler": sorted(kats, key=lambda x: x["haber_sayisi"], reverse=True),
        }
        for tarih, kats in sorted(gun_gruplari.items())
    ]
    db["gunluk_seyir"].insert_many(gunluk_belgeler)

    # 3) Toplu Soru 2B -> saatlik_dagilim
    s2_saat = csv_oku("veri_s2_saatlik.csv")
    db["saatlik_dagilim"].insert_many(
        [{"_id": int(r["saat"]), "haber_sayisi": int(r["haber_sayisi"])} for r in s2_saat]
    )

    # 4) Son 20.000 haber -> son_haberler (Servis akis sorgulari icin)
    olay = uretec("haber", random.Random(GRUP_KODU))
    simdi = datetime.now(timezone.utc)
    haber_belgeleri = []
    for i in range(HABER_ADET):
        ts = simdi - timedelta(seconds=i * 2)
        h = olay(ts)
        haber_belgeleri.append({
            "_id": f"{h['haber_id']}_{i}",
            "haber_id": h["haber_id"],
            "olay_zamani": zaman_damgasi(ts),
            "kaynak": h["kaynak"],
            "kategori": h["kategori"],
            "baslik": h["baslik"],
        })
    t_yukle = time.perf_counter()
    db["son_haberler"].insert_many(haber_belgeleri, ordered=False)
    yukle_sure = round(time.perf_counter() - t_yukle, 2)

    rapor.append("=== Z6 SERVIS DEPOSU (MONGODB): VERI MODELI VE GECIKME OLCUMLERI ===")
    rapor.append(f"Senaryo p99 siniri : <= 200 ms (Grup: {GRUP_KODU}, her sorgu {TEKRAR} tekrar)")
    rapor.append(
        f"Yuklenen koleksiyonlar: kaynak_ozetleri ({len(kaynak_belgeleri)}), "
        f"gunluk_seyir ({len(gunluk_belgeler)}), saatlik_dagilim ({len(s2_saat)}), "
        f"son_haberler ({HABER_ADET:,} belge, {yukle_sure} s)"
    )

    # Sorgu 1: Tek kaynak profili (_id lookup)
    m_s1 = sorgu_olc(lambda: db["kaynak_ozetleri"].find_one({"_id": "ajans-kuzey"}))

    # Sorgu 2: Son 7 gunun kategori seyri (gomulu belge tek okuma)
    m_s2 = sorgu_olc(
        lambda: list(db["gunluk_seyir"].find().sort("_id", DESCENDING).limit(7))
    )

    # Sorgu 3A: Indekssiz (COLLSCAN) - Bir kaynagin son 20 haberi
    def q_son_haberler():
        return list(
            db["son_haberler"]
            .find({"kaynak": "ajans-kuzey"}, {"baslik": 1, "olay_zamani": 1, "kategori": 1})
            .sort("olay_zamani", DESCENDING)
            .limit(20)
        )

    plan_once = db["son_haberler"].find({"kaynak": "ajans-kuzey"}).sort("olay_zamani", DESCENDING).limit(20).explain()
    m_s3_once = sorgu_olc(q_son_haberler, tekrar=50)

    # Bilesik indeks olustur: (kaynak ASC, olay_zamani DESC) + (kategori ASC, olay_zamani DESC)
    db["son_haberler"].create_index([("kaynak", ASCENDING), ("olay_zamani", DESCENDING)], name="idx_kaynak_zaman")
    db["son_haberler"].create_index([("kategori", ASCENDING), ("olay_zamani", DESCENDING)], name="idx_kategori_zaman")

    plan_sonra = db["son_haberler"].find({"kaynak": "ajans-kuzey"}).sort("olay_zamani", DESCENDING).limit(20).explain()
    m_s3_sonra = sorgu_olc(q_son_haberler, tekrar=TEKRAR)

    def plan_asama(plan):
        win = plan.get("queryPlanner", {}).get("winningPlan", {})
        return str(win)

    collscan_var = "COLLSCAN" in plan_asama(plan_once)
    ixscan_var = "IXSCAN" in plan_asama(plan_sonra)

    rapor.append("\n--- SORGU GECIKME OLCUMLERI (ms) ---")
    rapor.append(f"{'Sorgu':<44}{'Ort(ms)':>9}{'p50(ms)':>9}{'p95(ms)':>9}{'p99(ms)':>9}  Plan")
    rapor.append(f"{'S1: Tek kaynak ozeti (_id)':<44}{m_s1['ort']:>9.2f}{m_s1['p50']:>9.2f}{m_s1['p95']:>9.2f}{m_s1['p99']:>9.2f}  IDHACK")
    rapor.append(f"{'S2: Son 7 gun kategori seyri (gomulu)':<44}{m_s2['ort']:>9.2f}{m_s2['p50']:>9.2f}{m_s2['p95']:>9.2f}{m_s2['p99']:>9.2f}  IXSCAN (_id)")
    rapor.append(f"{'S3_Once: Kaynagin son 20 haberi (indekssiz)':<44}{m_s3_once['ort']:>9.2f}{m_s3_once['p50']:>9.2f}{m_s3_once['p95']:>9.2f}{m_s3_once['p99']:>9.2f}  {'COLLSCAN' if collscan_var else '?'}")
    rapor.append(f"{'S3_Sonra: Kaynagin son 20 haberi (indeksli)':<44}{m_s3_sonra['ort']:>9.2f}{m_s3_sonra['p50']:>9.2f}{m_s3_sonra['p95']:>9.2f}{m_s3_sonra['p99']:>9.2f}  {'IXSCAN (idx_kaynak_zaman)' if ixscan_var else '?'}")

    rapor.append("\n--- SHARD ANAHTARI VE MONOTON ARTAN ANAHTAR RISKI ---")
    rapor.append("  Secilen shard anahtari : { haber_id: 'hashed' } veya bilesik { kaynak: 1, haber_id: 1 }")
    rapor.append("  Neden monoton artan anahtar (olay_zamani / artan ID) secilmedi?")
    rapor.append("  -> Tum yeni yazmalar (tepe yukte 1.200 haber/sn) her zaman en son araligi tutan tek shard'a")
    rapor.append("     yigilir (yazma sicak noktasi / write hotspot) ve yatay olcekleme bozulur.")

    ekle("s1_kaynak_profili", "p99_gecikme", m_s1["p99"], "ms", f"ort={m_s1['ort']} ms, p50={m_s1['p50']} ms, IDHACK")
    ekle("s2_son7gun_seyir", "p99_gecikme", m_s2["p99"], "ms", f"ort={m_s2['ort']} ms, gomulu belge")
    ekle("s3_son_haberler_indekssiz", "p99_gecikme", m_s3_once["p99"], "ms", f"ort={m_s3_once['ort']} ms, COLLSCAN (20k belge)")
    ekle("s3_son_haberler_indeksli", "p99_gecikme", m_s3_sonra["p99"], "ms", f"ort={m_s3_sonra['ort']} ms, IXSCAN (kaynak+olay_zamani)")

    olcumler_csv_ekle(olcumler)
    ozet = "\n".join(rapor)
    with open(os.path.join(KANIT_DIZIN, "asama4-servis-ozet.txt"), "w", encoding="utf-8") as f:
        f.write(ozet + "\n")

    print(ozet)
    print("\n✓ Kaydedildi: /kit/kanit/asama4-servis-ozet.txt ve /kit/kanit/olcumler.csv (guncellendi)")


if __name__ == "__main__":
    main()