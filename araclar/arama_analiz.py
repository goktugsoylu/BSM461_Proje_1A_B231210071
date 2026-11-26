# -*- coding: utf-8 -*-
"""
BSM 461 — Asama 4 (Arama / Elasticsearch & Turkce Analizor · Z9) · Grup B231210071
Kullanim:
  docker compose --profile arama up -d
  docker compose exec araclar python araclar/arama_analiz.py
"""
import csv
import os
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/kit/araclar")
from elasticsearch import Elasticsearch, helpers
from ortak import ES_ADRES
from uretici import uretec, zaman_damgasi

ADET = 20000
GRUP_KODU = "B231210071"
INDEKS_TR = "haberler_turkce"
INDEKS_STD = "haberler_standard"
KANIT_DIZIN = "/kit/kanit"

AYAR_TURKCE = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "turkce": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "apostrophe",
                        "turkish_lowercase",
                        "turkish_stop",
                        "turkish_stemmer",
                    ],
                }
            },
            "filter": {
                "turkish_lowercase": {"type": "lowercase", "language": "turkish"},
                "turkish_stop": {"type": "stop", "stopwords": "_turkish_"},
                "turkish_stemmer": {"type": "stemmer", "language": "turkish"},
            },
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "olay_zamani": {"type": "date"},
            "haber_id": {"type": "keyword"},
            "kaynak": {"type": "keyword"},
            "kategori": {"type": "keyword"},
            "baslik": {"type": "text", "analyzer": "turkce"},
            "metin": {"type": "text", "analyzer": "turkce"},
        },
    },
}

AYAR_STANDARD = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "olay_zamani": {"type": "date"},
            "haber_id": {"type": "keyword"},
            "kaynak": {"type": "keyword"},
            "kategori": {"type": "keyword"},
            "baslik": {"type": "text", "analyzer": "standard"},
            "metin": {"type": "text", "analyzer": "standard"},
        },
    },
}


def olcumler_csv_ekle(yeni_satirlar):
    csv_yol = os.path.join(KANIT_DIZIN, "olcumler.csv")
    alanlar = ["asama", "bilesen", "deney", "metrik", "deger", "birim", "notlar"]
    mevcut = []
    if os.path.exists(csv_yol):
        with open(csv_yol, "r", encoding="utf-8") as f:
            mevcut = list(csv.DictReader(f))
    temiz_mevcut = [r for r in mevcut if r.get("asama") != "Asama4_Arama"]
    with open(csv_yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar)
        w.writeheader()
        w.writerows(temiz_mevcut + yeni_satirlar)


def indeks_olustur_ve_yukle(es, indeks_adi, ayar, veriler):
    if es.indices.exists(index=indeks_adi):
        es.indices.delete(index=indeks_adi)
    es.indices.create(index=indeks_adi, **ayar)

    t0 = time.perf_counter()
    belgeler = (
        {"_index": indeks_adi, "_id": h["haber_id"], "_source": h}
        for h in veriler
    )
    ok, _ = helpers.bulk(es, belgeler, chunk_size=1000)
    es.indices.refresh(index=indeks_adi)
    sure = round(time.perf_counter() - t0, 2)

    boyut_bayt = es.indices.stats(index=indeks_adi)["indices"][indeks_adi]["total"]["store"]["size_in_bytes"]
    boyut_mb = round(boyut_bayt / (1024 * 1024), 2)
    return ok, sure, boyut_mb


def sorgu_olc(es, indeks_adi, sorgu_metni, tekrar=5):
    sureler_ms = []
    sonuc = None
    body_query = {
        "multi_match": {
            "query": sorgu_metni,
            "fields": ["baslik^2", "metin"],
        }
    }
    for _ in range(tekrar):
        t0 = time.perf_counter()
        sonuc = es.search(
            index=indeks_adi,
            query=body_query,
            size=3,
            track_total_hits=True,
        )
        sureler_ms.append((time.perf_counter() - t0) * 1000)
    ort_ms = round(sum(sureler_ms) / len(sureler_ms), 2)
    toplam = sonuc["hits"]["total"]["value"]
    return toplam, ort_ms, sonuc["hits"]["hits"]


def main():
    es = Elasticsearch(ES_ADRES, request_timeout=60)
    os.makedirs(KANIT_DIZIN, exist_ok=True)

    olay = uretec("haber", random.Random(GRUP_KODU))
    simdi = datetime.now(timezone.utc)
    veriler = []
    for _ in range(ADET):
        h = olay(simdi)
        veriler.append({"olay_zamani": zaman_damgasi(simdi), **h})

    rapor = []
    olcumler = []

    def ekle(deney, metrik, deger, birim, notlar=""):
        olcumler.append({
            "asama": "Asama4_Arama",
            "bilesen": "Z9",
            "deney": deney,
            "metrik": metrik,
            "deger": deger,
            "birim": birim,
            "notlar": notlar,
        })

    # 1) Indeksleme olcumleri
    ok_tr, sure_tr, mb_tr = indeks_olustur_ve_yukle(es, INDEKS_TR, AYAR_TURKCE, veriler)
    ok_std, sure_std, mb_std = indeks_olustur_ve_yukle(es, INDEKS_STD, AYAR_STANDARD, veriler)

    rapor.append("=== Z9 ELASTICSEARCH: TURKCE vs STANDARD ANALIZOR KARSILASTIRMASI ===")
    rapor.append(f"Indekslenen haber sayisi : {ADET:,} (Grup: {GRUP_KODU})")
    rapor.append(f"  haberler_turkce   -> {ok_tr:,} belge | {sure_tr} s | {mb_tr} MB")
    rapor.append(f"  haberler_standard -> {ok_std:,} belge | {sure_std} s | {mb_std} MB")

    ekle("indeksleme_turkce", "sure", sure_tr, "s", f"{ok_tr} belge, {mb_tr} MB")
    ekle("indeksleme_turkce", "boyut", mb_tr, "MB", "1 shard, 0 replica")
    ekle("indeksleme_standard", "sure", sure_std, "s", f"{ok_std} belge, {mb_std} MB")
    ekle("indeksleme_standard", "boyut", mb_std, "MB", "1 shard, 0 replica")

    # 2) Analizor token (kok) karsilastirmasi ve arama sonuclari
    test_sorgulari = ["ihracatımızdan", "enflasyonlarından", "İstanbul'daki", "yatırımcılarımız"]
    rapor.append("\n--- ANALIZOR TOKEN VE SONUC SAYISI KARSILASTIRMASI ---")
    rapor.append(f"{'Sorgu':<20}{'Turkce Token':<20}{'Std Token':<24}{'TR Sonuc':>10}{'Std Sonuc':>11}{'TR(ms)':>9}{'Std(ms)':>9}")

    ilk_uc_ornek = None
    for s in test_sorgulari:
        tr_tok = [b["token"] for b in es.indices.analyze(index=INDEKS_TR, analyzer="turkce", text=s)["tokens"]]
        std_tok = [b["token"] for b in es.indices.analyze(index=INDEKS_STD, analyzer="standard", text=s)["tokens"]]

        tr_n, tr_ms, tr_hits = sorgu_olc(es, INDEKS_TR, s)
        std_n, std_ms, _ = sorgu_olc(es, INDEKS_STD, s)

        if s == "ihracatımızdan":
            ilk_uc_ornek = tr_hits

        rapor.append(
            f"{s:<20}{str(tr_tok):<20}{str(std_tok):<24}{tr_n:>10,}{std_n:>11,}{tr_ms:>9.2f}{std_ms:>9.2f}"
        )
        ekle(f"sorgu_{s}", "turkce_sonuc", tr_n, "belge", f"token={tr_tok}, {tr_ms} ms")
        ekle(f"sorgu_{s}", "standard_sonuc", std_n, "belge", f"token={std_tok}, {std_ms} ms")

    rapor.append("\n--- ORNEK ARAMA SORGUSU ('ihracatımızdan' - BM25 ilk 3 sonuc, baslik^2) ---")
    for h in (ilk_uc_ornek or []):
        rapor.append(f"  BM25={h['_score']:.2f} | [{h['_source']['kaynak']} / {h['_source']['kategori']}] {h['_source']['baslik']}")

    # 3) Toplama (Aggregation) sorgusu: Kaynak ve kategori bazinda dagilim
    agg_res = es.search(
        index=INDEKS_TR,
        size=0,
        aggs={
            "kaynaklar": {
                "terms": {"field": "kaynak", "size": 10},
                "aggs": {"en_cok_kategori": {"terms": {"field": "kategori", "size": 3}}},
            },
            "kategoriler": {"terms": {"field": "kategori", "size": 10}},
        },
    )
    rapor.append("\n--- TOPLAMA (AGGREGATION) SORGUSU: KAYNAK VE KATEGORI DAGILIMI ---")
    rapor.append("Kaynak bazinda haber sayisi (en baskin kategoriyle):")
    for b in agg_res["aggregations"]["kaynaklar"]["buckets"]:
        alt = ", ".join(f"{k['key']}:{k['doc_count']}" for k in b["en_cok_kategori"]["buckets"])
        rapor.append(f"  {b['key']:<18} -> {b['doc_count']:>6,} haber  (ilk 3: {alt})")

    rapor.append("Kategori bazinda haber sayisi:")
    for b in agg_res["aggregations"]["kategoriler"]["buckets"]:
        rapor.append(f"  {b['key']:<18} -> {b['doc_count']:>6,} haber")

    # 4) dynamic: strict testi
    rapor.append("\n--- MAPPING 'dynamic: strict' TESTI ---")
    try:
        es.index(
            index=INDEKS_TR,
            id="strict_test_1",
            document={"haber_id": "strict_test_1", "tanimsiz_alan": "deneme"},
        )
        rapor.append("  UYARI: Tanimsiz alan kabul edildi!")
    except Exception as e:
        rapor.append(f"  BASARILI: Tanimsiz alan reddedildi -> {type(e).__name__} (strict_dynamic_mapping_exception)")

    olcumler_csv_ekle(olcumler)
    ozet = "\n".join(rapor)
    with open(os.path.join(KANIT_DIZIN, "asama4-arama-ozet.txt"), "w", encoding="utf-8") as f:
        f.write(ozet + "\n")

    print(ozet)
    print("\n✓ Kaydedildi: /kit/kanit/asama4-arama-ozet.txt ve /kit/kanit/olcumler.csv (guncellendi)")


if __name__ == "__main__":
    main()