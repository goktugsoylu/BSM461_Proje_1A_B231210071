# -*- coding: utf-8 -*-
"""
BSM 461 — Z10 İki Grafik · Grup B231210071 · Şablon 4 (haber)
Girdiler : kanit/veri_s1_kaynak.csv, kanit/veri_s2_gunluk.csv, kanit/veri_s2_saatlik.csv
Çıktılar : kanit/grafik1_kaynak_karsilastirma.png, kanit/grafik2_zaman_seyri.png
"""
import csv
import os
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

KOK = "/kit/kanit" if os.path.exists("/kit/kanit") else "kanit"


def csv_oku(ad):
    with open(os.path.join(KOK, ad), "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def grafik1_kaynak():
    satirlar = csv_oku("veri_s1_kaynak.csv")
    satirlar.sort(key=lambda r: int(r["toplam_haber"]))
    toplam_tum = sum(int(r["toplam_haber"]) for r in satirlar)

    renkler = {"ajans": "#1f77b4", "gazete": "#ff7f0e", "portal": "#2ca02c"}
    etiketler = [f"{r['kaynak']}\n({r['sehir']} · {r['kurulus_yili']})" for r in satirlar]
    degerler = [int(r["toplam_haber"]) / 1000.0 for r in satirlar]
    cubuk_renk = [renkler.get(r["tur"], "#7f7f7f") for r in satirlar]

    fig, ax = plt.subplots(figsize=(10, 5.4), dpi=150)
    bars = ax.barh(etiketler, degerler, color=cubuk_renk, edgecolor="#333333", linewidth=0.6)

    for bar, r in zip(bars, satirlar):
        sayi = int(r["toplam_haber"])
        pay = sayi / toplam_tum * 100
        gun_ort = float(r["gunluk_ort_haber"])
        ax.text(
            bar.get_width() + 14,
            bar.get_y() + bar.get_height() / 2,
            f"{sayi:,}  (%{pay:.1f} · {gun_ort:,.0f}/gün)".replace(",", "."),
            va="center",
            fontsize=9,
            fontweight="bold",
        )

    ax.set_xlim(0, max(degerler) * 1.36)
    ax.set_xlabel("14 Günlük Toplam Haber Sayısı (Bin)", fontsize=10)
    ax.set_title("Toplu Soru 1: Haber Kaynaklarının Tür ve Şehre Göre Hacim Karşılaştırması", fontsize=12, fontweight="bold", pad=12)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    lejant = [Patch(facecolor=c, edgecolor="#333333", label=t.capitalize()) for t, c in renkler.items()]
    ax.legend(handles=lejant, title="Kaynak Türü", loc="lower right", frameon=True)

    plt.tight_layout()
    cikti = os.path.join(KOK, "grafik1_kaynak_karsilastirma.png")
    plt.savefig(cikti, bbox_inches="tight")
    plt.close()
    print(f"✓ Kaydedildi: {cikti}")


def grafik2_zaman():
    saatlik = csv_oku("veri_s2_saatlik.csv")
    gunluk = csv_oku("veri_s2_gunluk.csv")

    saatler = [int(r["saat"]) for r in saatlik]
    saat_sayilar = [int(r["haber_sayisi"]) / 1000.0 for r in saatlik]

    # Günlük kategorileri grupla
    tarihler = sorted({r["tarih"] for r in gunluk})
    kat_toplam = defaultdict(int)
    kat_gun = defaultdict(dict)
    for r in gunluk:
        s = int(r["haber_sayisi"])
        kat_toplam[r["kategori"]] += s
        kat_gun[r["kategori"]][r["tarih"]] = s / 1000.0

    # En yoğun 5 kategoriyi çiz (diğerleri varsa üst üste binmesin)
    ust_kat = sorted(kat_toplam.keys(), key=lambda k: kat_toplam[k], reverse=True)[:5]

    print("\n--- Kategori Toplamları (14 Gün) ---")
    tum_kat = sum(kat_toplam.values())
    for k in sorted(kat_toplam.keys(), key=lambda x: kat_toplam[x], reverse=True):
        print(f"  {k:<12}: {kat_toplam[k]:>9,}  (%{kat_toplam[k]/tum_kat*100:.2f})")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10.5, 8.2), dpi=150)

    # Panel 1: Saatlik dağılım
    ax1.plot(saatler, saat_sayilar, marker="o", linewidth=2.2, color="#1f77b4")
    ax1.fill_between(saatler, saat_sayilar, alpha=0.15, color="#1f77b4")
    zirve_i = saat_sayilar.index(max(saat_sayilar))
    dip_i = saat_sayilar.index(min(saat_sayilar))
    ax1.annotate(
        f"Zirve: {saatler[zirve_i]:02d}:00 UTC ({saat_sayilar[zirve_i]:.1f} bin)",
        xy=(saatler[zirve_i], saat_sayilar[zirve_i]),
        xytext=(saatler[zirve_i] + 2.2, saat_sayilar[zirve_i] - 5),
        arrowprops=dict(arrowstyle="->", color="#d62728"),
        fontsize=9, fontweight="bold", color="#d62728",
    )
    ax1.annotate(
        f"Dip: {saatler[dip_i]:02d}:00 UTC ({saat_sayilar[dip_i]:.1f} bin)",
        xy=(saatler[dip_i], saat_sayilar[dip_i]),
        xytext=(saatler[dip_i] + 2.0, saat_sayilar[dip_i] + 18),
        arrowprops=dict(arrowstyle="->", color="#555555"),
        fontsize=9, color="#333333",
    )
    ax1.set_xticks(range(0, 24, 2))
    ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 2)])
    ax1.set_ylabel("Haber Sayısı (Bin)", fontsize=10)
    ax1.set_xlabel("Saat (UTC)", fontsize=10)
    ax1.set_title("Toplu Soru 2A: Günün Saatlerine Göre Haber Akış Yoğunluğu (14 Gün Toplamı)", fontsize=11.5, fontweight="bold")
    ax1.grid(True, linestyle="--", alpha=0.4)

    # Panel 2: Günlük kategori seyri
    kisa_tarih = [t[5:] for t in tarihler]  # AA-GG
    for k in ust_kat:
        y = [kat_gun[k].get(t, 0) for t in tarihler]
        ax2.plot(kisa_tarih, y, marker="s", linewidth=2, markersize=4.5, label=k)

    ax2.set_ylabel("Günlük Haber Sayısı (Bin)", fontsize=10)
    ax2.set_xlabel("Tarih (AA-GG)", fontsize=10)
    ax2.set_title("Toplu Soru 2B: En Yoğun 5 Kategorinin 14 Günlük Seyri", fontsize=11.5, fontweight="bold")
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.legend(title="Kategori", loc="center right", bbox_to_anchor=(1.16, 0.5), frameon=True)
    plt.setp(ax2.get_xticklabels(), rotation=35, ha="right")

    plt.tight_layout()
    cikti = os.path.join(KOK, "grafik2_zaman_seyri.png")
    plt.savefig(cikti, bbox_inches="tight")
    plt.close()
    print(f"✓ Kaydedildi: {cikti}")


if __name__ == "__main__":
    grafik1_kaynak()
    grafik2_zaman()