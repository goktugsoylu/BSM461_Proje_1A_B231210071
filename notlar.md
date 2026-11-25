# BSM 461 Dönem Projesi — Grup B231210071 Notları

* **Şablon:** Şablon 4 — Metin / Haber Akışı Analitiği
* **Grup Kodu:** `B231210071`
* **Kanıt Dosyaları (Aşama 1–2):** `kanit/ortam.png`, `kanit/senaryo.txt`, `kanit/olcumler.csv`, `kanit/asama2-ozet.txt`
* **Not:** Bu dosya, raporu derleyecek ekip arkadaşına aktarılmak üzere aşama aşama üretilen tablo, ölçüm, hesap ve gerekçeleri içerir.

---

## Aşama 1: Başlangıç (Adım 3–5 — Kesin Ölçümlerle Güncellendi)

### 1. Veri Sözlüğü *(Rapor: Ekler)*

| Tablo / Akış | Alan Adı | Veri Tipi | Açıklama | Örnek Değer |
| :--- | :--- | :--- | :--- | :--- |
| `haberler` (Olay) | `olay_zamani` | TIMESTAMP (ISO-8601) | Haberin yayımlanma zamanı (UTC) | `2026-11-25T13:27:13.683Z` |
| `haberler` (Olay) | `haber_id` | STRING | Haberin benzersiz kimliği | `H00000002` |
| `haberler` (Olay) | `kaynak` | STRING | Haberi yayımlayan kaynak (Kafka / Join anahtarı) | `gazete-merkez`, `ajans-kuzey` |
| `haberler` (Olay) | `kategori` | STRING | Haberin konusu | `kültür`, `ekonomi`, `spor`, `gündem` |
| `haberler` (Olay) | `baslik` | STRING | Haber başlığı (Türkçe) | `Seçim açıklandı` |
| `haberler` (Olay) | `metin` | STRING | Haber gövdesi (Türkçe, eklemeli kelimeler) | `Uzmanlar ihracatımızdan söz ederken...` |
| `kaynaklar.csv` (Boyut) | `kaynak` | STRING | Boyut tablosu birleştirme anahtarı (6 satır) | `ajans-kuzey`, `yerel-haber` |
| `kaynaklar.csv` (Boyut) | `tur` | STRING | Haber kaynağının türü | `ajans`, `gazete`, `portal` |
| `kaynaklar.csv` (Boyut) | `sehir` | STRING | Kaynağın bulunduğu şehir | `İstanbul`, `Adana`, `Ankara`, `Sakarya` |
| `kaynaklar.csv` (Boyut) | `kurulus_yili` | INT | Kaynağın kuruluş yılı | `1952`, `1987`, `1964`, `2011`, `1998`, `1979` |

**Boyut Tablosu Tam İçeriği (`s3://ham/boyut/haber/kaynaklar.csv` — 6 satır):**

| `kaynak` | `tur` | `sehir` | `kurulus_yili` |
| :--- | :--- | :--- | :--- |
| `ajans-kuzey` | ajans | İstanbul | 1952 |
| `ajans-guney` | ajans | Adana | 1987 |
| `gazete-merkez` | gazete | Ankara | 1964 |
| `portal-hizli` | portal | İstanbul | 2011 |
| `yerel-haber` | gazete | Sakarya | 1998 |
| `ekonomi-gunluk` | gazete | İstanbul | 1979 |

### 2. Verideki 4 Kusur *(Doğrulanmış Örneklerle)*

* **Çarpık anahtar (Skew):** 2.000.000 toplu kaydın **%48,37'si** tek başına `ajans-kuzey` kaynağından geliyor; `kaynak` anahtarının 8 bölümde `en dolu ÷ ortalama` oranı **2,14**.
* **Geç olay (Late data):** Canlı akışta `13:27:15–13:27:20` aralığındaki haberlerin arasında `H00000019` (`13:22:25Z` — ~5 dk geç) ve `H00000031` (`13:24:05Z` — ~3 dk geç) yakalandı.
* **Tekrar eden olay (Duplicate):** Üretici 2.000.000 toplu kaydın içine **4.007** tekrar bastı (örnek: `H00002877` → 2 kez, `H00005243` → 2 kez). `haber_id = NULL` kayıtlarla birlikte `dropDuplicates(["haber_id"])` toplam **4.841** kayıt (%0,242) ayıkladı.
* **Kirli kayıt (Missing/Null):** `haber_id` (838), `kaynak` (764), `kategori` (811), `baslik` (809) ve `metin` (812) alanlarında boş/null değerler tespit edildi (örnek: `H00000137` kaydında `baslik = None`, `H00000228` kaydında `metin = None`).

### 3. Projenin 4 Sorusu *(Rapor: Bölüm 2 — Özet)*

* **Toplu Soru 1 (Kaynak karşılaştırması — Join):** `kaynaklar.csv` boyut tablosuyla birleştirildiğinde, kaynak türüne (`tur`) ve şehre (`sehir`) göre günlük haber hacmi ile kategori çeşitliliği nasıl değişiyor?
* **Toplu Soru 2 (Zaman içindeki seyir):** Haber kategorilerinin ve öne çıkan gündem konularının günlere ve günün saatlerine göre yayınlanma eğilimi nasıl?
* **Akış Soru 1 (Gündem / Sıralama):** Son 1 saatlik pencerede (senaryo kartındaki en fazla 600 s uyarı süresi içinde) başlık/metinde en sık geçen konular hangileri?
* **Akış Soru 2 (Eşik / Hız):** Kategori başına dakikalık/pencere bazlı haber akış hızı normalin (veya belirlenen eşiğin) üzerine ne zaman çıkıyor?

### 4. KVKK Notu — Z10 (Parça 1) *(Rapor: Bölüm 10)*

Veri sözlüğünde IP adresi, TCKN, e-posta veya kullanıcı kimliği gibi doğrudan bir gerçek kişiyi tanımlayan yapısal bir alan bulunmamaktadır. Serbest metin alanları olan `baslik` ve `metin` içinde gerçek sistemlerde kişi veya muhabir adları geçebilir; bu veriler kamuya açık basın/haber metni niteliğinde olduğundan saklanmasında sakınca görülmemiştir. Yine de özel nitelikli kişisel veri sızıntısına karşı üretim ortamında metin içi PII (kimlik/telefon/e-posta) taraması ve maskeleme filtresi uygulanması öngörülür.

---

## Adım 4: Ölçek ve Eşik Hesabı — Z1 *(Rapor: Bölüm 4 — Kesin Ölçümlerle Güncellendi)*

Senaryo kartındaki gerçek koşullar (`600 haber/s` ortalama, `2 kat` tepe yük, `365 gün` saklama) ile Aşama 2'de 2.000.000 kayıt üzerinden ölçülen kesin ortalama kayıt boyutu (**327,53 bayt/kayıt**, 624,71 MB ham JSON) ve **Parquet-zstd** sıkıştırma oranı (**13,95 kat**) kullanılarak güncellenmiş hacim hesabı:

| Metrik | Hesap | Sonuç |
| :--- | :--- | :--- |
| **Ortalama hız** | Senaryo kartından | **600 haber/s** |
| **Tepe yük** | `600 × 2` (en yoğun saat) | **1.200 haber/s** |
| **Kayıt boyutu (Ham JSON)** | Ölçüm: `624,71 MiB ÷ 2.000.000 kayıt` | **327,53 bayt** |
| **Günlük olay sayısı** | `600 × 86.400 s` | **51.840.000 haber/gün** |
| **Günlük ham veri (JSON)** | `51.840.000 × 327,53 B` | **16,98 GB/gün** (15,81 GiB) |
| **Yıllık ham veri (365 gün, JSON)** | `16,98 GB × 365` | **6,20 TB** (5,64 TiB) |
| **Yıllık ham depolama (3 kopya)** | `6,20 TB × 3` (üretim dayanıklılığı) | **18,59 TB** |
| **Yıllık Parquet-zstd (1 kopya)** | `6,20 TB ÷ 13,95` *(Z2 ölçüm oranı)* | **444,3 GB** (~413,8 GiB) |
| **Yıllık Parquet-zstd (3 kopya)** | `444,3 GB × 3` | **1,33 TB** |
| **Tepe ağ/yazma hızı** | `1.200 × 327,53 B` | **393,0 KB/s** (~3,14 Mbit/s) |

### Kararlar ve Gerekçeler

* **Ortalama mı, tepe yük mü?** Kapasite planlaması (Kafka bölüm sayısı, akış işleme ve Elasticsearch indeksleme) **tepe yüke (1.200 haber/s)** göre yapılır; aksi hâlde en yoğun saatte tüketici gecikmesi (lag) büyür ve 600 s uyarı süresi ile 200 ms p99 servis sınırı aşılır.
* **Tek makine eşiği (H1 tablosu):** Anlık akış hızı (~393 KB/s) tek makinenin ağını zorlamaz; ancak **365 günlük ham veri (6,20 TB JSON, 3 kopyayla 18,59 TB)** Hafta 1 eşik tablosunda `> birkaç TB, sürekli büyüyor` bölgesindedir. Parquet-zstd ile işlenmiş veri **444 GB'a** (3 kopyayla **1,33 TB'a**) inerek tek güçlü sunucunun disk sınırına yaklaşsa da, ham verinin 365 gün saklanma zorunluluğu, aynı anda Türkçe ters indeksleme (Elasticsearch) ve **200 ms p99** servis sorgusu hedefi yüzünden **ilk tıkanacak bileşen disk G/Ç (aktarım) ve arama indeksinin belleği** olur; bu yüzden dağıtık depolama + dağıtık işleme (S3 + Spark + Elasticsearch) gereklidir.
* **10x ve 100x ölçekte ne değişir?**
  * **10 katı (~62 TB ham / ~4,4 TB Parquet-zstd, 12.000 haber/s tepe):** Tek makine tamamen imkânsızdır. Çok düğümlü Kafka kümesi, çoklu Spark Executor düğümleri, S3'te tarih bölümlemesi + `zstd` sıkıştırma ve çok shard'lı Elasticsearch kümesi şarttır.
  * **100 katı (~620 TB ham / ~44,4 TB Parquet-zstd, 120.000 haber/s tepe):** Sıcak/soğuk katmanlama (hot-warm-cold) gerekir; son 30 gün hızlı arama için Elasticsearch'te tutulurken eski veriler yalnızca S3'te Delta/Parquet olarak arşivlenir, ön-toplulaştırılmış özetler servis deposuna yazılır.

---

## Adım 5: Mimari Şema ve Garanti Tablosu İlk Taslağı — Z0 *(Rapor: Bölüm 3)*

### Omurga Şeması (Taslak)

```text
[Haber Üretici] ──► [Kafka 4.0: haberler] ──► [Spark Structured Streaming (Z8)] ──► [Servis Deposu (?)]
       │                                                                                    │
       └──(Toplu geçmiş veri)──► [SeaweedFS S3 (ham/)] ──► [Spark Toplu İş (Z5)] ──────────┼──► [Elasticsearch (Z9, Türkçe)]
                                       │                           │
                                       └──► [Parquet-zstd (islenmis/)] ─┴──► [Delta Lake (tablolar/)]
```

### Garanti Tablosu (İlk Taslak ve Kit Sadeleştirmeleri)

| Bileşen / Ok | ① Kaç kopya? (Kit → Üretim) | ② Sıra garantisi var mı? | ③ Ağ bölünürse / düğüm çökerse ne olur? |
| :--- | :--- | :--- | :--- |
| **Kafka (`haberler`)** | Kit: 1 kopya (tek düğüm) → Üretim: 3 kopya (`min.insync.replicas=2`) | Yalnızca aynı anahtarın düştüğü **bölüm içinde** sıralı; bölümler arası garanti yok | Kitte düğüm düşerse yazma/okuma durur; üretimde lider devri olur |
| **S3 (`ham/`, `islenmis/`)** | Kit: 1 kopya (tek SeaweedFS) → Üretim: Sağlayıcı 3+ kopya | Yok (nesne bazlı; atomik klasör taşıma yok) | Yarım kalan Spark yazması tabloyu bozabilir → **Delta Lake (Z7)** ile çözülecek |
| **Spark Toplu (Z5)** | Veri kopyası yok; RDD/DF **soy ağacı (lineage)** ile yeniden hesaplar | Açık `orderBy` yapılmazsa sıra garantisi yok | Çöken görev soy ağacından tekrar çalışır; S3 giderse iş hata verir (Z11'de denenecek) |
| **Akış / Servis / Arama** | `?` *(Z6, Z8, Z9 aşamalarında netleştirilecek)* | `?` | `?` |

---

## Aşama 2: Depolama (Adım 6–7 · Z2, Z4, Z3, Z10)

### 1. Z2 — Dosya Biçimi ve Sıkıştırma Kodeki *(Rapor: Bölüm 5)*

* **Şema okuma ölçümü:** Spark'a şemayı tahmin ettirmek (`inferSchema`) **2,39 s**, şemayı elle vermek (`StructType`) **0,16 s** sürdü (~15 kat fark; şema çıkarımı ham JSON'u fazladan tarar).
* **Aynı sorgu (`groupBy("kaynak").count()`, 3 tur ortanca) ve kodek karşılaştırması:**

| Biçim / Kodek | Dosya Sayısı | Toplam Boyut (MB) | JSON ÷ Parquet Oranı | Yazma Süresi (s) | Sorgu Süresi (s) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **JSON (ham)** | 14 | 624,71 | 1,00× | — | 1,84 |
| **Parquet + snappy** | 8 | 79,17 | 7,89× | 7,45 | 0,26 |
| **Parquet + zstd** | 8 | **44,77** | **13,95×** | **3,38** | **0,18** |
| **Parquet + gzip** | 8 | 39,03 | 16,00× | 3,89 | 0,19 |

* **Seçim ve Gerekçe (`Parquet + zstd`):**
  * **JSON → Parquet kazancı:** Sütunlu yapı sayesinde sorgu yalnızca `kaynak` sütununu okudu (projection pushdown), JSON alan adlarının her satırda tekrarlanması ve metin ayrıştırma (parsing) yükü kalktı; sorgu süresi **1,84 s → 0,18 s** ile **10,2 kat** hızlandı.
  * **Neden `zstd`?** Haber verisi metin ağırlıklı olduğu için `zstd`, `snappy`'ye göre boyutu neredeyse yarıya indirdi (79,17 MB → 44,77 MB, %43 tasarruf) ve S3'e daha az veri yazdığı/okuduğu için hem yazmada (3,38 s) hem sorguda (0,18 s) en iyi performansı verdi. `gzip` yalnızca 5,74 MB daha küçük olmasına rağmen daha yavaş yazdı (3,89 s) ve okudu (0,19 s).
  * **Bölünebilirlik (H2):** Parquet sıkıştırmayı dosya geneline değil her sütun içindeki ~1 MB'lık bağımsız sayfalara (page) ve satır gruplarına (row group) uyguladığı için `zstd` ve `gzip` kodekleriyle bile **tamamen bölünebilirdir** (paralel okunabilir).
  * **Başabaş (amortisman) hesabı:** `3,38 s ÷ (1,84 − 0,18 s) = 2,04` → yalnızca **3. sorgudan itibaren** dönüştürme maliyeti kendini amorti etmektedir.
  * *(Not: Projection ve predicate pushdown'ın `PushedFilters` / `ReadSchema` kanıtı Aşama 3'teki `explain()` çıktısında gösterilecektir.)*

---

### 2. Z4 — Nesne Deposu ve Dosya Düzeni *(Rapor: Bölüm 5)*

14 günlük haber verisi üç farklı S3 düzeniyle yazılıp tek bir günün sorgusu (`tarih = '2026-11-18'`, `groupBy("kaynak").count()`, 3 tur ortanca) ölçüldü:

| S3 Dizin Düzeni | Toplam Dosya | Toplam Boyut (MB) | Ort. Dosya Boyutu (MB) | Tek Gün Okunan Dosya | Tek Gün Okunan Veri (MB) | Süre (s) | `PartitionFilters` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **(a) Bölümsüz** | 8 | 79,17 | 9,90 | 8 | 79,20 | 0,86 | Yok |
| **(b) `tarih=.../` (birleştirmesiz)** | 28 | 78,91 | 2,82 | 2 | 5,60 | 0,24 | `tarih = 2026-11-18` |
| **(c) `tarih=.../` + `repartition("tarih")`** | **14** | 78,70 | **5,62** | **1** | **5,60** | **0,15** | `tarih = 2026-11-18` |

* **Seçim ve Gerekçe:** Çıktılar **`tarih=YYYY-AA-GG/`** dizin düzeninde ve yazmadan önce **`repartition("tarih")`** uygulanarak saklanacaktır.
  * **Bölüm budama (Partition Pruning):** Tek gün sorgusunda okunan veri **79,20 MB → 5,60 MB**'a (14 kat azalma), sorgu süresi **0,86 s → 0,15 s**'ye (5,7 kat hızlanma) düştü.
  * **Küçük dosya birleştirme:** `repartition("tarih")` yapılmadığında her bölüm görev başına bölünerek 28 küçük dosya (ort. 2,82 MB) oluşturdu ve bellek uyarısına yol açtı; `repartition("tarih")` ile her gün tam **1 dosya** olacak şekilde toplam **14 dosyaya** (temiz zstd tabloda 14 dosya, 63,89 MB, ort. 4,56 MB) indirildi. (Üretim ölçeğinde günlük ~1,2 GB Parquet-zstd veri için gün başına ~8–10 adet 128 MB'lık dosya hedeflenir.)
* **HDFS yerine Nesne Deposu (S3) Kullanmanın Bedeli (H4):**
  * **Listeleme (`LIST`) maliyeti:** S3 düz bir anahtar-değer alanıdır, gerçek klasör yoktur. Her `LIST` isteği en fazla 1.000 nesne döndürür ve devam fişiyle (pagination) sıralı çalışır; çok sayıda küçük dosya oluşursa sorgu daha veri okumadan listeleme aşamasında yavaşlar.
  * **Atomik yeniden adlandırma olmaması:** HDFS'te bir klasörü taşımak tek bir atomik NameNode meta veri işlemidir; S3'te ise her nesnenin kopyalanıp eskisinin silinmesi gerekir. Spark toplu işi S3'e yazarken yarıda çökerse çıktı klasöründe yarım kalmış dosyalar kalır ve okuyan sorgular sessizce eksik/bozuk veri görür. Bu risk **Z7 (Delta Lake)** işlem günlüğü (`_delta_log`) ile giderilecektir.

---

### 3. Z3 — Bölümleme, Sıcak Nokta, Çoğaltma ve Tutarlılık *(Rapor: Bölüm 6)*

2.000.000 kayıt, $K = 8$ bölüme `pmod(hash(anahtar), 8)` ile dağıtılarak aday anahtarların dengesi ölçüldü (ideal = **1,00**, sıcak nokta eşiği = **1,50**):

| Aday Bölüm Anahtarı | Farklı Değer | En Büyük Değer | En Büyük Pay (%) | `En Dolu ÷ Ortalama` | Değerlendirme |
| :--- | :---: | :--- | :---: | :---: | :--- |
| `kaynak` | 7 | `ajans-kuzey` | %48,37 | **2,14** | ❌ Ciddi sıcak nokta (tek kaynak verinin yarısı) |
| `kategori` | 9 | `gündem` | %36,58 | **2,20** | ❌ Sıcak nokta |
| `tarih` | 14 | `2026-11-24` | %7,14 | **2,50** | ❌ Az anahtar + çakışma; artan zaman yazmada sıcak düğüm yaratır |
| `haber_id` | 1.995.159 | `None` | %0,04 | **1,00** | ✅ Kusursuz dağılım (ancak kaynak/tarih gruplamasına yardım etmez) |
| *Çözüm 1:* Tuzlama (`ajans-kuzey#0..15`) | 22 | — | — | **2,41** | ❌ Düşük kardinalitede (6 kaynak) diğer büyük kaynakla (`ajans-guney`) çakıştı |
| *Çözüm 2:* **Bileşik (`kaynak\|tarih`)** | ~98 | — | — | **1,29** | ✅ **Eşiğin altında (2,14 → 1,29), sıcak nokta çözüldü** |

* **Sıcak Nokta Analizi ve Seçim:** `kaynak` anahtarı verinin neredeyse yarısını (%48,37) tek değerde (`ajans-kuzey`) topladığı için 2,14 oranıyla sıcak nokta oluşturdu. İlginç bir ölçüm bulgusu olarak, yalnızca `ajans-kuzey`'i 16 parçaya tuzlamak toplam anahtar sayısı çok küçük olduğundan (`K = 8` kovada `ajans-guney` ile aynı kovaya denk gelme çakışması yüzünden) dengeyi iyileştirmeyip **2,41**'e çıkardı. Buna karşılık **`kaynak|tarih` bileşik anahtarı** anahtar uzayını ~98 değere genişleterek oranı **2,14 → 1,29**'a indirdi. Buna göre: S3'te zaman aralığı sorguları için `tarih=.../` klasör bölümlemesi, dağıtık işleme/parçalama katmanında ise sıcak noktayı kıran **`kaynak|tarih`** (veya tam tekil dağılım gereken indeks katmanında `haber_id`) tercih edilir.
* **İdempotentlik (Tekrar Eden Olaylar):** Üretici 2.000.000 olay içine bilerek **4.007** tekrar eklemiştir (ör. `H00002877`, `H00005243`). Tekilleştirme yapılmazsa kaynak ve kategori sayımları olduğundan yüksek çıkar ve sonuç bozulur. Toplu hatta `dropDuplicates(["haber_id"])` ile tekrarlar temizlenerek toplu iş idempotent hâle getirilmiştir.
* **Çoğaltma Düzeni (Kit → Üretim):**
  * **Kafka:** Kit: 1 kopya (tek broker) → Üretim: **3 kopya** (`replication.factor=3`, `min.insync.replicas=2`, `acks=all`) — bir broker düşse bile veri kaybı ve kesinti olmaz.
  * **S3:** Kit: 1 kopya (tek SeaweedFS) → Üretim: **3+ kopya** (veya erasure coding, farklı erişilebilirlik bölgeleri).
  * **Elasticsearch:** Kit: 0 replika (tek düğüm) → Üretim: **1 birincil + 1 replika** (toplam 2 kopya) — hem düğüm arızasına dayanım hem okuma paralelliği için.
* **Tutarlılık Kararı (CAP & PACELC):** Haber toplayıcı ve arama analitiği sisteminde finansal işlem gibi katı tutarlılık gerekmez; haberlerin kesintisiz toplanması ve **p99 ≤ 200 ms** gecikmeyle aranabilmesi önceliklidir. Bu nedenle bir ağ bölünmesinde **Erişilebilirlik (A)**, normal zamanda ise **Düşük Gecikme (L)** seçilir → **PACELC sınıfı: `PA/EL`** (nihai tutarlılık kabul edilebilir; veri kaybı önleme sorumluluğu üretimde Kafka'nın 3 kopyalı çoğaltma katmanındadır).

---

### 4. Z10 — Kirli Veri Sayımı ve Temizleme Kararı (Parça 2) *(Rapor: Bölüm 10)*

2.000.000 ham kayıt üzerinde yapılan veri kalitesi taraması:

| Kusur Türü | Etkilenen Kayıt | Oran (%) | Uygulanan Karar |
| :--- | :---: | :---: | :--- |
| Tekrar eden `haber_id` *(4.007 tekrar + 837 null birleşimi)* | 4.841 | %0,242 | **Tekilleştirildi** (`dropDuplicates(["haber_id"])`) |
| Bozuk / boş `olay_zamani` | 0 | %0,000 | — |
| Boş / `null` `haber_id` | 838 | %0,042 | **Karantinaya ayrıldı** |
| Boş / `null` `kaynak` | 764 | %0,038 | **Karantinaya ayrıldı** |
| Boş / `null` `kategori` | 811 | %0,041 | **Karantinaya ayrıldı** |
| Boş / `null` `baslik` | 809 | %0,040 | **Karantinaya ayrıldı** |
| Boş / `null` `metin` | 812 | %0,041 | **Karantinaya ayrıldı** |

* **Temizleme Kararı ve Gerekçesi:**
  * Tekrar eden haberler (`haber_id`) sayımları şişirmemek için tekilleştirildi.
  * Zorunlu alanlardan (`olay_zamani`, `haber_id`, `kaynak`, `kategori`, `baslik`, `metin`) herhangi biri boş/null olan kayıtlar silinip kaybedilmek yerine **izlenebilirlik ve hata ayıklama** amacıyla ayrı bir karantina tablosuna (**`s3a://islenmis/haber_karantina/` — 3.188 kayıt**) yazıldı.
  * Geriye kalan **1.991.971 temiz kayıt**, `tarih` ve `saat` sütunları eklenerek **`s3a://islenmis/haber_temiz/`** yoluna `repartition("tarih")` + `Parquet-zstd` ile (**14 dosya, 63,89 MB**) kaydedildi. (`1.991.971 + 3.188 + 4.841 = 2.000.000`)