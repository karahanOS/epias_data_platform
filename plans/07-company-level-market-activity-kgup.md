# ADR-0007: Şirket Bazlı Piyasa Aktivitesi — GİP Değil, GÖP + KGÜP-Bulk Üzerinden

**Status:** Accepted (Phase 1 implementasyonda)
**Date:** 2026-08-09
**Deciders:** Mehmet Karahan Çetinkaya

**Update (2026-08-09):** İlk taslak sadece KGÜP-bulk'u önerdi. Canlı API testinde (`/v1/markets/dam/data/clearing-quantity-organization-list` + `organizationId` filtreli `/v1/markets/dam/data/clearing-quantity`) **GÖP (day-ahead) tarafında da gerçek, şirket bazlı, saatlik eşleşen hacim (`matchedBids`/`matchedOffers`) verisinin var olduğu doğrulandı** — 1629 organizasyon, canlı sorgulandı, değerler mantıklı (bkz. aşağıdaki "GÖP Şirket Bazlı Eşleşme Miktarı" bölümü). Bu, GİP'in kendisinden daha iyi bir "piyasa aktivitesi" sinyali çünkü doğrudan GÖP'ün (ana spot piyasa) kendisi. Faz sıralaması buna göre güncellendi: **Faz 1 = GÖP şirket bazlı eşleşme miktarı** (bu belge), **Faz 2 = KGÜP-bulk üretim planı aktivitesi** (alttaki orijinal analiz, değişmedi).

## Context

İstek: "GİP aktivitesi şirket bazında eklensin, piyasayı daha iyi anlayıp analiz üretebilelim."

### Zaten bir deneme var, ve ölü

`mart_gip_company_analysis.sql` (2026-06-09) ve dashboard'daki "🏢 Şirket Bazlı GİP Aktivitesi" bölümü tam olarak bunu hedefleyerek yazılmış: `stg_idm_transactions`'taki `buyerOrganizationId`/`sellerOrganizationId` üzerinden GİP işlemlerini şirkete atfetmek. Model her zaman 0 satır dönüyor — dosyanın kendi yorumu bunu zaten belgeliyor.

### Bugünkü doğrulama (EPİAŞ Şeffaflık Platformu resmi dokümantasyonu, canlı kontrol edildi)

Kullanıcının işaret ettiği iki URL üzerinden başlayıp GİP'in **tüm 16 endpoint'ini** (8 veri + 8 dışa aktarım) tek tek tarayarak doğrulandı:

- `transaction-history`'nin gerçek response şeması (`TransactionHistoryGipDataDto`): `contractName`, `date`, `hour`, `id`, `price`, `quantity`. **Organizasyon alanı hiçbir zaman olmamış** — `stg_idm_transactions.sql`'deki `buyerOrganizationId`/`sellerOrganizationId` referansları, Silver'dan "kaybolan" değil, en baştan var olmayan alanlar.
- Kullanıcının bulduğu `matching-quantity-organization-list` → gerçek yolu `POST /v1/markets/dam/data/clearing-quantity-organization-list`, etiketi `markets-gop-data-controller` — yani **GÖP'e ait, GİP'e değil**. Şeması da (`ClearingQuantityOrganizationDataDto`: `eic`, `organizationId`, `organizationName`, `organizationStatus`, `shortName`) sadece bir roster — hacim/aktivite yok.
- Diğer 15 GİP endpoint'i (matching-quantity, weighted-average-price, bid-offer-quantities, min-max fiyat servisleri, trade-value, hepsinin export'ları) tek tek kontrol edildi — hiçbirinde organizasyon alanı yok.

**Sonuç:** EPİAŞ'ın herkese açık Şeffaflık Platformu'nda GİP işlemini bir şirkete atfeden hiçbir endpoint yok. Bu, sürekli çift taraflı müzayede piyasalarında standart olan karşı taraf anonimliği — geçici bir API eksikliği değil, kasıtlı bir tasarım kararı gibi görünüyor. Bu yüzden **doğrudan istenen şey (ham GİP işleminin şirkete atfı) inşa edilemez.**

### Gerçek alternatif bulundu — ve isimlendirme karışıklığı ortaya çıktı

BGÜP/KGÜP ailesi `organizationId`/`uevcbId` destekliyor. Ama inceleme sırasında kod tabanındaki isimlendirmenin EPİAŞ'ın kendi resmi doküman başlıklarıyla **ters düştüğü** ortaya çıktı:

| Kod tabanındaki isim | Gerçek endpoint | EPİAŞ'ın resmi başlığı | Gerçekte ne |
|---|---|---|---|
| `get_dpp` → "BGÜP" | `POST /v1/generation/data/dpp` | "5.71. **Kesinleşmiş** Günlük Üretim Planı (**KGÜP**) Listeleme Servisi" | KGÜP (gün öncesi kesinleşmiş plan), BGÜP değil |
| `get_sbfgp` → "KGÜP/SBFGP" | `POST /v1/generation/data/sbfgp` | "5.83. Kesinleştirilmiş **Uzlaştırma Dönemi** Üretim Planı (**KUDÜP**) Listeleme Servisi" | KUDÜP — KGÜP'ten sonraki, uzlaştırma dönemine ait ayrı bir katman |
| *(wire edilmemiş)* | `POST /v1/generation/data/dpp-first-version` | "5.73. KGÜP **İlk Versiyon** Listeleme Servisi" | Katılımcının bir sonraki gün için ilk bildirdiği değer — **gerçek BGÜP karşılığı bu** |
| `get_dpp_bulk` → "BGÜP" | `POST /v1/generation/data/dpp-bulk` | "5.72. Uevçb Bazlı **Toplu KGÜP** Listeleme Servisi" | KGÜP, UEVÇB bazında, toplu |

Bu isimlendirme karışıklığı `mart_production_plan.sql`'in "BGÜP vs KGÜP revizyonu" yorumunu da etkiliyor (muhtemelen aslında KGÜP→KUDÜP farkını ölçüyor) — ama bu ADR'ın kapsamı dışında, ayrı bir düzeltme olarak işaretlendi (bkz. Action Items).

Bizim için asıl önemli olan **`dpp-bulk`**: gerçek response şeması (`KgupBulkDataDto`) `orgId` **ve** `uevcbId` **ve** saatlik yakıt-kırılımı (`toplam`, `dogalgaz`, `ruzgar`, `barajli`, `gunes`, ...) taşıyor. Bu, ihtiyacımız olan gerçek, şirket bazlı piyasa verisi — GİP'in kendisi değil, ama şirketlerin üretim planlaması/uzlaştırma davranışının somut, ölçülebilir bir proxy'si.

### Mevcut durum ve eksik parçalar

- `get_dpp_bulk()` **şu anki haliyle çalışmıyor**: `KgupBulkRequestDto` `date` (tekil gün, startDate/endDate değil) + `region` + `uevcbIds` (zorunlu, ≤1000, tekrarsız) istiyor. Mevcut kod `_date_body(start_date, end_date)` (yani `{startDate, endDate}`) gönderiyor — şema uyuşmuyor. Hiçbir DAG'da çağrılmadığı için (`dags/` içinde `get_dpp_bulk` referansı yok) bu hiç ortaya çıkmamış.
- `get_uevcb_list()` zaten günlük pipeline'da wired (`EPIAS_SOURCES["uevcb_list"]`, `daily_eligible=True`, `backfill_eligible=False`) — `get_market_participants()`'tan gelen org id'lerini 100'lük gruplar halinde `uevcb-list-bulk`'a gönderip org↔UEVÇB eşlemesini zaten üretiyor. Ama bu eşleme hiçbir yerde Silver/Gold'a taşınmıyor, sadece bronze'a yazılıp bırakılıyor.
- `KgupBulkDataDto.orgId`'nin referans listesi olarak EPİAŞ dokümantasyonu **"Organizasyon Listesi Getirme Servisi"** (`get_organization_list()` → `/v1/generation/data/organization-list`) işaret ediyor — `stg_participants` değil. `stg_participants` şu an `market-participants` (GÖP/GİP/VEP/YEK-G üyeliği) kaynaklı; üretim lisansı olan ama piyasa üyeliği farklı olan şirketler için isim eşleşmesi eksik kalabilir. `organization-list` şu an `EPIAS_SOURCES`'ta hiç yok (sadece client'ta method var).
- İlgili mevcut özellik: `mart_production_plan.sql` zaten "revizyon" hesaplıyor ama (a) Türkiye geneli agregat, şirket kırılımı yok, (b) `stg_sbfgp`'nin backfill'i tamamlanmadığı için `DBT_EXCLUDE_PENDING_BACKFILL`'de — yani dbt run'larından hâlâ hariç tutuluyor, muhtemelen boş. Yeni özellik bunun yerine geçmiyor, üstüne şirket kırılımı ekliyor.

## Decision

Şirket bazlı "piyasa aktivitesi" özelliğini **GİP işlem verisi yerine iki gerçek, doğrulanmış kaynak üzerinden** inşa et:

- **Faz 1 — GÖP şirket bazlı eşleşme miktarı** (`clearing-quantity` + `organizationId` filtresi): "hangi şirket GÖP'te ne kadar alım/satım eşleştiriyor, günden güne nasıl değişiyor" — GÖP'ün (ana spot piyasa) kendisi, gerçek hacim.
- **Faz 2 — KGÜP-bulk üretim planı aktivitesi**: "hangi şirket ne kadar üretim planlıyor, plan gün içinde nasıl revize ediliyor."

Ölü `mart_gip_company_analysis.sql` + dashboard'daki "🏢 Şirket Bazlı GİP Aktivitesi" bölümünü retire et; aynı yeri, Faz 1'in doğru veriye dayanan ve doğru etiketlenmiş (GİP değil, GÖP) yeni martı ile değiştir.

## Faz 1: GÖP Şirket Bazlı Eşleşme Miktarı (canlı doğrulandı)

**Endpoint aile:**
- `POST /v1/markets/dam/data/clearing-quantity-organization-list` — `{period}` → roster (`organizationId`, `organizationName`, `eic`, `shortName`, `organizationStatus`). Zaten `epias_client.py`'de yok, eklenmeli.
- `POST /v1/markets/dam/data/clearing-quantity` — zaten `get_dam_clearing_quantity()` olarak wired (`EPIAS_SOURCES["dam_clearing"]`, Türkiye-geneli). Request DTO'su **opsiyonel `organizationId`** filtresi kabul ediyor ("Tüm Liste için bkz: Göp Eşleşme Miktarı Organizasyon Listeleme Servisi" — yani iki endpoint'in birlikte kullanılması EPİAŞ tarafından da öngörülmüş). Response (`ClearingQuantityDataDto`): `date`, `hour`, `matchedBids`, `matchedOffers` — organizationId satırda geri dönmüyor, çağıran taraf enjekte etmeli (KGÜP'teki `KgupRequestDto`ile aynı desen).

**Canlı test sonucu (2026-08-09, `python` ile gerçek TGT/credential kullanılarak):**
- `period=2026-08-05` için roster **1629 organizasyon** döndü.
- Örnek org (id=10374, "1461 TRABZON ELEKTRİK ÜRETİM A.Ş") için `organizationId` filtreli sorgu 24 satır (saatlik) döndü — o gün için hepsi 0 (şirket o gün GÖP'te pozisyon almamış, beklenen davranış).
- Aynı gün filtresiz (Türkiye-geneli) sorgu ile karşılaştırıldı: saat 00:00 için 27178 MWh — mevcut `mart_ptf_*` serilerindeki büyüklükle tutarlı, sağlaması yapıldı.
- **Sonuç: endpoint gerçek, dokümante edildiği gibi çalışıyor, veri mantıklı.**

**Maliyet/tasarım kısıtı:** `clearing-quantity`'nin KGÜP-bulk'un aksine toplu/batch modu yok — 1629 şirketin hepsini almak 1629 ayrı POST demek. Bu depoda zaten belgelenen ~80 req/dk limitine göre (`get_uevcb_list()`'in kendi yorumu) bu iş **~20 dakika/gün** sürer. Bu yüzden:
- Bu kaynak **sadece günlük** olmalı (`daily_eligible=True`, `daily`'de hourly kritik yola girmemeli — `uevcb_list`/`sbfgp`/`participants` ile aynı sınıf).
- Mevcut `_post()`'un retry/backoff mekanizması zaten var; ek olarak çağrılar arası `time.sleep` ile 80 req/dk'nın altında kalınmalı (mevcut `get_uevcb_list()`'teki `time.sleep(0.5)` deseniyle aynı ruh, ama batch değil tekil çağrı olduğu için her çağrı arası bekleme).
- Çoğu şirket çoğu gün için 0 satır dönecek (1461 Trabzon örneğinde olduğu gibi) — bu gürültü değil, gerçek sinyal ("bu şirket bugün pasif").

## Options Considered

### Option A: KGÜP-Bulk şirket bazlı aktivite (Faz 2, Önerilen)

| Boyut | Değerlendirme |
|---|---|
| Veri gerçekliği | Gerçek, EPİAŞ tarafından doğrulanmış, `orgId` taşıyan veri |
| Karmaşıklık | Orta — yeni bronze/silver/staging/mart adımı, mevcut pattern'e tam uyumlu |
| Değer | Yüksek — "hangi şirketler piyasada en aktif/en çok revize ediyor" sorusuna gerçek cevap |
| Maliyet | Günde birkaç ekstra bulk API çağrısı (UEVÇB sayısı 1000'i aşarsa >1 batch) |

**Artı:** Var olan `uevcb_list`/`organization-list` altyapısını tamamlar; `mart_production_plan`'ın doğal şirket-seviyesi genişlemesi.
**Eksi:** GİP'in kendisi (fiyat/hacim, kimin alıp sattığı) hâlâ görünmez kalır — kullanıcı beklentisi buna göre ayarlanmalı.

### Option B: Sadece roster (GÖP org-list + market-participants)

Şirketlerin hangi piyasalara üye olduğunu gösterir ama hacim/aktivite taşımaz. Düşük değer, "aktivite" adını hak etmiyor.

### Option C: Ölü mart'ı sessizce bırak, yeni özellik yok

Dürüst ama isteğe cevap vermiyor.

### Option D: EPİAŞ üye/kayıtlı erişimi

Kullanıcının böyle bir erişimi olmadığı teyit edildi (bu ADR'ın gerekçe bölümünde ele alınan soruya verilen yanıtla) — elendi.

## Trade-off Analysis

Asıl gerilim, "istenen" (GİP şirket aktivitesi) ile "mümkün olan" (KGÜP şirket aktivitesi) arasında. EPİAŞ'ın kamuya açık API'si bu boşluğu asla kapatmayacak (müzayede anonimliği kasıtlı) — bu yüzden Option A, isteğin *ruhuna* ("piyasada şirketlerin davranışını anlamak") en yakın, gerçekten inşa edilebilir seçenek. Dashboard'da GİP ile karıştırılmaması için net etiketleme şart.

## Consequences

- Gerçek, EPİAŞ doğrulamalı şirket bazlı üretim planı verisi elde edilir; "en çok revize eden / en büyük plan yapan şirketler" gibi analizler mümkün olur.
- `mart_production_plan`'daki Türkiye-geneli revizyon konsepti şirket seviyesine iner.
- GİP'in kendisi (fiyat/hacim, alıcı-satıcı) hâlâ şirket bazında görünmez — bu asla değişmeyecek, dashboard'da net belirtilmeli.
- Günlük pipeline'a yeni bir API yükü eklenir (UEVÇB sayısına bağlı, muhtemelen birkaç bulk çağrı/gün) — implementasyon sırasında gerçek UEVÇB sayısı ölçülüp `DATAPROC_POOL`/rate-limit etkisi değerlendirilmeli.
- `dpp`/`sbfgp`/`dpp-first-version` isimlendirme karışıklığı bu ADR ile çözülmüyor, ayrı bir görev olarak bırakılıyor — bu ADR'ın implementasyonu sırasında en azından *yeni* kod doğru isimlendirmeyle yazılmalı (KGÜP-bulk, BGÜP değil).

## Action Items — Faz 1 (GÖP) — kod tamamlandı, deploy bekliyor

1. [x] `epias_client.py`: `get_dam_clearing_quantity_organizations(period)` + `get_dam_clearing_quantity_by_organization(start_date, end_date)` — canlı testte 1629 org, 3-org smoke test PASS
2. [x] `dags/epias_gop_company_activity_dag.py` (yeni, ayrı DAG — hourly'ye eklenmedi, bkz. dosyanın docstring'i): `schedule_interval="30 11 * * *"`, fetch+save+silver-batch
3. [x] `dags/epias_sources.py`: `dam_clearing_by_org` girdisi eklendi (`daily_eligible=False` — DRY için sadece method_name/gcs_path kaynağı, zamanlama yukarıdaki ayrı DAG'da)
4. [x] `spark_jobs/bronze_to_silver_dam_clearing_by_org.py` (yeni) — `bronze_to_silver_sbfgp.py` deseniyle
5. [x] `epias_dbt/models/staging/sources.yml` + `schema.yml`'e yeni silver kaynağı + testler
6. [x] `epias_dbt/models/staging/stg_dam_clearing_by_org.sql`
7. [x] `epias_dbt/models/marts/mart_company_gop_activity.sql`
8. [x] Dashboard: ölü "🏢 Şirket Bazlı GİP Aktivitesi" bölümü `mart_company_gop_activity` ile değiştirildi, GÖP olduğu netleştirildi
9. [x] Temizlik: kalıcı olarak ölü `mart_gip_company_analysis.sql` silindi; `stg_idm_transactions.sql` ve `bronze_to_silver_idm_transactions.py`'deki hiç var olmamış `buyerOrganizationId`/`sellerOrganizationId` referansları kaldırıldı

**Deploy edilmedi — kalan adımlar (manuel/altyapı, bu oturumda yapılmadı):**
- `spark_jobs/bronze_to_silver_dam_clearing_by_org.py` (+ `spark_utils.py`) `gs://epias-data-lake/dataproc/jobs/`'a senkronize edilmeli (mevcut deploy sürecinizle — repoda otomatik bir deploy script bulunamadı)
- Yeni DAG dosyası (`epias_gop_company_activity_dag.py`) GCE VM'deki Airflow `dags/` klasörüne yayılmalı
- İlk `dbt run --select stg_dam_clearing_by_org mart_company_gop_activity` DAG en az bir kez çalışıp Silver tablosu oluştuktan sonra

## Action Items — Faz 2 (KGÜP) — kod tamamlandı, deploy bekliyor

1. [x] `epias_client.py`: `get_dpp_bulk` gerçek `KgupBulkRequestDto` şemasına göre düzeltildi (`date` tekil gün + `region="TR1"` + `uevcbIds` ≤1000/batch); `get_organization_list` de aynı oturumda düzeltildi (canlı testte 400 döndüğü tespit edildi — boş body gönderiyordu, `startDate`/`endDate` zorunluymuş)
2. [x] `get_kgup_bulk_by_organization(start_date, end_date)` — yeni orkestratör: organization-list (706 org, canlı doğrulandı) → uevcb-list-bulk (~1900 UEVÇB) → dpp-bulk, günlük döngü. Smoke test PASS (5-org slice, 192 satır, hepsi orgId/uevcbId taşıyor)
3. [x] `dags/epias_sources.py`: `kgup_bulk_by_org` girdisi eklendi — `daily_eligible=True`, mevcut hourly loop'a katıldı (Faz 1'in aksine: ~10 batched çağrı/gün, saniyeler sürüyor, `dpp` kaynağıyla aynı emsal — ayrı DAG'a gerek yoktu)
4. [x] `spark_jobs/bronze_to_silver_kgup_bulk_by_org.py`, `epias_dbt/models/staging/{sources,schema}.yml` + `stg_kgup_bulk_by_org.sql`
5. [x] `epias_dbt/models/marts/mart_company_production_activity.sql` — günlük şirket bazlı toplam KGÜP + yakıt kırılımı + aktif santral sayısı
6. [x] Dashboard: "🏭 Üretim Planı (BGÜP vs KGÜP)" sayfasına (Page 10) "🏢 Şirket Bazlı KGÜP Aktivitesi" bölümü eklendi (bu sayfa madde 7 kapsamında "KGÜP vs KUDÜP" olarak yeniden adlandırıldı)
7. [x] `dpp`/`sbfgp`/`dpp-first-version` BGÜP/KGÜP/KUDÜP isimlendirme karışıklığını kod genelinde düzelt. Model/kaynak/fonksiyon adları (stg_dpp, stg_sbfgp, get_dpp, get_sbfgp, source `dpp`/`sbfgp`) değiştirilmedi — disruptive table rename kapsam dışı bırakıldı. Düzeltilenler: `src/epias_client.py` (get_dpp/get_sbfgp docstring'leri), `spark_jobs/bronze_to_silver_dpp.py` + `bronze_to_silver_sbfgp.py` (docstring + log satırları), `epias_dbt/models/staging/{stg_dpp.sql, stg_sbfgp.sql, sources.yml, schema.yml}`, `docs/epias_openapi.yaml` (/dpp, /dpp-bulk, /sbfgp özet/açıklamaları), `epias_dbt/models/marts/mart_production_plan.sql` (CTE + çıktı kolonları `bgup_*`→`kgup_*`, `kgup_*`→`kudup_*`, delta hâlâ aynı sinyal: KUDÜP−KGÜP), `dashboard.py` (Page 10 nav etiketi, başlık, sorgu kolonları, grafik başlıkları/etiketleri "KGÜP vs KUDÜP" olarak düzeltildi — mart'ın "Şirket Bazlı KGÜP Aktivitesi" bölümü zaten doğruydu, dokunulmadı).

**Deploy edilmedi (Faz 1 ile aynı gerekçe):** `spark_jobs/bronze_to_silver_kgup_bulk_by_org.py` `gs://epias-data-lake/dataproc/jobs/`'a senkronize edilmeli; kod GitHub'a henüz push edilmedi (kullanıcı onayı bekleniyor).
