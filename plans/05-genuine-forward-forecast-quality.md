# ADR-0005: Açık Artırma Kapanmadan Önce Gerçek Gün-Öncesi PTF Tahmini — Mimari Kalite Kararı

**Status:** Proposed
**Date:** 2026-08-07
**Deciders:** Mehmet Karahan Çetinkaya

## Context

Sistem zaten iki ayrı, birbirini tamamlayan yol üzerinden çalışıyor:

1. **K.PTF / final PTF** — GERÇEK GÖP sonucu. Açık artırma D-1 ~14:00 TRT'de kapanır kapanmaz K.PTF (itiraz öncesi) yayınlanıyor; final PTF ise teslim gününün kendi ~14:00'ünde. `mart_ptf_realized` ikisini `price_status` ile coalesce ediyor (bkz. `[[epias_api_kptf_finding]]`).
2. **XGBoost forward forecast** (`mart_ptf_forward_features` → `run_forward_forecast()`) — açık artırma HENÜZ kapanmamışken, sadece LEP/RES-tahmin/AIC gibi gün-öncesi verilerle üretilen kör tahmin. Bu, kullanıcının sorduğu "GÖP fiyatları daha yayınlanmadan önden tahminleyebilmek" sorusunun tam karşılığı — sistem bunu zaten yapıyor, soru bunun **kalitesi**.

**Bugün (2026-08-07) ilk gerçek, canlı doğrulama geldi:** Aug 8'in 20 saati, 12–31 saat önceden (ort. 21.5 saat) kör tahmin edildi, sonra `gold_ptf_forward_accuracy`'ye arşivlendi. Sonuç:

| Metrik | Değer |
|---|---|
| Model MAE | 722.53 TL/MWh |
| Naive T-24h MAE | 767.75 TL/MWh |
| **MASE** | **0.941** |

Bu oturumun daha önceki walk-forward backtest'inde ölçülen ~0.6 MASE'nin çok altında — model naive'i sadece %6 geçiyor. Tek günlük örneklem (n=20) kesin hüküm için yetersiz, ama sayı kendi başına bir "neden" araştırmasını haklı çıkarıyor.

**Kök neden hipotezi (kodda doğrulandı):** `mart_ptf_forward_features.sql`'in kendi docstring'i açıkça şunu söylüyor — `ptf_lag_1h`, `ptf_rolling_avg_24h/168h` gibi özellikler hedef saate göre gerçek T-1 değil, **tek bir "en son kesinleşmiş fiyat" anına dondurulup** (`latest_settled` CTE, `QUALIFY rn=1`) `CROSS JOIN` ile tüm tahmin batch'ine (3 saatten 31 saate kadar fark etmeksizin) aynen yayılıyor. Eğitim verisinde (`mart_ptf_lag_features`) ise aynı kolonlar her zaman gerçek T-1/T-24/T-168 laglerdir — SHAP importance'ta bu grafiğin en güçlü sinyalleri arasında (bu oturumda daha önce görüldü: `ptf_lag_1h`, `ptf_lag_24h` üst sıralarda). Yani model, eğitimde "çok taze ve çok güvenilir" olarak öğrendiği bir sinyalin, gerçek forward-inference'ta ufuk büyüdükçe (özellikle 12-31 saat) giderek bayatladığını **hiç bilmiyor** — klasik bir train/serve skew (covariate shift) durumu. Bu, gözlemlenen MASE düşüşünün en olası açıklaması.

**İkincil bulgu (kod incelemesinden, aynı gün):** "gerçek vs model" birleştirme mantığı 3 farklı yerde bağımsız olarak yeniden yazılmış (SQL anti-join `ptf_inference.py`, pandas `isin()` ML sayfası, pandas `merge` Vardiya sayfası) — mimari borç, doğruluk sorununun kendisi değil ama aynı kararla birlikte ele alınabilir.

## Decision

**Önerilen: Sorun 1 için Option C (ufka-göre decay, hızlı/ucuz) hemen uygula + veriyi biriktirirken Option A'yı (lead-time-aware retrain) paralel değerlendir. Sorun 2 için Option A (merkezi dbt mart).**

Gerekçe: `gold_ptf_forward_accuracy` bugün kuruldu ve şu an sadece 1 günlük veri var — Option A/B (retrain gerektiren) için "önce/sonra" karşılaştırması yapacak yeterli örneklem yok. Option C hem çok ucuz hem de retrain'i beklemeden bugün uygulanabilir, ve MASE'i gerçekten iyileştirip iyileştirmediği birkaç gün içinde `gold_ptf_forward_accuracy`'den doğrudan ölçülebilir — bu da Option A'ya geçip geçmeyeceğimize karar vermek için gereken veriyi üretir.

## Options Considered

### Sorun 1: Lead-time'a duyarsız (stale lag) model — asıl doğruluk sorunu

#### Option A: Lead_time_hours'ı feature olarak ekleyip retrain et
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Orta — eğitim verisine (mart_ptf_lag_features) simüle edilmiş "dondurulmuş lag" senaryoları enjekte etmek gerekir (walk-forward sırasında T-1/T-24/T-168'i bilerek bayatlatıp `lead_time_hours` feature'ıyla birlikte modele öğretmek) |
| Maliyet | $0 altyapı, orta emek (trainer'a yeni bir simülasyon adımı) |
| Etki | En doğru çözüm — EPF literatüründeki (Lago et al. vb.) standart yaklaşıma en yakın; model artık "bu sinyal N saat eski" bilgisini kullanarak kendi güvenini ayarlayabilir |

**Artılar:** Kök nedeni gerçekten çözer, tek model kalır (bakım yükü artmaz).
**Eksiler:** Retrain + yeni bir backtest metodolojisi gerektirir; sonucu görmek için `gold_ptf_forward_accuracy`'den birkaç haftalık veri lazım — bugün karşılaştırma yapılamaz.

#### Option B: Sadece-forward, ayrı bir model eğit
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Yüksek — `ptf_trainer.py` ikiye bölünür, iki GCS model artifact'ı, iki (ya da tek DAG'da iki adım) haftalık retrain |
| Maliyet | Ekstra GCS depolama + haftalık eğitim süresi 2x |
| Etki | Akademik pratiğe en sadık ("hiç aynı-gün actual görmemiş" bir model) ama bu proje ölçeğinde bakım yükü orantısız |

**Artılar:** En temiz ayrım, gelecekte iki modelin farklı hiperparametrelerle ayrı ayrı optimize edilmesine izin verir.
**Eksiler:** Solo-geliştirilen bir projede iki model senkron tutmak (feature seti driftlerini iki yerde takip etmek) gerçek bir sürdürülebilirlik riski.

#### Option C: Lag feature'ları ufka göre decay et (retrain gerekmez)
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Düşük — sadece `mart_ptf_forward_features.sql`'de bir SQL değişikliği |
| Maliyet | $0, retrain yok, bugün deploy edilebilir |
| Etki | Kaba ama yönü doğru: dondurulmuş `ptf_lag_1h`'ı, hedef saate uzaklığa göre `ptf_rolling_avg_168h`'a (uzun-vadeli, ufuktan bağımsız daha "güvenilir" bir referans) doğru üstel ağırlıklı ortalamayla karıştırmak — örn. `decay = EXP(-lead_hours/24)`, `adjusted_lag_1h = decay*frozen_lag_1h + (1-decay)*rolling_avg_168h` |

**Artılar:** Aynı gün test edilebilir, model retrain'e gerek yok, `gold_ptf_forward_accuracy` ile A/B ölçülebilir (decay öncesi/sonrası MASE karşılaştırması).
**Eksiler:** Model hâlâ "bu taze bir 1-saatlik lag" sanıyor — sinyali daha nötr hale getiriyoruz ama modelin kendi güven kalibrasyonunu düzeltmiyoruz; Option A'nın yerini almaz, sadece köprü çözüm.

### Sorun 2: "Gerçek vs model" birleştirme mantığının 3 kopyası

#### Option A: Merkezi dbt mart (`mart_ptf_forecast_outlook`)
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Düşük-Orta — yeni bir dbt model (`mart_ptf_realized` + `gold_ptf_forward_predictions`'ı `value_source` ('final'\|'interim'\|'model') ile coalesce eder) + iki dashboard sayfasının tek `SELECT` ile yeniden bağlanması |
| Etki | Python'da hiç coalesce mantığı kalmaz; bugünkü code review'ın bulduğu 3 kopya → 1 SQL view |

**Artılar:** dbt'nin zaten kanıtlanmış test/dedup disiplini (`unique_combination_of_columns` vb.) bu mart'a da otomatik uygulanır.
**Eksiler:** `gold_ptf_forward_predictions` dbt-yönetimli değil (Python/BigQuery client ile yazılıyor) — bu mart'ın her saat taze kalması için dbt run sırasının inference run'dan sonra gelmesi gerekir, küçük bir sıralama bağımlılığı ekler.

#### Option B: Sadece paylaşılan Python helper
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Düşük — dashboard.py'deki 2 kopya 1 fonksiyona iner, `ptf_inference.py`'deki SQL anti-join'e dokunulmaz |

**Artılar:** En az riskli, en hızlı.
**Eksiler:** 3 yerine 2 implementasyon kalır (SQL'de biri, Python'da biri) — tam merkezi değil, gelecekte üçüncü bir tüketici (örn. bir API) yine kendi versiyonunu yazma riski taşır.

## Trade-off Analysis

Sorun 1'in üç seçeneği bir spektrum: C (ucuz/hızlı/kısmi) → A (doğru/orta emek) → B (en temiz/en pahalı). Bu ölçekte bir proje için B'nin maliyeti (iki model senkronizasyonu) getirisini haklı çıkarmıyor — elenir. A ile C arasındaki seçim gerçekte bir sıralama sorunu: C'yi bugün uygulamak, A'nın gerekip gerekmediğine karar vermek için gereken ölçüm penceresini açar. İkisi çelişmiyor, C bir sona varış noktası değil, A'ya giden yolun ucuz bir ilk adımı.

Sorun 2'de A ile B arasındaki fark küçük ama gerçek: Vardiya sayfası zaten kendi (self-consistent ama farklı pencereli) mantığını taşıyor — B seçilirse bu farklılık Python seviyesinde kalıcı hale gelir; A seçilirse dbt mart'ın kendi WHERE'i tek doğruluk kaynağı olur ve iki sayfa da aynı pencereyi otomatik miras alır.

## Consequences

- **Kolaylaşan:** `gold_ptf_forward_accuracy` sayesinde artık her karar (decay parametresi, retrain zamanlaması) gerçek veriyle doğrulanabilir — varsayıma dayalı ayarlama yapmaya gerek yok.
- **Zorlaşan:** Option C devreye girdiğinde, dashboard'daki ham `ptf_lag_1h` etiketi artık "gerçek 1 saatlik lag" değil "ufka-göre ayarlanmış tahmini lag" anlamına gelecek — SHAP/feature-importance yorumlarken bu ayrımı hatırlamak gerekir.
- **Tekrar gözden geçirilmesi gereken:** `gold_ptf_forward_accuracy` 2-3 haftaya ulaştığında bu ADR'a dönüp Option A'ya (retrain) geçip geçmeyeceğine gerçek MASE trendiyle karar vermek.

## Action Items

1. [ ] Sorun 2, Option A: `mart_ptf_forecast_outlook.sql` (veya benzeri) dbt modelini yaz, dashboard'ın iki sayfasını buna bağla.
2. [ ] Sorun 1, Option C: `mart_ptf_forward_features.sql`'de `ptf_lag_1h`/`ptf_rolling_avg_*` için ufka-göre decay uygula, deploy et.
3. [ ] `gold_ptf_forward_accuracy`'yi haftalık gözden geçirme rutinine ekle (örn. haftada bir MASE trendini kontrol et).
4. [ ] ~2-3 hafta / yeterli örneklem (öneri: en az 10-14 gün, farklı volatilite rejimlerini kapsayacak şekilde) sonra: decay öncesi/sonrası MASE'i karşılaştır, Option A'ya (lead-time-aware retrain) geçip geçmeyeceğine karar ver.
5. [ ] Dashboard'a `gold_ptf_forward_accuracy`'yi gösteren bir panel eklemeyi değerlendir (lead-time bucket'larına göre MAE/MASE) — Action Item 3'ü manuel sorgudan UI'a taşır.
