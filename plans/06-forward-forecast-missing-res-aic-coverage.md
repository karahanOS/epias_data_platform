# ADR-0006: İleriye Dönük Tahminde Eksik RES/AIC Kapsaması — Baskın Hata Kaynağı

**Status:** Proposed
**Date:** 2026-08-08
**Deciders:** Mehmet Karahan Çetinkaya

## Context

ADR-0005 (2026-08-07), ilk gerçek forward-tahmin verisinde (MASE 0.941) donmuş `ptf_lag_1h`/rolling özelliklerin ufka göre bayatlamasını kök neden olarak teşhis etmiş ve bir decay stopgap uygulamıştı. Bugün elde edilen daha fazla veri (40 arşivlenmiş tahmin, Aug 8-9) **farklı ve daha baskın bir hata kaynağını** ortaya çıkardı.

**Önemli not:** Bu 40 satırın tamamı, decay fix'inin deploy edildiği andan (2026-08-08 ~13:09 UTC) ÖNCE üretilmiş tahminler. Decay'in gerçek etkisi henüz hiç ölçülmedi (o andan beri yeni bir forward tahmin yazılmadı, çünkü ufkumuzun eriştiği her tarih zaten gerçek K.PTF/PTF ile kaplandı).

**Yeni bulgu — bias rastgele değil, rejime bağlı:**

| Gerçekleşen fiyat tercili | Ort. imzalı hata (tahmin − gerçek) | n |
|---|---|---|
| Düşük | **+1719.6** (model çöküşü göremiyor) | 15 |
| Orta | +157.6 | 12 |
| Yüksek | **−841.8** (model sıçramayı göremiyor) | 13 |

Bu, klasik bir "ortalamaya regresyon" imzası — model, gerçek uç değerlerden bağımsız olarak sürekli ~2000-3100 TL/MWh bandında tahmin üretiyor.

**Kök neden doğrudan doğrulandı:** Aug 9 tahmini üretilirken kullanılan `mart_ptf_forward_features`'da o günün **her saati için** `forecasted_res_mwh = 0` ve `capacity_utilization_ratio = NULL` çıkıyor. Ham EPİAŞ API'si doğrudan sorgulandığında Aug 9 için RES tahmininin (93 satır) ve AIC'in (24 satır) **zaten mevcut olduğu** görüldü — yani bu bir EPİAŞ yayın gecikmesi değil. `stg_res_forecast`/`stg_aic` Gold'da sadece Aug 8'e kadar uzanıyor, `stg_load_estimation` (LEP) ise Aug 9'a kadar — üç kaynak da aynı saatlik pipeline'da, aynı `delay=0` ile çekiliyor olmasına rağmen.

**Neden bu özellikle zararlı:** `mart_ptf_forward_features.sql`'in mevcut `COALESCE(rf.forecasted_res_mwh, 0)` fallback'i, eksik veriyi NÖTR bir varsayılana değil, **aktif olarak yanıltıcı** bir değere çeviriyor — "0 RES tahmini" modele "bugün yenilenebilir yok" diyor, halbuki fiyat çöktüğünde tipik açıklama tam tersi (yüksek RES/güneş üretimi, arz fazlası). Model, eğitim verisinde (`mart_ml_features`, hemen hemen her zaman gerçek RES/kapasite değerleriyle) bu özelliğe güvenmeyi öğrenmişken, forward tahminde bu sinyal sistematik olarak yanlış yönü işaret ediyor.

## Decision

**Önerilen: İki bağımsız, düşük maliyetli düzeltmeyi birlikte uygula.**

1. `mart_ptf_forward_features.sql`'deki `COALESCE(...,0)` fallback'ini, ADR-0005'in decay mantığına benzer şekilde, saatlik/mevsimsel bir referans ortalamaya doğru değiştir — "0" yerine "bu saat için tipik" bir değer.
2. `stg_res_forecast`/`stg_aic`'in neden `stg_load_estimation`'ın bir gün gerisinde kaldığını araştır — aynı pipeline'da, aynı gecikme ayarıyla çekiliyorlarsa bu ya geçici bir zamanlama yarışı ya da yapısal bir hata olmalı.

Gerekçe: (1) bugün, retrain beklemeden deploy edilebilir ve modelin gördüğü sinyali en azından "yanlış yönlü" olmaktan çıkarır. (2) kök nedeni kapatırsa (1) zaten gereksiz hale gelebilir — ama (1) yine de savunma hattı olarak kalmalı, çünkü RES/AIC ne zaman "sadece 1 gün öncesine kadar hazır" bir kaynağa dönüşürse (EPİAŞ tarafında bir değişiklikle), aynı sorun sessizce geri gelir.

## Options Considered

### Sorun A: `COALESCE(...,0)` yanıltıcı fallback

#### Option A1: Saatlik rolling ortalamaya COALESCE (Önerilen)
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Düşük — `mart_ptf_forward_features.sql`'de tek bir CTE değişikliği (aynı saat için son N günün ortalama `forecasted_res_mwh`/`capacity_utilization_ratio`'sunu hesapla, eksikse onu kullan) |
| Etki | "0" yerine "bu saat için tipik" değer — model artık aktif olarak yanlış yönlendirilmiyor, en kötü ihtimalle nötr |

**Artılar:** Retrain gerekmez, bugün deploy edilebilir, ADR-0005'in decay felsefesiyle tutarlı.
**Eksiler:** Hâlâ bir tahmin (gerçek RES tahmini değil) — genuine sinyal kaybı devam ediyor, sadece zararı azaltıyor.

#### Option A2: Eksikse tahmin yapma (o saati atla)
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Düşük — `extract_forward_features()`'a `forecasted_res_mwh IS NOT NULL` filtresi ekle |
| Etki | Yanlış sinyalle tahmin üretmeyi tamamen engeller |

**Artılar:** En "dürüst" seçenek — bilmediğimiz bir şeyi tahmin etmiyoruz.
**Eksiler:** Sorun B çözülene kadar (RES/AIC her zaman 1 gün geride kalıyorsa) sistem NEREDEYSE HİÇ forward tahmin üretemez hale gelir — kullanıcının orijinal "GÖP yayınlanmadan önce tahmin" hedefini boşa çıkarır.

### Sorun B: RES/AIC neden LEP'in gerisinde kalıyor

#### Option B1: Kök nedeni araştır ve düzelt
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Bilinmiyor — muhtemelen düşük (bir Dataproc batch grubu veya bronze fetch sırası sorunu) ama önce teşhis gerekiyor |
| Etki | Sorun A'yı büyük ölçüde gereksiz kılar — RES/AIC gerçekten LEP kadar güncel olursa model gerçek sinyali görür |

**Artılar:** Kalıcı, doğru çözüm.
**Eksiler:** Zaman gerektirir, bugün hemen sonuç vermeyebilir.

#### Option B2: Kabul et, sadece Option A1'e güven
| Boyut | Değerlendirme |
|---|---|
| Karmaşıklık | Sıfır ek iş |

**Artılar:** Hızlı.
**Eksiler:** Kök nedeni asla çözmez, RES/AIC verisi sürekli 1 gün eski kalır.

## Trade-off Analysis

Sorun A ve B birbirini dışlamıyor — A1 hemen bugün savunma hattı kurar, B1 paralel olarak asıl nedeni araştırır. A2 (atla) çekici görünse de mevcut B durumuyla (RES/AIC sistematik 1 gün geride) sistemi kullanışsız kılma riski taşıyor — B1 sonuçlanana kadar ertelenmeli.

## Consequences

- **Kolaylaşan:** Model artık "0 RES = yenilenebilir yok" gibi aktif yanlış bir öncülle çalışmayacak.
- **Zorlaşan:** `mart_ptf_forward_features`'a bir "fallback kalitesi" katmanı daha eklenmiş olacak (ADR-0005'in decay'i + bu ADR'ın COALESCE düzeltmesi) — ileride ikisini tek bir tutarlı "belirsizlik altında day-ahead feature inşası" prensibiyle birleştirmek gerekebilir.
- **Tekrar gözden geçirilmesi gereken:** Option B1 sonuçlandıktan ve/veya decay fix'inin kendi post-deploy verisi biriktikten sonra, ADR-0005'in Option A/B (retrain) kararının hâlâ gerekli olup olmadığını yeniden değerlendir.

## Action Items

1. [ ] `mart_ptf_forward_features.sql`: `forecasted_res_mwh`/`capacity_utilization_ratio` için saatlik rolling-ortalama fallback (Option A1), deploy et.
2. [ ] `stg_res_forecast`/`stg_aic`'in `stg_load_estimation`'ın gerisinde kalma nedenini araştır (Option B1) — Dataproc batch grubu, bronze fetch sırası, ya da EPİAŞ API'sinin kendisinde bir zamanlama farkı olabilir.
3. [ ] Bir dbt test/Airflow kontrolü ekle: `mart_ptf_forward_features`'ın RES/AIC kapsaması, kendi LEP kapsamasından N saatten fazla geride kalırsa uyar — bu sınıf sorunun bir daha sessizce fark edilmeden birikmemesi için.
4. [ ] Bu düzeltmelerden sonra biriken YENİ `gold_ptf_forward_accuracy` verisiyle bias'ın (özellikle tercil-bazlı) gerçekten küçülüp küçülmediğini ölç.
