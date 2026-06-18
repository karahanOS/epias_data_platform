{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_ml_features: PTF Tahmin Modeli Feature Store
--
-- Literatür temelli feature grupları (Tschora et al. 2022, Applied Energy;
-- Karatekin & Başaran, Türkiye GÖP; Maciejowska et al., Energies):
--
--   1. Fiyat özellikleri    — PTF, SMF, PTF-SMF spread
--   2. Yük & RES            — LEP tahmini, RES tahmini, residual yük
--   3. Piyasa hacimleri     — GÖP alış/satış hacmi, GÖP imbalance (top feature in lit.)
--   4. Kapasite             — AIC, arıza → kapasite kullanım oranı
--   5. Üretim karışımı      — Gerçekleşen yenilenebilir oranı
--   6. İmbalans sinyali     — Net imbalance (DGP baskısı)
--   7. GİP spread sinyali   — GİP-GÖP fiyat farkı (Maciejowska et al.)
--   8. Zaman özellikleri    — EXTRACT kolonları; sin/cos Python tarafında eklenir
-- ─────────────────────────────────────────────────────────────────────────────

WITH pricing AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }}
),
smf AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
),
load_est AS (
    SELECT date, hour, forecasted_load_mwh FROM {{ ref('stg_load_estimation') }}
),
res_forecast AS (
    SELECT date, hour, forecasted_res_mwh FROM {{ ref('stg_res_forecast') }}
),
outages AS (
    SELECT date, SUM(outage_capacity_mwh) AS total_outage_mwh
    FROM {{ ref('stg_outages') }}
    GROUP BY 1
),
-- ── GÖP Piyasa Hacmi & Imbalance ─────────────────────────────────────────
-- Tschora et al.: buy/sell volume imbalance consistently top-3 feature for DAM EPF
dam AS (
    SELECT
        date, hour,
        matched_bids_mwh,
        matched_offers_mwh,
        -- Pozitif → alış fazlası (talep > arz), negatif → satış fazlası
        (matched_bids_mwh - matched_offers_mwh) AS gop_volume_imbalance_mwh
    FROM {{ ref('stg_dam_clearing') }}
),
-- ── Emre Amade Kapasite (AIC) ────────────────────────────────────────────
-- Kapasite kullanım oranı = residual yük / AIC → piyasa sıkışıklığı göstergesi
aic AS (
    SELECT date, hour, total_aic_mwh
    FROM {{ ref('stg_aic') }}
),
-- ── Gerçekleşen Üretim Karışımı ──────────────────────────────────────────
-- Yenilenebilir (rüzgar+güneş+akarsu) / toplam üretim oranı
-- Literature: merit-order effect → yüksek RES penetrasyonu PTF'i aşağı iter
-- Ayrıca gaz ve baraj üretimi PTF tahmininde önemli merit-order sinyalleridir.
gen AS (
    SELECT
        date, hour,
        COALESCE(wind_generation_mwh, 0)                AS wind_generation_mwh,
        COALESCE(solar_generation_mwh, 0)               AS solar_generation_mwh,
        COALESCE(dam_generation_mwh, 0)
            + COALESCE(river_generation_mwh, 0)         AS hydro_generation_mwh,
        COALESCE(gas_generation_mwh, 0)                 AS gas_generation_mwh,
        total_generation_mwh,
        COALESCE(wind_generation_mwh, 0)
            + COALESCE(solar_generation_mwh, 0)
            + COALESCE(river_generation_mwh, 0)
            + COALESCE(dam_generation_mwh, 0)           AS actual_renewable_mwh,
        SAFE_DIVIDE(
            COALESCE(wind_generation_mwh, 0)
                + COALESCE(solar_generation_mwh, 0)
                + COALESCE(river_generation_mwh, 0)
                + COALESCE(dam_generation_mwh, 0),
            total_generation_mwh
        )                                               AS actual_renewable_ratio
    FROM {{ ref('stg_generation') }}
),
-- ── Gerçekleşen Tüketim ───────────────────────────────────────────────────
-- Talep sürprizi = gerçekleşen − tahmin → güçlü PTF sinyali
consumption AS (
    SELECT date, hour, actual_consumption AS actual_consumption_mwh
    FROM {{ ref('stg_load_vs_actual') }}
),
-- ── Hava Durumu (Ulusal Ortalama) ─────────────────────────────────────────
-- Şehir bazlı verinin ulusal ortalaması; sıcaklık ısıtma/soğutma yükünü,
-- rüzgar hızı rüzgar üretimini, güneş radyasyonu güneş üretimini etkiler.
-- stg_weather granülüne göre birden fazla şehir olabilir → AVG alıyoruz.
weather_nat AS (
    SELECT
        date, hour,
        AVG(temperature_celsius)   AS temperature_celsius,
        AVG(wind_speed_kmh)        AS wind_speed_kmh,
        AVG(shortwave_radiation)   AS shortwave_radiation,
        AVG(relative_humidity)     AS relative_humidity
    FROM {{ ref('stg_weather') }}
    GROUP BY 1, 2
),
-- ── Net İmbalans (DGP Baskısı) ────────────────────────────────────────────
imbalance AS (
    SELECT date, hour, net_imbalance_mwh
    FROM {{ ref('stg_imbalance') }}
),
-- ── GİP-GÖP Fiyat Farkı (Spread Sinyali) ────────────────────────────────
-- Maciejowska et al.: GİP-DAM spread predicts next-hour PTF direction
-- Günlük ortalama GİP fiyatı (saatlik GİP verisi mart_gip_company_activity'de)
gip_daily AS (
    SELECT
        trade_date,
        AVG(avg_transaction_price_try) AS avg_gip_price_try,
        SUM(total_volume_mwh)          AS total_gip_volume_mwh
    FROM {{ ref('mart_gip_company_activity') }}
    GROUP BY 1
),
-- ── USD/TRY Kur Özellikleri ───────────────────────────────────────────────
-- Doğal gaz ithalat maliyeti ve genel enflasyon baskısı için proxy.
-- Günlük tane — pricing'in saatlik granülüne date JOIN ile genişletilir.
-- Lag penceresi: T-1d (gaz fiyat şoklarının bir günlük gecikmesi),
--                T-7d (haftalık trend sinyali).
fx_features AS (
    SELECT
        date,
        usdtry,
        LAG(usdtry, 1) OVER (ORDER BY date)  AS usdtry_lag_1d,
        LAG(usdtry, 7) OVER (ORDER BY date)  AS usdtry_lag_7d,
        SAFE_DIVIDE(
            usdtry - LAG(usdtry, 1) OVER (ORDER BY date),
            LAG(usdtry, 1) OVER (ORDER BY date)
        )                                    AS usdtry_pct_change_1d
    FROM {{ ref('stg_fx_rates') }}
),
-- ── Çapraz Piyasa Lag Sinyalleri (Maciejowska et al. temel bulgu) ────────
-- "Dünün GİP-GÖP spread'i yarının PTF'ini yönlendirir" → T-24 lag
-- mart_cross_market_spread'den saatlik spread ve arbitraj skoru çekiyoruz.
-- Bu CTE mart_cross_market_spread'e ref vererek döngüsel bağımlılık oluşturmaz
-- çünkü mart_cross_market sadece stg_* tablolarına ref verir.
cross_lag AS (
    SELECT
        date,
        hour,
        gip_gop_spread_try,
        arbitrage_opportunity_score,
        yal_delivered_mwh,
        yat_delivered_mwh,
        net_dgp_mwh
    FROM {{ ref('mart_cross_market_spread') }}
)

SELECT
    p.date,
    p.hour,

    -- ── 1. Fiyat Özellikleri ──────────────────────────────────────────────
    p.ptf_try,
    s.smf_try,
    (s.smf_try - p.ptf_try)                         AS ptf_smf_spread,

    -- ── 2. Yük & RES ──────────────────────────────────────────────────────
    l.forecasted_load_mwh,
    r.forecasted_res_mwh,
    (l.forecasted_load_mwh - r.forecasted_res_mwh)  AS forecasted_residual_load_mwh,

    -- ── 3. GÖP Piyasa Hacimleri ───────────────────────────────────────────
    COALESCE(d.matched_bids_mwh, 0)                 AS gop_matched_bids_mwh,
    COALESCE(d.matched_offers_mwh, 0)               AS gop_matched_offers_mwh,
    COALESCE(d.gop_volume_imbalance_mwh, 0)         AS gop_volume_imbalance_mwh,

    -- ── 4. Kapasite Sıkışıklığı ───────────────────────────────────────────
    COALESCE(o.total_outage_mwh, 0)                 AS total_outage_mwh,
    COALESCE(a.total_aic_mwh, 0)                    AS total_aic_mwh,
    SAFE_DIVIDE(
        l.forecasted_load_mwh - r.forecasted_res_mwh,
        a.total_aic_mwh
    )                                               AS capacity_utilization_ratio,

    -- ── 5. Gerçekleşen Üretim Karışımı ───────────────────────────────────
    COALESCE(g.actual_renewable_mwh, 0)             AS actual_renewable_mwh,
    COALESCE(g.actual_renewable_ratio, 0)           AS actual_renewable_ratio,
    -- Bireysel kaynak türleri (merit order sıralaması için)
    COALESCE(g.wind_generation_mwh, 0)              AS wind_generation_mwh,
    COALESCE(g.solar_generation_mwh, 0)             AS solar_generation_mwh,
    COALESCE(g.hydro_generation_mwh, 0)             AS hydro_generation_mwh,
    COALESCE(g.gas_generation_mwh, 0)               AS gas_generation_mwh,
    COALESCE(g.total_generation_mwh, 0)             AS total_generation_mwh,

    -- ── 6. İmbalans Sinyali ───────────────────────────────────────────────
    COALESCE(imb.net_imbalance_mwh, 0)              AS net_imbalance_mwh,

    -- ── 7. GİP-GÖP Spread (günlük) ────────────────────────────────────────
    gd.avg_gip_price_try,
    gd.total_gip_volume_mwh,
    (gd.avg_gip_price_try - p.ptf_try)              AS gip_gop_price_spread,

    -- ── 8. Çapraz Piyasa Lag Sinyalleri (Maciejowday et al.) ──────────────
    -- T-24: bir önceki günün aynı saatinin GİP-GÖP spread'i
    cl.gip_gop_spread_try                          AS gip_gop_spread_lag24,
    cl.arbitrage_opportunity_score                 AS arb_score_lag24,
    -- DGP regülasyon baskısı (önceki gün)
    COALESCE(cl.yal_delivered_mwh, 0)              AS yal_lag24,
    COALESCE(cl.yat_delivered_mwh, 0)              AS yat_lag24,
    COALESCE(cl.net_dgp_mwh, 0)                   AS net_dgp_lag24,

    -- ── 9. Gerçekleşen Tüketim & Talep Sürprizi ─────────────────────────
    -- Talep sürprizi = gerçekleşen − tahmin; pozitif → beklentiden yüksek tüketim
    c.actual_consumption_mwh,
    (c.actual_consumption_mwh - l.forecasted_load_mwh) AS consumption_error_mwh,

    -- ── 10. Hava Durumu (Ulusal Ortalama) ────────────────────────────────
    w.temperature_celsius,
    w.wind_speed_kmh,
    w.shortwave_radiation,
    w.relative_humidity,

    -- ── 11. Zaman Bileşenleri (sin/cos Python'da eklenir) ────────────────
    EXTRACT(DAYOFWEEK FROM p.date)                  AS day_of_week,
    EXTRACT(MONTH FROM p.date)                      AS month,
    EXTRACT(YEAR FROM p.date)                       AS year,

    -- ── 12. Döviz Kuru (USD/TRY) — TCMB EVDS ────────────────────────────
    -- Doğal gaz maliyeti kanalı: gaz fiyatı $ bazlı → kur PTF'e gecikimli yansır.
    COALESCE(fx.usdtry, 0)                          AS usdtry,
    fx.usdtry_lag_1d,
    fx.usdtry_lag_7d,
    fx.usdtry_pct_change_1d

FROM pricing p
LEFT JOIN smf s           ON p.date = s.date AND p.hour = s.hour
LEFT JOIN load_est l      ON p.date = l.date AND p.hour = l.hour
LEFT JOIN res_forecast r  ON p.date = r.date AND p.hour = r.hour
LEFT JOIN outages o       ON p.date = o.date
LEFT JOIN dam d           ON p.date = d.date AND p.hour = d.hour
LEFT JOIN aic a           ON p.date = a.date AND p.hour = a.hour
LEFT JOIN gen g           ON p.date = g.date AND p.hour = g.hour
LEFT JOIN imbalance imb   ON p.date = imb.date AND p.hour = imb.hour
LEFT JOIN gip_daily gd    ON p.date = gd.trade_date
LEFT JOIN consumption c   ON p.date = c.date AND p.hour = c.hour
LEFT JOIN weather_nat w   ON p.date = w.date AND p.hour = w.hour
-- T-24 lag: bugünün saati için dünün aynı saatinin cross-market verisi
LEFT JOIN cross_lag cl    ON cl.date = DATE_SUB(p.date, INTERVAL 1 DAY)
                         AND cl.hour = p.hour
-- Günlük kur: aynı gün için tüm saatlere yayılır
LEFT JOIN fx_features fx  ON p.date = fx.date