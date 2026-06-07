{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_forecast_error: EPİAŞ RES öngörü hatası analizi (YEP vs Gerçekleşen)
--
-- EPİAŞ /forecasting/wind-power-plant-generation-status endpoint'i hem
-- tahmin (forecast) hem gerçekleşen üretimi (actual) içerir.
-- stg_res_forecast.actual_res_generation_mwh = LEP enerji üretim planı
-- stg_generation.{wind,solar}_generation_mwh = RT gerçekleşen ÜAP/KGÜP
--
-- Hata metrikleri:
--   forecast_error_mwh   — tahmin − gerçekleşen  (pozitif = aşırı öngörü)
--   abs_error_mwh        — |forecast_error_mwh|
--   error_pct            — abs_error / actual  × 100 (MAPE bileşeni)
--   rmse_daily           — günlük RMSE (pencere: günün 24 saati)
--
-- Gerçekleşen RES üretimi = wind + solar (jeotermal ve diğer kaynaklar hariç,
-- çünkü forecast endpoint sadece RÜZGAR+GÜNEŞ projeksiyonu içerir).
-- ─────────────────────────────────────────────────────────────────────────────

WITH actual AS (
    SELECT
        date,
        hour,
        COALESCE(wind_generation_mwh, 0.0)  AS wind_mwh,
        COALESCE(solar_generation_mwh, 0.0) AS solar_mwh,
        COALESCE(wind_generation_mwh, 0.0)
            + COALESCE(solar_generation_mwh, 0.0) AS actual_res_mwh
    FROM {{ ref('stg_generation') }}
),

forecast AS (
    SELECT
        date,
        hour,
        forecasted_res_mwh,
        actual_res_generation_mwh AS epias_actual_res_mwh   -- EPİAŞ kendi aktüeli
    FROM {{ ref('stg_res_forecast') }}
),

joined AS (
    SELECT
        COALESCE(f.date, a.date)  AS date,
        COALESCE(f.hour, a.hour)  AS hour,
        f.forecasted_res_mwh,
        a.actual_res_mwh,
        a.wind_mwh,
        a.solar_mwh,
        f.epias_actual_res_mwh,

        -- Hata: tahmin − gerçekleşen
        f.forecasted_res_mwh - a.actual_res_mwh            AS forecast_error_mwh,
        ABS(f.forecasted_res_mwh - a.actual_res_mwh)       AS abs_error_mwh,

        -- % hata (MAPE bileşeni — sıfır gerçekleşen hariç)
        SAFE_DIVIDE(
            ABS(f.forecasted_res_mwh - a.actual_res_mwh),
            NULLIF(a.actual_res_mwh, 0.0)
        ) * 100                                             AS error_pct,

        -- Hata yönü
        CASE
            WHEN f.forecasted_res_mwh > a.actual_res_mwh THEN 'Aşırı Tahmin'
            WHEN f.forecasted_res_mwh < a.actual_res_mwh THEN 'Eksik Tahmin'
            ELSE 'Eşleşme'
        END AS error_direction

    FROM forecast f
    FULL OUTER JOIN actual a ON f.date = a.date AND f.hour = a.hour
)

SELECT
    j.*,

    -- Günlük RMSE (pencere: o günün 24 saatlik RMSE'si)
    ROUND(
        SQRT(
            AVG(POW(j.forecast_error_mwh, 2)) OVER (PARTITION BY j.date)
        ),
        2
    ) AS daily_rmse_mwh,

    -- Günlük MAPE
    ROUND(
        AVG(j.error_pct) OVER (PARTITION BY j.date),
        2
    ) AS daily_mape_pct

FROM joined j
