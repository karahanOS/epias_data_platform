{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
  )
}}

-- mart_res_intraday_volatility: Yenilenebilir enerji saat-içi oynaklık analizi
-- stg_res_forecast'tan saatlik tahmin hataları ve günlük volatilite metrikleri üretir.
--
-- forecast_error     = actual - forecasted (pozitif → beklenenden fazla üretim)
-- abs_error_mwh      = |actual - forecasted|
-- forecast_error_pct = hata / tahmin × 100
-- intraday_volatility_mwh = gün içinde maximum ile minimum forecasted_res_mwh farkı
-- Yüksek volatilite → balancing maliyetleri artar, PTF baskısı oluşur.

WITH hourly AS (
    SELECT
        date,
        hour,
        forecasted_res_mwh,
        actual_res_generation_mwh,
        -- Point-in-time forecast error
        actual_res_generation_mwh - forecasted_res_mwh  AS forecast_error_mwh,
        ABS(actual_res_generation_mwh - forecasted_res_mwh) AS abs_forecast_error_mwh,
        SAFE_DIVIDE(
            ABS(actual_res_generation_mwh - forecasted_res_mwh),
            NULLIF(forecasted_res_mwh, 0)
        ) * 100 AS forecast_error_pct
    FROM {{ ref('stg_res_forecast') }}
),

daily_stats AS (
    SELECT
        date,
        -- Intraday range of forecasted generation
        MAX(forecasted_res_mwh) - MIN(forecasted_res_mwh)          AS intraday_forecast_range_mwh,
        MAX(actual_res_generation_mwh) - MIN(actual_res_generation_mwh) AS intraday_actual_range_mwh,
        -- Daily MAE and MAPE
        AVG(abs_forecast_error_mwh)                                 AS daily_mae_mwh,
        AVG(forecast_error_pct)                                     AS daily_mape_pct,
        -- Signed bias (positive = systematic under-forecasting)
        AVG(forecast_error_mwh)                                     AS daily_bias_mwh,
        -- Peak hour identification
        MAX_BY(hour, actual_res_generation_mwh)                     AS peak_res_hour,
        MAX(actual_res_generation_mwh)                              AS peak_res_mwh
    FROM hourly
    GROUP BY date
)

SELECT
    h.date,
    h.hour,
    h.forecasted_res_mwh,
    h.actual_res_generation_mwh,
    h.forecast_error_mwh,
    h.abs_forecast_error_mwh,
    h.forecast_error_pct,
    -- Daily aggregates repeated per row (for easy dashboard filtering)
    d.intraday_forecast_range_mwh,
    d.intraday_actual_range_mwh,
    d.daily_mae_mwh,
    d.daily_mape_pct,
    d.daily_bias_mwh,
    d.peak_res_hour,
    d.peak_res_mwh,
    -- 7-day rolling MAE (trailing, window on daily_stats)
    AVG(d.daily_mae_mwh) OVER (
        ORDER BY h.date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_mae_mwh

FROM hourly h
JOIN daily_stats d ON h.date = d.date

{% if is_incremental() %}
WHERE h.date >= (SELECT DATE_SUB(MAX(date), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
