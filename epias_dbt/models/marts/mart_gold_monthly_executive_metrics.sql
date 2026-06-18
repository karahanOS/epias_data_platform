{{ config(materialized='table') }}

WITH base_metrics AS (
    SELECT
        EXTRACT(YEAR FROM p.date) AS year,
        EXTRACT(MONTH FROM p.date) AS month,
        ROUND(AVG(p.ptf_try), 2) AS avg_ptf,
        ROUND(MAX(p.ptf_try), 2) AS max_ptf,
        ROUND(MIN(p.ptf_try), 2) AS min_ptf,
        ROUND(AVG(p.price_spread), 2) AS avg_price_spread,
        COUNTIF(sd.system_direction = 'ENERGY_DEFICIT') AS energy_deficit_hours,
        COUNTIF(sd.system_direction = 'ENERGY_SURPLUS') AS energy_surplus_hours
    FROM {{ ref('mart_price_analysis') }} p
    LEFT JOIN {{ ref('stg_system_direction') }} sd ON p.date = sd.date AND p.hour = sd.hour
    GROUP BY 1, 2
), 

forecast_metrics AS (
    SELECT 
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(forecasted_load_mwh), 2) AS avg_forecast_consumption
    FROM {{ ref('mart_forecasted_residual_load') }}
    GROUP BY 1, 2
)

SELECT
    p.*,
    f.avg_forecast_consumption,
    CONCAT(CAST(p.year AS STRING), '-', LPAD(CAST(p.month AS STRING), 2, '0')) AS year_month,
    CASE 
        WHEN p.month IN (12, 1, 2) THEN 'Kış'
        WHEN p.month IN (3, 4, 5) THEN 'İlkbahar'
        WHEN p.month IN (6, 7, 8) THEN 'Yaz'
        ELSE 'Sonbahar'
    END AS season
FROM base_metrics p
LEFT JOIN forecast_metrics f ON p.year = f.year AND p.month = f.month
ORDER BY p.year DESC, p.month DESC