{{ config(materialized='view') }}

WITH base_metrics AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(ptf), 2) AS avg_ptf,
        ROUND(MAX(ptf), 2) AS max_ptf,
        ROUND(MIN(ptf), 2) AS min_ptf,
        ROUND(AVG(price_spread), 2) AS avg_price_spread,
        COUNTIF(system_direction = 'Enerji Açığı') AS energy_deficit_hours,
        COUNTIF(system_direction = 'Enerji Fazlası') AS energy_surplus_hours
    FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}
    GROUP BY 1, 2
),
consumption_metrics AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(SUM(actual_consumption), 2) AS total_consumption,
        ROUND(AVG(actual_consumption), 2) AS avg_hourly_consumption
    FROM {{ source('epias_gold', 'gold_load_vs_actual') }}
    GROUP BY 1, 2
),
forecast_metrics AS (
    SELECT 
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(forecast_consumption), 2) AS avg_forecast_consumption
    FROM {{ source('epias_gold', 'gold_load_vs_actual') }}
    GROUP BY 1, 2
),
final_joined AS (
    SELECT
        p.*,
        c.total_consumption,
        c.avg_hourly_consumption,
        f.avg_forecast_consumption,
        CONCAT(CAST(p.year AS STRING), '-', LPAD(CAST(p.month AS STRING), 2, '0')) AS year_month,
        CASE 
            WHEN p.month IN (12, 1, 2) THEN 'Kış'
            WHEN p.month IN (3, 4, 5) THEN 'İlkbahar'
            WHEN p.month IN (6, 7, 8) THEN 'Yaz'
            ELSE 'Sonbahar'
        END AS season
    FROM base_metrics p
    LEFT JOIN consumption_metrics c ON p.year = c.year AND p.month = c.month
    LEFT JOIN forecast_metrics f ON p.year = f.year AND p.month = f.month
)
SELECT * FROM final_joined
ORDER BY year DESC, month DESC