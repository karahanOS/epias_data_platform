{{ config(materialized='table') }}

WITH ptf_metrics AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(ptf), 2) AS avg_ptf,
        ROUND(MAX(ptf), 2) AS max_ptf,
        ROUND(MIN(ptf), 2) AS min_ptf
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

spread_metrics AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(price_spread), 2) AS avg_price_spread,
        COUNTIF(system_direction = 'Enerji Açığı') AS energy_deficit_hours,
        COUNTIF(system_direction = 'Enerji Fazlası') AS energy_surplus_hours
    FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}
    GROUP BY 1, 2
)

SELECT
    p.year,
    p.month,
    CONCAT(CAST(p.year AS STRING), '-', LPAD(CAST(p.month AS STRING), 2, '0')) AS year_month,
    p.avg_ptf,
    p.max_ptf,
    p.min_ptf,
    c.total_consumption,
    c.avg_hourly_consumption,
    s.avg_price_spread,
    s.energy_deficit_hours,
    s.energy_surplus_hours
FROM ptf_metrics p
LEFT JOIN consumption_metrics c 
    ON p.year = c.year AND p.month = c.month
LEFT JOIN spread_metrics s 
    ON p.year = s.year AND p.month = s.month
ORDER BY p.year DESC, p.month DESC