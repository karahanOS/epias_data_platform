{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

WITH unlicensed_hourly AS (
    SELECT
        date,
        SUM(total_unlicensed_mwh) AS total_unlicensed_mwh
        -- Not: Eğer stg_ modeline solar/wind kırılımlarını eklerseniz buraya da ekleyebilirsiniz
    FROM {{ ref('stg_unlicensed_generation') }}
    GROUP BY 1
),

pricing_hourly AS (
    SELECT
        date,
        AVG(ptf_try) AS ptf_try
    FROM {{ ref('stg_pricing') }}
    GROUP BY 1
),

unlicensed_impact AS (
    SELECT
        u.date,
        u.total_unlicensed_mwh,
        p.ptf_try,
        (u.total_unlicensed_mwh * p.ptf_try) AS estimated_market_value_try
    FROM unlicensed_hourly u
    LEFT JOIN pricing_hourly p ON u.date = p.date
)

SELECT * FROM unlicensed_impact