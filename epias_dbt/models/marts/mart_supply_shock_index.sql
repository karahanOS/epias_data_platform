{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

WITH daily_outages AS (
    SELECT date, SUM(outage_capacity_mwh) AS total_outage_mwh 
    FROM {{ ref('stg_outages') }} 
    GROUP BY 1
),
daily_aic AS (
    SELECT date, SUM(total_aic_mwh) AS total_available_capacity_mwh 
    FROM {{ ref('stg_aic') }} 
    GROUP BY 1
)
SELECT
    o.date,
    o.total_outage_mwh,
    a.total_available_capacity_mwh,
    SAFE_DIVIDE(o.total_outage_mwh, a.total_available_capacity_mwh) AS supply_shock_index
FROM daily_outages o
LEFT JOIN daily_aic a ON o.date = a.date