{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH pricing AS (
    SELECT date, hour, ptf_try, smf_try FROM {{ ref('stg_pricing') }}
),
generation AS (
    SELECT 
        date,
        hour,
        total_generation_mwh,
        gas_generation_mwh,
        (COALESCE(wind_generation_mwh, 0) + COALESCE(solar_generation_mwh, 0) + COALESCE(river_generation_mwh, 0) + COALESCE(geothermal_generation_mwh, 0)) AS total_renewable_mwh
    FROM {{ ref('stg_generation') }}
),
dam_clearing AS (
    SELECT date, hour, matched_bids_mwh AS total_demand_mwh FROM {{ ref('stg_dam_clearing') }}
)

SELECT
    p.date,
    p.hour,
    p.ptf_try,
    p.smf_try,
    d.total_demand_mwh,
    g.total_generation_mwh,
    g.total_renewable_mwh,
    g.gas_generation_mwh,
    SAFE_DIVIDE(g.total_renewable_mwh, g.total_generation_mwh) * 100 AS renewable_penetration_pct,
    SAFE_DIVIDE(g.gas_generation_mwh, g.total_generation_mwh) * 100 AS gas_dependency_pct
FROM pricing p
LEFT JOIN generation g ON p.date = g.date AND p.hour = g.hour
LEFT JOIN dam_clearing d ON p.date = d.date AND p.hour = d.hour