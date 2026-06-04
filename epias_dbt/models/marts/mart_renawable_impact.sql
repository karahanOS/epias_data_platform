{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH licensed_gen AS (
    SELECT 
        date,
        hour,
        total_generation_mwh AS total_licensed_mwh,
        (COALESCE(wind_generation_mwh, 0) + COALESCE(solar_generation_mwh, 0) + COALESCE(river_generation_mwh, 0) + COALESCE(geothermal_generation_mwh, 0) + COALESCE(biomass_generation_mwh, 0)) AS licensed_renewable_mwh
    FROM {{ ref('stg_generation') }} lg
),
unlicensed_gen AS (
    SELECT date, hour, total_unlicensed_mwh FROM {{ ref('stg_unlicensed_generation') }} ug
),
dam_demand AS (
    SELECT date, hour, matched_bids_mwh AS total_demand_mwh FROM {{ ref('stg_dam_clearing') }} d
),
pricing AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }} p
)

SELECT
    lg.date,
    lg.hour,
    p.ptf_try,
    d.total_demand_mwh,
    lg.licensed_renewable_mwh,
    COALESCE(ug.total_unlicensed_mwh, 0) AS total_unlicensed_mwh,
    (lg.licensed_renewable_mwh + COALESCE(ug.total_unlicensed_mwh, 0)) AS total_green_energy_mwh,
    (d.total_demand_mwh - (lg.licensed_renewable_mwh + COALESCE(ug.total_unlicensed_mwh, 0))) AS residual_load_mwh
FROM licensed_gen lg
LEFT JOIN unlicensed_gen ug ON lg.date = ug.date AND lg.hour = ug.hour
LEFT JOIN dam_demand d ON lg.date = d.date AND lg.hour = d.hour
LEFT JOIN pricing p ON lg.date = p.date AND lg.hour = p.hour