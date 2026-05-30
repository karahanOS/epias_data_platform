{{ config(materialized='table') }}

SELECT
    date,
    hour,
    total_generation_mwh AS total_generation,
    -- Oranları burada hesaplıyoruz (Staging'de değil!)
    SAFE_DIVIDE((wind_generation_mwh + solar_generation_mwh + river_generation_mwh), total_generation_mwh) AS renewable_ratio,
    SAFE_DIVIDE(gas_generation_mwh, total_generation_mwh) AS fossil_ratio
FROM {{ ref('stg_generation') }}