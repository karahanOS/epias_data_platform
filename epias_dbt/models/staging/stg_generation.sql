{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(toplam AS FLOAT64) AS total_generation_mwh,
    CAST(ruzgar AS FLOAT64) AS wind_generation_mwh,
    CAST(jeotermal AS FLOAT64) AS geothermal_generation_mwh,
    CAST(rezervuarli AS FLOAT64) AS dam_generation_mwh,
    CAST(kanalTipi AS FLOAT64) AS canal_generation_mwh,
    CAST(nehirTipi AS FLOAT64) AS river_generation_mwh,
    CAST(copGazi AS FLOAT64) AS waste_gas_generation_mwh,
    CAST(biyogaz AS FLOAT64) AS biogas_generation_mwh,
    CAST(gunes AS FLOAT64) AS solar_generation_mwh,
    CAST(biyokutle AS FLOAT64) AS biomass_generation_mwh
FROM {{ source('silver', 'generation') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}