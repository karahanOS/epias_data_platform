{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour,
    CAST(toplam AS FLOAT64) AS total_aic_mwh,
    CAST(dogalgaz AS FLOAT64) AS gas_aic_mwh,
    CAST(ruzgar AS FLOAT64) AS wind_aic_mwh,
    CAST(linyit AS FLOAT64) AS lignite_aic_mwh,
    CAST(tasKomur AS FLOAT64) AS hard_coal_aic_mwh,
    CAST(ithalKomur AS FLOAT64) AS imported_coal_aic_mwh,
    CAST(fuelOil AS FLOAT64) AS fueloil_aic_mwh,
    CAST(jeotermal AS FLOAT64) AS geothermal_aic_mwh,
    CAST(barajli AS FLOAT64) AS dam_aic_mwh,
    CAST(nafta AS FLOAT64) AS naphtha_aic_mwh,
    CAST(biokutle AS FLOAT64) AS biomass_aic_mwh,
    CAST(akarsu AS FLOAT64) AS river_aic_mwh,
    CAST(gunes AS FLOAT64) AS solar_aic_mwh
FROM {{ source('silver', 'aic') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}