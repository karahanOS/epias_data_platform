{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'generation') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(total AS FLOAT64) AS total_generation_mwh,
    CAST(naturalGas AS FLOAT64) AS gas_generation_mwh,
    CAST(importedCoal AS FLOAT64) AS imported_coal_generation_mwh,
    CAST(lignite AS FLOAT64) AS lignite_generation_mwh,
    CAST(stoneCoal AS FLOAT64) AS stone_coal_generation_mwh,
    CAST(biomass AS FLOAT64) AS biomass_generation_mwh,
    CAST(fueloil AS FLOAT64) AS fueloil_generation_mwh,
    CAST(geoThermal AS FLOAT64) AS geothermal_generation_mwh,
    CAST(river AS FLOAT64) AS river_generation_mwh,
    CAST(dam AS FLOAT64) AS dam_generation_mwh,
    CAST(wind AS FLOAT64) AS wind_generation_mwh,
    CAST(solar AS FLOAT64) AS solar_generation_mwh,
    CAST(asphaltite AS FLOAT64) AS asphaltite_generation_mwh,
    CAST(naphtha AS FLOAT64) AS naphtha_generation_mwh,
    CAST(lng AS FLOAT64) AS lng_generation_mwh,
    CAST(internationalImport AS FLOAT64) AS international_import_mwh,
    CAST(internationalExport AS FLOAT64) AS international_export_mwh,
    CAST(other AS FLOAT64) AS other_generation_mwh
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}