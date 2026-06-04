{{ config(
    materialized='incremental', 
    unique_key=['date', 'hour'], 
    incremental_strategy='merge', 
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH deduped AS (
    SELECT
        CAST(date AS DATE) AS date,
        CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
        
        -- Kaynak Tablodaki Gerçek Kolonlar
        CAST(total AS FLOAT64) AS total_generation_mwh,
        CAST(naturalGas AS FLOAT64) AS gas_generation_mwh,
        CAST(dammedHydro AS FLOAT64) AS dam_generation_mwh,
        CAST(lignite AS FLOAT64) AS lignite_generation_mwh,
        CAST(river AS FLOAT64) AS river_generation_mwh,
        CAST(importCoal AS FLOAT64) AS imported_coal_generation_mwh,
        CAST(wind AS FLOAT64) AS wind_generation_mwh,
        CAST(sun AS FLOAT64) AS solar_generation_mwh,
        CAST(fueloil AS FLOAT64) AS fueloil_generation_mwh,
        CAST(geothermal AS FLOAT64) AS geothermal_generation_mwh,
        CAST(asphaltiteCoal AS FLOAT64) AS asphaltite_coal_generation_mwh,
        CAST(blackCoal AS FLOAT64) AS black_coal_generation_mwh,
        CAST(biomass AS FLOAT64) AS biomass_generation_mwh,
        CAST(naphta AS FLOAT64) AS naphtha_generation_mwh,
        CAST(lng AS FLOAT64) AS lng_generation_mwh,
        CAST(importExport AS FLOAT64) AS import_export_mwh,
        CAST(wasteheat AS FLOAT64) AS waste_heat_generation_mwh,
        
        -- API'den aynı saat için gelen olası çift kayıtları önlemek için Row Number
        ROW_NUMBER() OVER(
            PARTITION BY CAST(date AS DATE), CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) 
            ORDER BY _record_hash DESC
        ) as rn
    FROM {{ source('silver', 'generation') }}
)
SELECT * EXCEPT(rn) FROM deduped WHERE rn = 1

{% if is_incremental() %}
  AND date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}