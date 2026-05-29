{{ config(
    materialized='incremental',
    unique_key=['date', 'basinName'], 
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'dams') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(basinName AS STRING) AS basin_name,
    CAST(waterLevel AS FLOAT64) AS water_level_m,
    CAST(activeVolume AS FLOAT64) AS active_volume_hm3
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}