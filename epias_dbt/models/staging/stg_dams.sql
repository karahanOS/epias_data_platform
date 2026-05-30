{{ config(
    materialized='incremental',
    unique_key=['date', 'dam_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(damId AS INT64) AS dam_id,
    CAST(basinName AS STRING) AS basin_name,
    CAST(damName AS STRING) AS dam_name,
    CAST(activeVolume AS FLOAT64) AS active_volume
FROM {{ source('silver', 'dams') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}