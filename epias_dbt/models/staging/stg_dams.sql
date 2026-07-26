{{ config(
    materialized='incremental',
    unique_key=['date', 'dam_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(s.date AS DATE) AS date,
    CAST(damId AS INT64) AS dam_id,
    CAST(basinName AS STRING) AS basin_name,
    CAST(damName AS STRING) AS dam_name,
    CAST(activeVolume AS FLOAT64) AS active_volume
FROM {{ source('silver', 'dams') }} AS s

{% if is_incremental() %}
  WHERE CAST(s.date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Dams reports "as of" a lagged UTC timestamp (e.g. 21:00 UTC of the previous
-- day) that can land in BOTH that day's Hive partition and the next day's own
-- fetch, duplicating a (date, dam_id) row with identical values. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(s.date AS DATE), CAST(damId AS INT64)
  ORDER BY s.date DESC
) = 1