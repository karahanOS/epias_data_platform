{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(s.date AS DATE) AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour,
    CAST(lep AS FLOAT64) AS forecasted_load_mwh
FROM {{ source('silver', 'load_estimation') }} AS s

{% if is_incremental() %}
  WHERE CAST(s.date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- boundary slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(s.date AS DATE), CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)
  ORDER BY s.date DESC
) = 1