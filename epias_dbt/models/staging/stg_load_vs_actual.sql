{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

-- 💡 "00:00" metninin ilk 2 karakterini (00) alıp INT64'e çeviriyoruz!
SELECT
    CAST(t.date AS DATE) AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour,
    CAST(t.consumption AS FLOAT64) AS actual_consumption
FROM {{ source('silver', 'consumption') }} AS t

{% if is_incremental() %}
  WHERE CAST(t.date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- boundary slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(t.date AS DATE), CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)
  ORDER BY t.date DESC
) = 1