{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Silver imbalance.date is a UTC TIMESTAMP (parse_epias_timestamp).
-- Convert to Turkish local date/hour to align with all other hourly staging models.
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(generationTotal  AS FLOAT64) AS total_generation_mwh,
    CAST(consumption      AS FLOAT64) AS total_consumption_mwh,
    CAST(imbalanceQuantity AS FLOAT64) AS net_imbalance_mwh
FROM {{ source('silver', 'imbalance') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- UTC 21:00-23:59 slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul')
  ORDER BY s.date DESC
) = 1