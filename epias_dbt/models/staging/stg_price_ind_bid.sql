{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- 2026-08-19 DÜZELTME: s.date UTC bir TIMESTAMP; Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini yanlış tarihe
-- etiketliyordu (bkz. stg_pricing.sql'in aynı notu).
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(bidVolume AS FLOAT64) AS price_independent_bid_mwh
FROM {{ source('silver', 'price_ind_bid') }} AS s

{% if is_incremental() %} WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }}) {% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- boundary slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'), CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64)
  ORDER BY s.date DESC
) = 1