{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Silver smf.date is a UTC TIMESTAMP (parse_epias_timestamp).
-- Convert to Turkish local date/hour to align with all other hourly staging models.
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(systemMarginalPrice AS FLOAT64) AS smf_try
FROM {{ source('silver', 'smf') }} AS s

{% if is_incremental() %}
  -- 1-day lookback (2026-08-18): SMF's official S+5 settlement lag means
  -- hours 22-23 (Istanbul) aren't published until 03:00-04:00 the NEXT
  -- Istanbul day — after the last hourly run for that date has already
  -- executed. A plain `>= MAX(date)` filter would permanently exclude
  -- yesterday from ever being reprocessed once today's date appears here,
  -- silently dropping those late-settling hours forever. See
  -- src/silver_lookback_fix.py, which corrects the Silver partition itself;
  -- this widened filter is what lets that correction actually get merged in.
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= DATE_SUB((SELECT MAX(date) FROM {{ this }}), INTERVAL 1 DAY)
{% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- UTC 21:00-23:59 slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul')
  ORDER BY s.date DESC
) = 1