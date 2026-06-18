{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

-- NOTE: Turkey is permanently UTC+3 (no DST since 2016).
-- Silver pricing.date is a UTC TIMESTAMP (written by parse_epias_timestamp in Spark).
-- We convert to Turkish local date/hour so that the unique_key (date, hour) aligns
-- with all other hourly staging tables (stg_load_estimation, stg_dam_clearing,
-- stg_aic, stg_price_ind_bid, stg_generation) which carry Turkish local hours via
-- their time/hour string fields.  Without this conversion every JOIN in
-- mart_ml_features and mart_forecasted_residual_load silently returns NULL because
-- UTC hour 19 ≠ Turkish hour 22 for the same physical slot.
--
-- Column mapping (EPIAS API actual field names → dbt alias):
--   price     → ptf_try   (API returns "price", not "mcp" despite Swagger spec)
--   priceUsd  → ptf_usd
--   priceEur  → ptf_eur
--   smp/sdf   → not read here; stg_smf uses the separate BPM endpoint
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(price    AS FLOAT64) AS ptf_try,
    CAST(priceUsd AS FLOAT64) AS ptf_usd,
    CAST(priceEur AS FLOAT64) AS ptf_eur
FROM {{ source('silver', 'pricing') }}

{% if is_incremental() %}
  -- Use Turkish local date for the lookback filter to avoid accidentally skipping
  -- the first 3 hours of each day (UTC 21–23 of the previous calendar day).
  WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}