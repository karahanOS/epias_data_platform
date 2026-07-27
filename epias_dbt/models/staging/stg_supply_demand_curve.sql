{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'bid_offer_price_try'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- Silver supply_demand.date is a full UTC TIMESTAMP with per-hour granularity
-- (many price/supply/demand points per hour, not just one) -- the original
-- CAST(date AS DATE) discarded the hour entirely, which both let its own
-- (date, price) grain collide across different hours of the same day, and
-- made mart_merit_order's JOIN to stg_pricing on `date` alone fan out every
-- price point against all 24 hours of that day's pricing (the ~1.3M-row
-- mart_merit_order size). Fixed by extracting hour like every other hourly
-- staging model.
WITH source AS (
    SELECT * FROM {{ source('silver', 'supply_demand') }}
)

SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(price AS FLOAT64) AS bid_offer_price_try,
    CAST(supply AS FLOAT64) AS cumulative_supply_mwh,
    CAST(demand AS FLOAT64) AS cumulative_demand_mwh
FROM source

{% if is_incremental() %}
  WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Same cross-partition-boundary duplication as stg_pricing/stg_dams/etc --
-- self-heal here too, now that (date,hour,price) is the real grain.
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly -- cast
-- price to STRING for grouping purposes only (equality, not ordering).
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul'),
               CAST(CAST(price AS FLOAT64) AS STRING)
  ORDER BY CAST(date AS TIMESTAMP) DESC
) = 1