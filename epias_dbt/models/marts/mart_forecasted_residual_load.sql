{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- IMPORTANT: all four staging tables are hourly (unique_key = [date, hour]).
-- Joining on date alone creates a 24^4 = 331,776× Cartesian explosion per day.
-- Every CTE must carry `hour` and all joins must use BOTH date AND hour.
--
-- Each source may carry multiple rows per (date, hour) — e.g. stg_res_forecast
-- has ~6 plan-version rows per hour from the EPIAS API.  The dbt unique_key
-- deduplication only runs during incremental MERGE operations, NOT on full-refresh
-- SELECTs.  Aggregating with AVG here produces one canonical row per (date, hour)
-- and prevents a fan-out in the downstream join.

WITH lep AS (
    SELECT date, hour, AVG(forecasted_load_mwh) AS forecasted_load_mwh
    FROM {{ ref('stg_load_estimation') }}
    GROUP BY 1, 2
),
res_forecast AS (
    SELECT date, hour, AVG(forecasted_res_mwh) AS forecasted_res_mwh
    FROM {{ ref('stg_res_forecast') }}
    GROUP BY 1, 2
),
pib AS (
    SELECT date, hour, AVG(price_independent_bid_mwh) AS price_independent_bid_mwh
    FROM {{ ref('stg_price_ind_bid') }}
    GROUP BY 1, 2
),
pricing AS (
    SELECT date, hour, AVG(ptf_try) AS ptf_try
    FROM {{ ref('stg_pricing') }}
    GROUP BY 1, 2
)

SELECT
    l.date,
    l.hour,
    -- Reconstruct the correct UTC timestamp for this Turkish local (date, hour) slot.
    --
    -- l.date  = Turkish local calendar date (DATE type)
    -- l.hour  = Turkish local hour 1–24 (from stg_load_estimation's `time` field)
    --
    -- TIMESTAMP(l.date, 'Asia/Istanbul') gives UTC midnight for the Turkish calendar
    -- date, e.g. '2026-06-05' → '2026-06-04T21:00:00Z'.
    -- Adding l.hour hours then lands on the correct UTC slot:
    --   hour=1  → '2026-06-04T22:00:00Z'  (Turkish 01:00 on 2026-06-05) ✓
    --   hour=22 → '2026-06-05T19:00:00Z'  (Turkish 22:00 on 2026-06-05) ✓
    --
    -- The previous formula TIMESTAMP_ADD(CAST(l.date AS TIMESTAMP), INTERVAL l.hour HOUR)
    -- used UTC midnight instead of Istanbul midnight, producing timestamps 3 h too late
    -- (Turkish 22:00 came out as '2026-06-05T22:00:00Z' instead of '2026-06-05T19:00:00Z').
    TIMESTAMP_ADD(TIMESTAMP(l.date, 'Asia/Istanbul'), INTERVAL l.hour HOUR) AS datetime,
    p.ptf_try,
    l.forecasted_load_mwh,
    r.forecasted_res_mwh,
    pb.price_independent_bid_mwh,
    (l.forecasted_load_mwh - r.forecasted_res_mwh) AS forecasted_residual_load_mwh
FROM lep l
LEFT JOIN res_forecast r  ON l.date = r.date  AND l.hour = r.hour
LEFT JOIN pricing p        ON l.date = p.date  AND l.hour = p.hour
LEFT JOIN pib pb           ON l.date = pb.date AND l.hour = pb.hour