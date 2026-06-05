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
    -- Reconstruct the full hourly timestamp so downstream consumers (ptf_trainer.py)
    -- can use it as a proper time-series index with correct .hour, lag, and rolling features.
    TIMESTAMP_ADD(CAST(l.date AS TIMESTAMP), INTERVAL l.hour HOUR) AS datetime,
    p.ptf_try,
    l.forecasted_load_mwh,
    r.forecasted_res_mwh,
    pb.price_independent_bid_mwh,
    (l.forecasted_load_mwh - r.forecasted_res_mwh) AS forecasted_residual_load_mwh
FROM lep l
LEFT JOIN res_forecast r  ON l.date = r.date  AND l.hour = r.hour
LEFT JOIN pricing p        ON l.date = p.date  AND l.hour = p.hour
LEFT JOIN pib pb           ON l.date = pb.date AND l.hour = pb.hour