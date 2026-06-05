{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- Each outage case spans [start_time, end_time].  The previous version keyed rows by
-- caseStartDate only, so outages that began before the training window never joined
-- against the load-estimation dates (which are operational dates, not start dates).
-- This version generates one row per *active* calendar date for each outage, then
-- aggregates — giving the correct daily capacity under forced outage.

WITH outage_dates AS (
    -- Expand each outage across every calendar day it was active.
    -- GENERATE_DATE_ARRAY caps at end_time or TODAY to avoid unbounded ranges.
    SELECT
        active_date,
        outage_capacity_mwh
    FROM {{ ref('stg_outages') }},
    UNNEST(GENERATE_DATE_ARRAY(
        DATE(start_time),
        LEAST(DATE(end_time), CURRENT_DATE())
    )) AS active_date
    WHERE start_time IS NOT NULL
      AND end_time   IS NOT NULL
      AND end_time   > start_time
),
daily_outages AS (
    SELECT active_date AS date, SUM(outage_capacity_mwh) AS total_outage_mwh
    FROM outage_dates
    GROUP BY 1
),
daily_aic AS (
    SELECT date, SUM(total_aic_mwh) AS total_available_capacity_mwh
    FROM {{ ref('stg_aic') }}
    GROUP BY 1
)
SELECT
    o.date,
    o.total_outage_mwh,
    a.total_available_capacity_mwh,
    SAFE_DIVIDE(o.total_outage_mwh, a.total_available_capacity_mwh) AS supply_shock_index
FROM daily_outages o
LEFT JOIN daily_aic a ON o.date = a.date