{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_ptf_forward_features: Genuine day-ahead feature set for FORWARD PTF
-- prediction (as opposed to mart_ptf_lag_features, which is driven by
-- stg_pricing and therefore only ever has rows for hours whose price is
-- ALREADY settled — see 2026-08-03 investigation).
--
-- Driving table is stg_load_estimation (LEP), confirmed empirically to
-- extend into genuinely-future dates ahead of GÖP/PTF publication (LEP for
-- "tomorrow" already populated while stg_pricing has zero rows for that
-- date). Every column here is restricted to what a real forecaster could
-- know before the target hour's price is set — see ptf_features.py's
-- FEATURE_COLS leakage-audit comment (2026-08-03) for the same discipline
-- applied to the backtest-oriented mart.
--
-- Price lag features (T-24h, T-168h) reference DATE_SUB(target_date, N) so
-- they always point at already-settled history, never at another row in
-- the same forecast batch (which would be a hidden autoregressive
-- dependency — the target day's own hours aren't known yet). The 1h-lag
-- and rolling-window features can't be computed per-target-hour for the
-- same reason (a rolling 24h average ending mid-forecast-horizon would
-- need hours later in the same unpublished day) — instead they're frozen
-- at their value as of the single latest SETTLED hour and repeated across
-- the whole forecast batch, matching how real day-ahead forecasters predict
-- all 24 target hours from one pre-auction information snapshot rather than
-- autoregressively.
--
-- Sources not yet confirmed to publish ahead of price at all times of day
-- (stg_res_forecast, stg_aic) are LEFT JOINed and COALESCE(...,0) same as
-- mart_ml_features — falls back gracefully, matching training-time
-- fillna(0) behaviour, rather than blocking the whole forecast on their
-- availability.
-- ─────────────────────────────────────────────────────────────────────────────

WITH targets AS (
    SELECT date, hour, forecasted_load_mwh
    FROM {{ ref('stg_load_estimation') }}
),

res_forecast AS (
    SELECT date, hour, forecasted_res_mwh FROM {{ ref('stg_res_forecast') }}
),

aic AS (
    SELECT date, hour, total_aic_mwh FROM {{ ref('stg_aic') }}
),

shock AS (
    SELECT date, supply_shock_index, total_outage_mwh
    FROM {{ ref('mart_supply_shock_index') }}
),

shock_trend AS (
    SELECT date, AVG(supply_shock_index) OVER (
        ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS supply_shock_trend_7d
    FROM {{ ref('mart_supply_shock_index') }}
),

-- T-24h cross-market signal: yesterday's same-hour cross-market data,
-- already fully settled regardless of which future hour we're targeting.
cross_lag AS (
    SELECT date, hour, arbitrage_opportunity_score, yal_delivered_mwh,
           yat_delivered_mwh, net_dgp_mwh
    FROM {{ ref('mart_cross_market_spread') }}
),

-- Price lag features: explicit DATE_SUB joins against settled history —
-- always resolve to a real, already-known row regardless of forecast horizon.
price_lag24 AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }}
),
price_lag168 AS (
    SELECT date, hour, ptf_try FROM {{ ref('stg_pricing') }}
),
smf_lag24 AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
),
smf_lag168 AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
),

-- Frozen snapshot: the single latest SETTLED price + its 24h/168h rolling
-- stats, reused as ptf_lag_1h/rolling_* for every hour in the forecast
-- batch (see module docstring for why these can't vary per target hour).
-- frozen_datetime is carried through so the final SELECT can compute how
-- stale this snapshot actually is for each target hour (see ADR-0005,
-- "lead-time-aware decay").
latest_settled AS (
    SELECT
        TIMESTAMP_ADD(TIMESTAMP(date, 'Asia/Istanbul'), INTERVAL hour HOUR) AS frozen_datetime,
        ptf_try AS frozen_ptf_lag_1h,
        AVG(ptf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_rolling_avg_24h,
        MAX(ptf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_rolling_max_24h,
        MIN(ptf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_rolling_min_24h,
        AVG(ptf_try) OVER (ORDER BY date, hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS frozen_rolling_avg_168h,
        ROW_NUMBER() OVER (ORDER BY date DESC, hour DESC) AS rn
    FROM {{ ref('stg_pricing') }}
    QUALIFY rn = 1
),

joined AS (
    SELECT
        t.date,
        t.hour,
        TIMESTAMP_ADD(TIMESTAMP(t.date, 'Asia/Istanbul'), INTERVAL t.hour HOUR) AS datetime,

        -- ADR-0005: 2026-08-07's first real forward-forecast validation (Aug
        -- 8, lead times 12-31h) showed MASE 0.941 vs. the walk-forward
        -- backtest's ~0.6 — barely beating naive T-24h in live use despite
        -- testing far better historically. Root cause: ptf_lag_1h/rolling_*
        -- below are frozen at the single latest SETTLED price and broadcast
        -- unchanged to every target hour in the batch, but training data
        -- always has these as genuinely-fresh T-1 lags — the model was
        -- never taught that this "1-hour lag" signal is actually N hours
        -- stale for a far-out target. lead_hours feeds the decay applied in
        -- the final SELECT below.
        GREATEST(0, TIMESTAMP_DIFF(
            TIMESTAMP_ADD(TIMESTAMP(t.date, 'Asia/Istanbul'), INTERVAL t.hour HOUR),
            ls.frozen_datetime, HOUR
        )) AS lead_hours,

        t.forecasted_load_mwh,
        COALESCE(rf.forecasted_res_mwh, 0)          AS forecasted_res_mwh,
        (t.forecasted_load_mwh - COALESCE(rf.forecasted_res_mwh, 0)) AS forecasted_residual_load_mwh,

        SAFE_DIVIDE(
            t.forecasted_load_mwh - COALESCE(rf.forecasted_res_mwh, 0),
            a.total_aic_mwh
        )                                            AS capacity_utilization_ratio,

        COALESCE(sh.supply_shock_index, 0)          AS supply_shock_index,
        COALESCE(sh.total_outage_mwh, 0)            AS total_outage_mwh,
        COALESCE(st.supply_shock_trend_7d, 0)       AS supply_shock_trend_7d,

        COALESCE(cl.arbitrage_opportunity_score, 0) AS arb_score_lag24,
        COALESCE(cl.yal_delivered_mwh, 0)           AS yal_lag24,
        COALESCE(cl.yat_delivered_mwh, 0)           AS yat_lag24,
        COALESCE(cl.net_dgp_mwh, 0)                 AS net_dgp_lag24,

        pl24.ptf_try                                AS ptf_lag_24h,
        pl168.ptf_try                                AS ptf_lag_168h,
        sl24.smf_try                                AS smf_try_lag_24h,
        sl168.smf_try                               AS smf_try_lag_168h,

        -- raw_* are the unadjusted frozen values — decayed toward
        -- ptf_rolling_avg_168h (the stable long-run anchor, left undecayed)
        -- in the final SELECT.
        ls.frozen_ptf_lag_1h                        AS raw_ptf_lag_1h,
        ls.frozen_rolling_avg_24h                   AS raw_ptf_rolling_avg_24h,
        ls.frozen_rolling_max_24h                   AS raw_ptf_rolling_max_24h,
        ls.frozen_rolling_min_24h                   AS raw_ptf_rolling_min_24h,
        ls.frozen_rolling_avg_168h                  AS ptf_rolling_avg_168h

    FROM targets t
    LEFT JOIN res_forecast rf  ON rf.date = t.date AND rf.hour = t.hour
    LEFT JOIN aic a            ON a.date  = t.date AND a.hour  = t.hour
    LEFT JOIN shock sh         ON sh.date = t.date
    LEFT JOIN shock_trend st   ON st.date = t.date
    LEFT JOIN cross_lag cl     ON cl.date = DATE_SUB(t.date, INTERVAL 1 DAY) AND cl.hour = t.hour
    LEFT JOIN price_lag24 pl24   ON pl24.date  = DATE_SUB(t.date, INTERVAL 1 DAY) AND pl24.hour  = t.hour
    LEFT JOIN price_lag168 pl168 ON pl168.date = DATE_SUB(t.date, INTERVAL 7 DAY) AND pl168.hour = t.hour
    LEFT JOIN smf_lag24 sl24     ON sl24.date  = DATE_SUB(t.date, INTERVAL 1 DAY) AND sl24.hour  = t.hour
    LEFT JOIN smf_lag168 sl168   ON sl168.date = DATE_SUB(t.date, INTERVAL 7 DAY) AND sl168.hour = t.hour
    CROSS JOIN latest_settled ls
)

SELECT
    date, hour, datetime, lead_hours,
    forecasted_load_mwh, forecasted_res_mwh, forecasted_residual_load_mwh,
    capacity_utilization_ratio,
    supply_shock_index, total_outage_mwh, supply_shock_trend_7d,
    arb_score_lag24, yal_lag24, yat_lag24, net_dgp_lag24,
    ptf_lag_24h, ptf_lag_168h, smf_try_lag_24h, smf_try_lag_168h,

    -- EXP(-lead_hours/24) decay constant: identity (decay=1) at lead_hours=0,
    -- ~37% weight on the frozen value at 24h out, ~27% at 31h out (today's
    -- observed max) — pulls near-term signals toward the stable 168h
    -- average as staleness grows, instead of passing a day-old "1-hour lag"
    -- through unchanged. See ADR-0005 for the full rationale; this is a
    -- stopgap (Option C) pending enough gold_ptf_forward_accuracy history to
    -- evaluate a proper lead-time-aware retrain (Option A).
    EXP(-lead_hours / 24) * raw_ptf_lag_1h
        + (1 - EXP(-lead_hours / 24)) * ptf_rolling_avg_168h AS ptf_lag_1h,
    EXP(-lead_hours / 24) * raw_ptf_rolling_avg_24h
        + (1 - EXP(-lead_hours / 24)) * ptf_rolling_avg_168h AS ptf_rolling_avg_24h,
    EXP(-lead_hours / 24) * raw_ptf_rolling_max_24h
        + (1 - EXP(-lead_hours / 24)) * ptf_rolling_avg_168h AS ptf_rolling_max_24h,
    EXP(-lead_hours / 24) * raw_ptf_rolling_min_24h
        + (1 - EXP(-lead_hours / 24)) * ptf_rolling_avg_168h AS ptf_rolling_min_24h,
    ptf_rolling_avg_168h

FROM joined
