{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_smf_forward_features: Genuine forward feature set for SMF 2-stage
-- forecasting (direction classifier + price regressor), mirroring
-- mart_ptf_forward_features.sql's leak-free discipline exactly — driven by
-- stg_load_estimation (LEP), which extends into genuinely-future dates ahead
-- of GÖP/PTF/SMF publication.
--
-- Difference from mart_ptf_forward_features.sql: PTF is a SAFE, KNOWN input
-- feature here (not the target) — PTF for any given hour is fixed ~14:00 TRT
-- the day before delivery, so by the time we're forecasting SMF for that hour
-- (whether later today or a future day), PTF is already known. ptf_try below
-- is a direct (date,hour) join against mart_ptf_realized (not lagged, not
-- frozen) — NULL only for target hours whose own day-ahead auction hasn't
-- cleared yet (a genuine "don't know yet", same honesty as every other column
-- here).
--
-- smf_try_lag_24h/168h use the same DATE_SUB-join technique as PTF's own
-- lag24/168, always resolvable regardless of forecast horizon. smf_try_lag_5h
-- / system_direction_lag_5h are NEW (2026-08-15 same-day SMF fix — see
-- mart_smf_lag_features.sql's header) and use the same frozen-latest-value +
-- lead-time decay pattern ADR-0005 already established for ptf_lag_1h,
-- because a target hour less than 5h out can't resolve a real "5h-ago" join
-- (that timestamp is itself still in the future). Decay constant is 5h (this
-- signal's own native lag) rather than PTF's 24h, since a 5h-old signal's
-- staleness is naturally measured in multiples of 5h, not 24h.
-- ─────────────────────────────────────────────────────────────────────────────

WITH targets AS (
    SELECT date, hour, forecasted_load_mwh
    FROM {{ ref('stg_load_estimation') }}
),

res_forecast AS (
    SELECT date, hour, forecasted_res_mwh FROM {{ ref('stg_res_forecast') }}
),

-- ADR-0006 fallback (see mart_ptf_forward_features.sql) — identical rationale.
res_forecast_hourly_avg AS (
    SELECT hour, AVG(forecasted_res_mwh) AS avg_res_by_hour
    FROM {{ ref('stg_res_forecast') }}
    WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Istanbul'), INTERVAL 14 DAY)
    GROUP BY hour
),

aic AS (
    SELECT date, hour, total_aic_mwh FROM {{ ref('stg_aic') }}
),

aic_hourly_avg AS (
    SELECT hour, AVG(total_aic_mwh) AS avg_aic_by_hour
    FROM {{ ref('stg_aic') }}
    WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Istanbul'), INTERVAL 14 DAY)
    GROUP BY hour
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

cross_lag AS (
    SELECT date, hour, arbitrage_opportunity_score, yal_delivered_mwh,
           yat_delivered_mwh, net_dgp_mwh, gip_gop_spread_try
    FROM {{ ref('mart_cross_market_spread') }}
),

-- PTF for the target hour itself — safe, known ahead (see header). Coalesces
-- final PTF with pre-appeal K.PTF, same "best known real price" source
-- ptf_inference.py's own forward anti-join already relies on.
ptf_target AS (
    SELECT date, hour, ptf_try FROM {{ ref('mart_ptf_realized') }}
),

smf_lag24 AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
),
smf_lag168 AS (
    SELECT date, hour, smf_try FROM {{ ref('stg_smf') }}
),

-- Frozen snapshot: the single latest SETTLED SMF + its 168h rolling average,
-- reused (with lead-time decay below) as smf_try_lag_5h for every hour in the
-- forecast batch — mirrors mart_ptf_forward_features.sql's latest_settled CTE.
latest_settled_smf AS (
    SELECT
        TIMESTAMP_ADD(TIMESTAMP(date, 'Asia/Istanbul'), INTERVAL hour HOUR) AS frozen_datetime,
        smf_try AS frozen_smf_lag_5h,
        AVG(smf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_smf_rolling_avg_24h,
        MAX(smf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_smf_rolling_max_24h,
        MIN(smf_try) OVER (ORDER BY date, hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)  AS frozen_smf_rolling_min_24h,
        AVG(smf_try) OVER (ORDER BY date, hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS frozen_smf_rolling_avg_168h,
        ROW_NUMBER() OVER (ORDER BY date DESC, hour DESC) AS rn
    FROM {{ ref('stg_smf') }}
    QUALIFY rn = 1
),

-- Regime persistence length (2026-08-25, mirrors mart_smf_lag_features.sql's
-- gaps-and-islands technique exactly — see that file's header comment).
-- Computed over the full stg_system_direction history so the frozen value
-- below reflects a real run-length, not just a same-day count.
direction_streaks AS (
    SELECT
        date, hour,
        ROW_NUMBER() OVER (ORDER BY date, hour)
          - ROW_NUMBER() OVER (PARTITION BY system_direction ORDER BY date, hour) AS grp
    FROM {{ ref('stg_system_direction') }}
),
direction_persistence AS (
    SELECT
        date, hour,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY date, hour) AS direction_persistence_hours
    FROM direction_streaks
),

-- Frozen latest known system direction — categorical persistence feature, no
-- numeric decay applies; the model learns staleness via lead_hours itself.
-- direction_persistence_lag5h gets the same no-decay treatment: it's frozen
-- as of the same latest-settled hour as frozen_system_direction_lag_5h, and
-- the model already has lead_hours available to learn how much to discount
-- an increasingly-stale streak count.
latest_settled_direction AS (
    SELECT
        sd.system_direction AS frozen_system_direction_lag_5h,
        dp.direction_persistence_hours AS frozen_direction_persistence_lag5h,
        ROW_NUMBER() OVER (ORDER BY sd.date DESC, sd.hour DESC) AS rn
    FROM {{ ref('stg_system_direction') }} sd
    JOIN direction_persistence dp ON dp.date = sd.date AND dp.hour = sd.hour
    QUALIFY rn = 1
),

joined AS (
    SELECT
        t.date,
        t.hour,
        TIMESTAMP_ADD(TIMESTAMP(t.date, 'Asia/Istanbul'), INTERVAL t.hour HOUR) AS datetime,

        GREATEST(0, TIMESTAMP_DIFF(
            TIMESTAMP_ADD(TIMESTAMP(t.date, 'Asia/Istanbul'), INTERVAL t.hour HOUR),
            ls.frozen_datetime, HOUR
        )) AS lead_hours,

        t.forecasted_load_mwh,
        COALESCE(rf.forecasted_res_mwh, rha.avg_res_by_hour, 0) AS forecasted_res_mwh,
        (t.forecasted_load_mwh - COALESCE(rf.forecasted_res_mwh, rha.avg_res_by_hour, 0)) AS forecasted_residual_load_mwh,

        SAFE_DIVIDE(
            t.forecasted_load_mwh - COALESCE(rf.forecasted_res_mwh, rha.avg_res_by_hour, 0),
            COALESCE(a.total_aic_mwh, aha.avg_aic_by_hour)
        )                                            AS capacity_utilization_ratio,

        COALESCE(sh.supply_shock_index, 0)          AS supply_shock_index,
        COALESCE(sh.total_outage_mwh, 0)            AS total_outage_mwh,
        COALESCE(st.supply_shock_trend_7d, 0)       AS supply_shock_trend_7d,

        COALESCE(cl.arbitrage_opportunity_score, 0) AS arb_score_lag24,
        COALESCE(cl.yal_delivered_mwh, 0)           AS yal_lag24,
        COALESCE(cl.yat_delivered_mwh, 0)           AS yat_lag24,
        COALESCE(cl.net_dgp_mwh, 0)                 AS net_dgp_lag24,
        cl.gip_gop_spread_try                       AS gip_gop_spread_lag24,

        pt.ptf_try                                  AS ptf_try,

        sl24.smf_try                                AS smf_try_lag_24h,
        sl168.smf_try                               AS smf_try_lag_168h,

        ls.frozen_smf_lag_5h                        AS raw_smf_try_lag_5h,
        ls.frozen_smf_rolling_avg_24h               AS raw_smf_rolling_avg_24h,
        ls.frozen_smf_rolling_max_24h               AS raw_smf_rolling_max_24h,
        ls.frozen_smf_rolling_min_24h               AS raw_smf_rolling_min_24h,
        ls.frozen_smf_rolling_avg_168h              AS smf_rolling_avg_168h,
        ld.frozen_system_direction_lag_5h           AS system_direction_lag_5h,
        ld.frozen_direction_persistence_lag5h       AS direction_persistence_lag5h

    FROM targets t
    LEFT JOIN res_forecast rf  ON rf.date = t.date AND rf.hour = t.hour
    LEFT JOIN res_forecast_hourly_avg rha ON rha.hour = t.hour
    LEFT JOIN aic a            ON a.date  = t.date AND a.hour  = t.hour
    LEFT JOIN aic_hourly_avg aha ON aha.hour = t.hour
    LEFT JOIN shock sh         ON sh.date = t.date
    LEFT JOIN shock_trend st   ON st.date = t.date
    LEFT JOIN cross_lag cl     ON cl.date = DATE_SUB(t.date, INTERVAL 1 DAY) AND cl.hour = t.hour
    LEFT JOIN ptf_target pt    ON pt.date = t.date AND pt.hour = t.hour
    LEFT JOIN smf_lag24 sl24   ON sl24.date  = DATE_SUB(t.date, INTERVAL 1 DAY) AND sl24.hour  = t.hour
    LEFT JOIN smf_lag168 sl168 ON sl168.date = DATE_SUB(t.date, INTERVAL 7 DAY) AND sl168.hour = t.hour
    CROSS JOIN latest_settled_smf ls
    CROSS JOIN latest_settled_direction ld
)

SELECT
    date, hour, datetime, lead_hours,
    forecasted_load_mwh, forecasted_res_mwh, forecasted_residual_load_mwh,
    capacity_utilization_ratio,
    supply_shock_index, total_outage_mwh, supply_shock_trend_7d,
    arb_score_lag24, yal_lag24, yat_lag24, net_dgp_lag24, gip_gop_spread_lag24,
    ptf_try,
    smf_try_lag_24h, smf_try_lag_168h,
    system_direction_lag_5h, direction_persistence_lag5h,

    -- EXP(-lead_hours/5): identity (decay=1) at lead_hours=0 (the frozen value
    -- really is ~5h old, matching the signal's native lag), ~37% weight left
    -- at 5h further out, decaying toward the stable 168h anchor as staleness
    -- grows — same shape as ADR-0005's ptf_lag_1h decay, scaled to this
    -- signal's own lag instead of borrowing PTF's 24h forecast-horizon constant.
    EXP(-lead_hours / 5) * raw_smf_try_lag_5h
        + (1 - EXP(-lead_hours / 5)) * smf_rolling_avg_168h AS smf_try_lag_5h,
    EXP(-lead_hours / 5) * raw_smf_rolling_avg_24h
        + (1 - EXP(-lead_hours / 5)) * smf_rolling_avg_168h AS smf_rolling_avg_24h,
    EXP(-lead_hours / 5) * raw_smf_rolling_max_24h
        + (1 - EXP(-lead_hours / 5)) * smf_rolling_avg_168h AS smf_rolling_max_24h,
    EXP(-lead_hours / 5) * raw_smf_rolling_min_24h
        + (1 - EXP(-lead_hours / 5)) * smf_rolling_avg_168h AS smf_rolling_min_24h,
    smf_rolling_avg_168h

FROM joined
