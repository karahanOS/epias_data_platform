-- models/marts/mart_smf_lag_features.sql
{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_smf_lag_features: SMF 2-stage forecaster (direction classifier + price
-- regressor) feature store — backtest/training-oriented (only rows whose SMF
-- is already settled; see mart_smf_forward_features.sql for genuinely-future
-- prediction).
--
-- Extends mart_ml_features (already has ptf_try, weather, RES/load forecasts,
-- actual generation, and 24h-lag DGP signals — yal_lag24/yat_lag24/net_dgp_lag24
-- — see mart_ml_features.sql) with:
--   - system_direction: the Stage-1 classifier's TARGET label for this hour
--     (ENERGY_DEFICIT / ENERGY_SURPLUS / IN_BALANCE), from stg_system_direction.
--   - smf_try lag/rolling features, mirroring mart_ptf_lag_features.sql's exact
--     LAG-window technique for ptf_try.
--   - system_direction_lag_5h: persistence input feature (NOT the target).
--
-- Lag design (2026-08-15 investigation — see plans/federated-rolling-dragon.md):
--   - smf_try_lag_24h/168h: same "leakage-free" discipline mart_ptf_lag_features
--     already uses for smf_try — always resolvable, EPİAŞ SMF is fully settled
--     by then regardless of ingestion timing.
--   - smf_try_lag_5h / system_direction_lag_5h: NEW — genuinely fresh, same-day
--     signals. Confirmed live (2026-08-15) that EPIAS publishes SMF at the
--     official S+5 lag (Kurul Kararı 10711, row 53) and that the pipeline's
--     get_smf() can now reach it (see epias_client.py's _safe_end_iso fix) —
--     these are 5-hour LAG() windows, not day-shifted joins, since a single
--     row-ordered LAG(x, 5) already crosses midnight correctly as long as the
--     underlying hourly series has no gaps (same assumption mart_ptf_lag_features
--     already relies on for its own 24h/168h lags).
-- ─────────────────────────────────────────────────────────────────────────────

WITH base AS (
    -- See mart_ptf_lag_features.sql's note on why window functions always run
    -- over the FULL mart_ml_features history even in incremental runs — same
    -- rationale applies here (mart_ml_features is small; the MERGE target
    -- below already restricts what actually gets written).
    SELECT
        ml.*,
        sd.system_direction
    FROM {{ ref('mart_ml_features') }} ml
    LEFT JOIN {{ ref('stg_system_direction') }} sd
        ON ml.date = sd.date AND ml.hour = sd.hour
),

-- Regime persistence length (2026-08-25 — added after a sustained ~16h+
-- deficit streak got badly under-predicted, see memory/smf_model_quality.md):
-- classic "gaps and islands" run-length, causal by construction (each row's
-- value only depends on rows at or before it in the (date,hour) ordering,
-- never on whether the streak keeps going afterward) — grp is constant across
-- a maximal run of identical system_direction values, then ROW_NUMBER()
-- within grp counts 1,2,3,... for how many consecutive hours (including this
-- one) the system has already been in that direction.
direction_streaks AS (
    SELECT
        date, hour,
        ROW_NUMBER() OVER (ORDER BY date, hour)
          - ROW_NUMBER() OVER (PARTITION BY system_direction ORDER BY date, hour) AS grp
    FROM base
),
direction_persistence AS (
    SELECT
        date, hour,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY date, hour) AS direction_persistence_hours
    FROM direction_streaks
),

with_lags AS (
    SELECT
        base.*,
        TIMESTAMP_ADD(
            TIMESTAMP(base.date, 'Asia/Istanbul'),
            INTERVAL CAST(base.hour AS INT64) HOUR
        ) AS datetime,

        -- SMF lag / rolling özellikleri — mart_ptf_lag_features.sql'deki
        -- LAG(smf_try, N) tekniğinin birebir aynısı.
        LAG(base.smf_try, 24)  OVER (ORDER BY base.date, base.hour) AS smf_try_lag_24h,
        LAG(base.smf_try, 168) OVER (ORDER BY base.date, base.hour) AS smf_try_lag_168h,
        AVG(base.smf_try) OVER (
            ORDER BY base.date, base.hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS smf_rolling_avg_24h,
        MAX(base.smf_try) OVER (
            ORDER BY base.date, base.hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS smf_rolling_max_24h,
        MIN(base.smf_try) OVER (
            ORDER BY base.date, base.hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS smf_rolling_min_24h,
        AVG(base.smf_try) OVER (
            ORDER BY base.date, base.hour
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
        ) AS smf_rolling_avg_168h,

        -- Taze (~5 saat) sinyaller — 2026-08-15 aynı-gün SMF düzeltmesiyle
        -- mümkün oldu (bkz. dosya başlığı).
        LAG(base.smf_try, 5)          OVER (ORDER BY base.date, base.hour) AS smf_try_lag_5h,
        LAG(base.system_direction, 5) OVER (ORDER BY base.date, base.hour) AS system_direction_lag_5h,

        -- Same 5h safety boundary as system_direction_lag_5h above — how many
        -- consecutive hours the direction AS OF T-5 had already held, not
        -- including the target hour itself.
        LAG(dp.direction_persistence_hours, 5) OVER (ORDER BY base.date, base.hour) AS direction_persistence_lag5h

    FROM base
    LEFT JOIN direction_persistence dp ON dp.date = base.date AND dp.hour = base.hour
)

SELECT * FROM with_lags
{% if is_incremental() %}
  -- 1-day lookback (2026-08-18), same rationale as stg_smf.sql/
  -- stg_system_direction.sql: a plain `>= MAX(date)` filter would never
  -- re-derive yesterday's row once today's date exists here, so a
  -- late-corrected smf_try/system_direction (see src/silver_lookback_fix.py)
  -- would never propagate past staging.
  WHERE date >= DATE_SUB((SELECT MAX(date) FROM {{ this }}), INTERVAL 1 DAY)
{% endif %}
