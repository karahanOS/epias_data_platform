{{
  config(
    materialized='incremental',
    unique_key=['date', 'dam_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
  )
}}

-- mart_hydro_risk: Hidrolik stres endeksi
-- Baraj aktif hacmini 90 günlük kayan pencerede tarihsel yüzdeliğe göre normalleştirir.
-- hydro_stress_index = 1.0 → normal, 0.0 → tarihsel minimum, >1.0 → tarihsel maksimumun üstünde
--
-- Havza seviyesinde de aggregate alınır (basin_stress_index).
-- Dashboard'da: kırmızı < 0.25 (kriz), sarı 0.25-0.50 (dikkat), yeşil > 0.50

WITH daily_vols AS (
    SELECT
        date,
        dam_id,
        basin_name,
        dam_name,
        active_volume
    FROM {{ ref('stg_dams') }}
),

-- 90-day rolling window statistics per dam
rolling_stats AS (
    SELECT
        date,
        dam_id,
        basin_name,
        dam_name,
        active_volume,
        -- Rolling min/max/avg over trailing 90 days (inclusive)
        MIN(active_volume) OVER (
            PARTITION BY dam_id
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS rolling_90d_min,
        MAX(active_volume) OVER (
            PARTITION BY dam_id
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS rolling_90d_max,
        AVG(active_volume) OVER (
            PARTITION BY dam_id
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS rolling_90d_avg,
        -- 7-day rolling avg for smoothed trend
        AVG(active_volume) OVER (
            PARTITION BY dam_id
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg,
        -- Week-over-week change
        LAG(active_volume, 7) OVER (
            PARTITION BY dam_id ORDER BY date
        ) AS active_volume_7d_ago
    FROM daily_vols
),

stress_index AS (
    SELECT
        date,
        dam_id,
        basin_name,
        dam_name,
        active_volume,
        rolling_90d_min,
        rolling_90d_max,
        rolling_90d_avg,
        rolling_7d_avg,
        active_volume_7d_ago,
        -- Normalized stress index: 0 = min, 1 = max of last 90 days
        -- Low index = depleted reservoir = high hydro supply risk
        SAFE_DIVIDE(
            active_volume - rolling_90d_min,
            rolling_90d_max - rolling_90d_min
        ) AS hydro_stress_index,
        -- Week-over-week delta (negative = dropping)
        active_volume - active_volume_7d_ago AS wow_volume_change,
        SAFE_DIVIDE(
            active_volume - active_volume_7d_ago,
            NULLIF(active_volume_7d_ago, 0)
        ) AS wow_volume_change_pct
    FROM rolling_stats
)

SELECT
    s.*,
    -- Basin-level aggregate stress index (weighted by active_volume share)
    AVG(s.hydro_stress_index) OVER (
        PARTITION BY s.date, s.basin_name
    ) AS basin_avg_stress_index,
    SUM(s.active_volume) OVER (
        PARTITION BY s.date, s.basin_name
    ) AS basin_total_active_volume_mwh

FROM stress_index s

{% if is_incremental() %}
WHERE date >= (SELECT DATE_SUB(MAX(date), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
