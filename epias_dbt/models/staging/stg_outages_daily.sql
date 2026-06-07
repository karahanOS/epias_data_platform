{{
  config(
    materialized='incremental',
    unique_key=['id', 'active_date'],
    incremental_strategy='merge',
    partition_by={"field": "active_date", "data_type": "date"}
  )
}}

-- stg_outages_daily: Arıza/bakım span olaylarını GENERATE_DATE_ARRAY ile
-- günlük satırlara genişletir. Her (id, active_date) çifti benzersizdir.
-- Bu model mart_supply_shock_index ve mart_capacity_utilization için temel veriyi sağlar.

WITH spans AS (
    SELECT
        id,
        company_name,
        plant_name,
        installed_capacity_mwh,
        outage_capacity_mwh,
        outage_reason,
        date           AS start_date,
        CAST(end_time AS DATE) AS end_date
    FROM {{ ref('stg_outages') }}
    WHERE date IS NOT NULL
),

daily_expanded AS (
    SELECT
        s.id,
        s.company_name,
        s.plant_name,
        s.installed_capacity_mwh,
        s.outage_capacity_mwh,
        s.outage_reason,
        s.start_date,
        s.end_date,
        active_date
    FROM spans s,
    UNNEST(
        GENERATE_DATE_ARRAY(
            s.start_date,
            -- Guard: cap at today to avoid unbounded future rows
            LEAST(s.end_date, CURRENT_DATE())
        )
    ) AS active_date
    WHERE s.end_date >= s.start_date  -- skip malformed spans
)

SELECT * FROM daily_expanded

{% if is_incremental() %}
WHERE active_date >= (SELECT DATE_SUB(MAX(active_date), INTERVAL 7 DAY) FROM {{ this }})
{% endif %}
