{{ config(materialized='view') }}

-- mart_smf_forecast_outlook: single "what do we currently think this hour's
-- SMF is" series — real settled SMF (mart_smf_realized) preferred; the
-- 2-stage XGBoost forward prediction (gold_smf_forward_predictions) only
-- fills hours mart_smf_realized doesn't cover yet. value_source tells
-- consumers which one a row came from ('realized' | 'model'). Mirrors
-- mart_ptf_forecast_outlook.sql's centralized real-vs-model coalesce —
-- see that file's header for why this lives in dbt rather than being
-- re-implemented per-consumer (dashboard page, inference anti-join, etc.).
--
-- materialized='view', not 'table': gold_smf_forward_predictions is written
-- by a separately-scheduled Airflow DAG (smf_hourly_inference), not dbt —
-- same staleness rationale as mart_ptf_forecast_outlook.sql.
--
-- Not referenced via a dbt source() macro: gold_smf_forward_predictions lives
-- in this same dbt target dataset (epias_gold) but isn't dbt-managed.

WITH real AS (
    SELECT date, hour, datetime, smf_try AS value, 'realized' AS value_source
    FROM {{ ref('mart_smf_realized') }}
),

model AS (
    SELECT
        predicted_date AS date,
        hour,
        TIMESTAMP_ADD(TIMESTAMP(predicted_date, 'Asia/Istanbul'), INTERVAL hour HOUR) AS datetime,
        predicted_smf AS value,
        'model' AS value_source
    FROM `{{ target.database }}.{{ target.schema }}.gold_smf_forward_predictions`
)

SELECT r.date, r.hour, r.datetime, r.value, r.value_source
FROM real r

UNION ALL

SELECT m.date, m.hour, m.datetime, m.value, m.value_source
FROM model m
WHERE NOT EXISTS (
    SELECT 1 FROM real r WHERE r.date = m.date AND r.hour = m.hour
)
