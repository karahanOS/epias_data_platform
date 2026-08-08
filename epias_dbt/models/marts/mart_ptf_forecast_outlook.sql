{{ config(materialized='view') }}

-- mart_ptf_forecast_outlook: single "what do we currently think this hour's
-- price is" series — real price (final PTF or pre-appeal K.PTF, via
-- mart_ptf_realized) preferred; the XGBoost forward prediction
-- (gold_ptf_forward_predictions) only fills hours neither final nor interim
-- covers yet. value_source tells consumers which one a row came from
-- ('final' | 'interim' | 'model').
--
-- Centralizes the "real vs model" coalesce that, as of ADR-0005
-- (2026-08-07), had been independently re-implemented 3 different ways: a
-- SQL anti-join in src/ptf_inference.py's extract_forward_features(), a
-- pandas isin() on the dashboard's PTF Tahmin & ML page, and a pandas
-- merge/indicator on the Vardiya Optimizasyonu page — with the two
-- dashboard copies already drifting on date window and timezone before this
-- model existed. Both dashboard pages should read this one view instead.
--
-- materialized='view', not 'table': gold_ptf_forward_predictions is written
-- by a separate, independently-scheduled Airflow DAG (ptf_hourly_inference),
-- not by dbt — there's no ordering guarantee between its hourly run and this
-- project's dbt build. A materialized table would introduce staleness up to
-- one dbt-build-cycle; a view stays exactly as fresh as querying the
-- underlying tables directly (matching what the dashboard did before this
-- model existed), while still centralizing the coalesce SQL in one place.
--
-- NOT referenced via {{ source(...) }}: gold_ptf_forward_predictions lives
-- in this same dbt target dataset (epias_gold) but isn't dbt-managed —
-- source() is meant for genuinely external (pre-dbt) data, so this uses
-- target.database/target.schema directly instead.

WITH real AS (
    SELECT date, hour, datetime, ptf_try AS value, price_status AS value_source
    FROM {{ ref('mart_ptf_realized') }}
),

model AS (
    SELECT
        predicted_date AS date,
        hour,
        TIMESTAMP_ADD(TIMESTAMP(predicted_date, 'Asia/Istanbul'), INTERVAL hour HOUR) AS datetime,
        predicted_ptf AS value,
        'model' AS value_source
    FROM `{{ target.database }}.{{ target.schema }}.gold_ptf_forward_predictions`
)

SELECT r.date, r.hour, r.datetime, r.value, r.value_source
FROM real r

UNION ALL

SELECT m.date, m.hour, m.datetime, m.value, m.value_source
FROM model m
WHERE NOT EXISTS (
    SELECT 1 FROM real r WHERE r.date = m.date AND r.hour = m.hour
)
