{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

{% set cutoff = (run_started_at.date() - modules.datetime.timedelta(days=7)).strftime('%Y-%m-%d') %}

-- Fixed 2026-08-25 (cost investigation): was materialized='table' — a full
-- rebuild reading the ENTIRE stg_idm_transactions table from scratch every
-- hour, with no incremental logic at all. Confirmed via
-- INFORMATION_SCHEMA.JOBS_BY_PROJECT as the single largest BigQuery cost in
-- the project (587 MiB/run, ~82% of total spend concentrated across this
-- model and 3 siblings that independently re-aggregate the same raw
-- transaction table). Switched to incremental + insert_overwrite with a
-- 7-day window — stg_idm_transactions is natively partitioned, so a plain
-- filtered SELECT prunes correctly (this isn't the MERGE-doesn't-prune case,
-- since insert_overwrite's read step is a plain SELECT).

SELECT
    date AS trade_date,
    hour,
    contract_name,
    COUNT(id) AS total_transaction_count,
    AVG(price_try) AS avg_transaction_price_try,
    SUM(quantity_mwh) AS total_volume_mwh,
    SUM(price_try * quantity_mwh) AS total_transaction_value_try
FROM {{ ref('stg_idm_transactions') }}
{% if is_incremental() %}
WHERE date >= DATE('{{ cutoff }}')
{% endif %}
GROUP BY 1, 2, 3
