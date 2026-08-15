{% set dest_cutoff = (run_started_at.date() - modules.datetime.timedelta(days=14)).strftime('%Y-%m-%d') %}

{{ config(
    materialized='incremental',
    unique_key=['id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"},
    incremental_predicates=[
        "DBT_INTERNAL_DEST.date >= DATE('" ~ dest_cutoff ~ "')"
    ]
) }}

-- Split 2026-08-15 (see stg_idm_transactions_recent.sql for why): this model
-- no longer reads the external table directly. MERGE ... USING (a subquery
-- against the Hive-partitioned external table) does not prune, regardless of
-- WHERE clause -- confirmed by two failed fix attempts, each scanning the
-- full 578 MB table. stg_idm_transactions_recent.sql does the actual
-- pruned read (a plain SELECT, which prunes correctly) into a small native
-- table; this model just merges from that.
--
-- Second, separate fix (same day): even after the source side dropped to
-- 8.3 MB, the MERGE itself still scanned 586 MB -- because a MERGE also has
-- to check the DESTINATION table for each incoming id, and this destination
-- has accumulated history since 2025 with nothing scoping that lookup.
-- incremental_predicates adds a filter on DBT_INTERNAL_DEST (dbt-bigquery's
-- alias for the MERGE's target) so BigQuery can prune destination partitions
-- too -- this table IS native (not external), so partition pruning inside
-- MERGE isn't subject to the limitation found above. 14-day buffer is double
-- the source model's 7-day window: any incoming row's `id` is tied to a
-- `date` within the last 7 days, so a genuine match in the destination must
-- also fall within a similarly recent window -- 14 days is a safe margin,
-- not a tight one.

SELECT
    id,
    date,
    hour,
    contract_name,
    price_try,
    quantity_mwh
FROM {{ ref('stg_idm_transactions_recent') }}

{% if is_incremental() %}
WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
