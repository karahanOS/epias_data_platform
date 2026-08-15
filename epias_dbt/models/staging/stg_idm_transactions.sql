{{ config(materialized='incremental', unique_key=['id'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Split 2026-08-15 (see stg_idm_transactions_recent.sql for why): this model
-- no longer reads the external table directly. MERGE ... USING (a subquery
-- against the Hive-partitioned external table) does not prune, regardless of
-- WHERE clause -- confirmed by two failed fix attempts, each scanning the
-- full 578 MB table. stg_idm_transactions_recent.sql does the actual
-- pruned read (a plain SELECT, which prunes correctly) into a small native
-- table; this model just merges from that.

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
