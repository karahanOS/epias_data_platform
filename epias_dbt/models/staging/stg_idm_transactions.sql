{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- Split 2026-08-15 (see stg_idm_transactions_recent.sql for why): this model
-- no longer reads the external table directly. MERGE ... USING (a subquery
-- against the Hive-partitioned external table) does not prune, regardless of
-- WHERE clause -- confirmed by two failed fix attempts, each scanning the
-- full 578 MB table. stg_idm_transactions_recent.sql does the actual
-- pruned read (a plain SELECT, which prunes correctly) into a small native
-- table; this model just writes from that.
--
-- Third fix, same day: even reading from the small 8.3 MB source, and even
-- after adding incremental_predicates to filter the destination side, the
-- MERGE still scanned ~606 MB every run -- confirmed via `bq show` that the
-- destination table (605 MB, 590 partitions) really is correctly
-- date-partitioned, so this isn't a missing-partitioning problem. It's a
-- documented BigQuery characteristic: MERGE statements generally scan the
-- entire target table regardless of predicates, because evaluating
-- WHEN NOT MATCHED conceptually requires checking the whole table. Switched
-- from `merge`/`unique_key` to `insert_overwrite`, which replaces whole
-- day-partitions atomically instead of row-matching by id -- partition
-- replacement prunes correctly since it operates on the partition key
-- directly.
--
-- Trade-off (accepted 2026-08-15): the cross-partition-boundary dedup below
-- (an id landing in two adjacent day-partitions) stays safe as long as both
-- adjacent partitions are in the same run's touched range, which they
-- normally are -- each run's touched range sits at the current leading
-- edge. Unlike MERGE's global id-matching, insert_overwrite would not clean
-- up a stale duplicate if an id's "winning" day ever shifted to fall
-- outside the currently-touched partitions between runs. Deemed acceptable:
-- the dedup logic itself (QUALIFY in stg_idm_transactions_recent.sql) is
-- deterministic and re-evaluates the same 7-day window fresh every run, so
-- once a day's partition is written correctly it stays correct unless the
-- underlying raw Silver data for that day changes later -- a scenario that
-- would have needed a manual reprocess under the old MERGE strategy too.

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
