{{ config(materialized='incremental', unique_key=['id'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- buyerOrganizationId / sellerOrganizationId were never real API fields — the
-- EPİAŞ transaction-history endpoint's actual response schema
-- (TransactionHistoryGipDataDto) only has contractName/date/hour/id/price/
-- quantity, confirmed 2026-08-09 against EPİAŞ's own live documentation while
-- researching ADR-0007 (plans/07-company-level-market-activity-kgup.md). GİP
-- has no organization-attributable endpoint at all (all 16 GİP-tagged
-- endpoints checked). Company-level market activity is now sourced from GÖP
-- instead — see mart_company_gop_activity.sql.

SELECT
    CAST(id AS STRING) AS id,
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(contractName AS STRING) AS contract_name,
    CAST(price    AS FLOAT64) AS price_try,
    CAST(quantity AS FLOAT64) AS quantity_mwh
FROM {{ source('silver', 'idm_transactions') }} AS s

{% if is_incremental() %}
{% set cutoff = (run_started_at.date() - modules.datetime.timedelta(days=7)) %}
WHERE DATE(s.date) >= (SELECT MAX(date) FROM {{ this }})
  -- Partition-pruning hint (2026-08-15, revised): first attempt used
  -- DATE(s.year, s.month, s.day) >= DATE_SUB((SELECT MAX(date) FROM {{ this }}), ...)
  -- and confirmed via INFORMATION_SCHEMA.JOBS_BY_PROJECT that it did NOT
  -- prune -- 578 MiB / 606M bytes processed, identical to before the change.
  -- Two likely reasons: wrapping the partition columns in DATE(...) hides
  -- them from the optimizer, and bounding by a correlated subquery
  -- (SELECT MAX(date) FROM {{ this }}) may not be resolvable before file
  -- selection. This version uses raw, unwrapped year/month/day comparisons
  -- against a literal cutoff computed at dbt-compile time (Jinja, not SQL) --
  -- the textbook-correct shape for triggering partition elimination. 7-day
  -- buffer (not 1) because this exact pipeline has already had real
  -- multi-day outages (billing suspension, dedup incident) -- must stay
  -- correct even after a gap that long. Still redundant with the line above
  -- for correctness; only changes what gets scanned, not what gets selected.
  -- NOT YET CONFIRMED to actually prune -- verify via INFORMATION_SCHEMA
  -- before trusting this comment.
  AND (
    s.year > {{ cutoff.year }}
    OR (s.year = {{ cutoff.year }} AND s.month > {{ cutoff.month }})
    OR (s.year = {{ cutoff.year }} AND s.month = {{ cutoff.month }} AND s.day >= {{ cutoff.day }})
  )
{% endif %}
-- Confirmed via real data (2026-07-27): the same transaction id can appear in
-- two adjacent Hive day-partitions with identical content (e.g. a transaction
-- timestamped 07-23 15:54 UTC showing up in both day=23's and day=24's
-- partition) — same cross-partition-boundary class as stg_pricing etc.
-- `id` is the true global grain (config unique_key=['id']); self-heal here.
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly (raw `id` is
-- FLOAT64 per the project's cast convention) -- cast to INT64 first, same fix
-- as stg_participants.
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(s.id AS INT64) ORDER BY s.date DESC) = 1