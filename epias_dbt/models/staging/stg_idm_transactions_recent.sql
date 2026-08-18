{{ config(materialized='table') }}

-- buyerOrganizationId / sellerOrganizationId were never real API fields — the
-- EPİAŞ transaction-history endpoint's actual response schema
-- (TransactionHistoryGipDataDto) only has contractName/date/hour/id/price/
-- quantity, confirmed 2026-08-09 against EPİAŞ's own live documentation while
-- researching ADR-0007 (plans/07-company-level-market-activity-kgup.md). GİP
-- has no organization-attributable endpoint at all (all 16 GİP-tagged
-- endpoints checked). Company-level market activity is now sourced from GÖP
-- instead — see mart_company_gop_activity.sql.

-- Intermediate staging model (2026-08-15): isolates the external-table read
-- from the MERGE in stg_idm_transactions.sql. Confirmed via
-- INFORMATION_SCHEMA.JOBS_BY_PROJECT that a plain SELECT with this exact
-- year/month/day filter prunes correctly (3.7 KB scanned for one day) --
-- but the identical filter sitting inside stg_idm_transactions' MERGE ...
-- USING (external table) statement scanned 578 MB regardless, twice, with
-- two different predicate shapes. BigQuery's MERGE appears to force a full
-- scan of an external-table source before it ever applies the predicate --
-- a limitation specific to MERGE, not to Hive partition pruning itself
-- (which works fine, as this model's own scan size proves). This model does
-- the cheap pruned read as a plain SELECT -- rebuilt fully every run, but
-- "fully" only ever means the ~7-day window below, not the whole table --
-- so the downstream incremental model can MERGE from this small native
-- table instead of the external source, avoiding the limitation entirely.

{% set cutoff = (run_started_at.date() - modules.datetime.timedelta(days=7)) %}

-- 2026-08-19 DÜZELTME: date UTC bir TIMESTAMP; Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini yanlış tarihe
-- etiketliyordu (bkz. stg_pricing.sql'in aynı notu). NOT: alttaki
-- year/month/day cutoff WHERE'i bilinçli olarak DOKUNULMADI — o, Hive
-- partition pruning için ayrı bir mekanizma (ds/Bronze fetch gününe göre),
-- bu satırdaki içerik tarihinden bağımsız çalışıyor.
SELECT
    CAST(id AS STRING) AS id,
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(contractName AS STRING) AS contract_name,
    CAST(price    AS FLOAT64) AS price_try,
    CAST(quantity AS FLOAT64) AS quantity_mwh
FROM {{ source('silver', 'idm_transactions') }} AS s
WHERE (
    s.year > {{ cutoff.year }}
    OR (s.year = {{ cutoff.year }} AND s.month > {{ cutoff.month }})
    OR (s.year = {{ cutoff.year }} AND s.month = {{ cutoff.month }} AND s.day >= {{ cutoff.day }})
)
-- Confirmed via real data (2026-07-27): the same transaction id can appear in
-- two adjacent Hive day-partitions with identical content (e.g. a transaction
-- timestamped 07-23 15:54 UTC showing up in both day=23's and day=24's
-- partition) — same cross-partition-boundary class as stg_pricing etc.
-- `id` is the true global grain; self-heal here rather than downstream, so
-- stg_idm_transactions' MERGE never sees the duplicate in the first place.
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly (raw `id` is
-- FLOAT64 per the project's cast convention) -- cast to INT64 first, same fix
-- as stg_participants.
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(s.id AS INT64) ORDER BY s.date DESC) = 1
