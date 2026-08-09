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

{% if is_incremental() %} WHERE DATE(s.date) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}
-- Confirmed via real data (2026-07-27): the same transaction id can appear in
-- two adjacent Hive day-partitions with identical content (e.g. a transaction
-- timestamped 07-23 15:54 UTC showing up in both day=23's and day=24's
-- partition) — same cross-partition-boundary class as stg_pricing etc.
-- `id` is the true global grain (config unique_key=['id']); self-heal here.
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly (raw `id` is
-- FLOAT64 per the project's cast convention) -- cast to INT64 first, same fix
-- as stg_participants.
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(s.id AS INT64) ORDER BY s.date DESC) = 1