{{ config(materialized='incremental', unique_key=['id'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- buyer_organization_id / seller_organization_id come from the EPIAS IDM transactions
-- API and are stored in the Silver layer by bronze_to_silver_idm_transactions.py.
-- They are the key to answering "which company uses GİP?" — JOIN to stg_participants
-- on organization_id to resolve human-readable org names.
--
-- RESILIENCE: older Silver runs may not contain these columns if the Bronze pipeline
-- fetched data before buyerOrganizationId / sellerOrganizationId were added.
-- We detect their presence at compile time with adapter.get_columns_in_relation()
-- and fall back to NULL so the model never errors — mart_gip_company_analysis will
-- return NULL org names until the Silver table is rebuilt with the new columns.

{% set silver_cols = adapter.get_columns_in_relation(source('silver', 'idm_transactions'))
                     | map(attribute='name') | list %}

SELECT
    CAST(id AS STRING) AS id,
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(contractName AS STRING) AS contract_name,
    CAST(price    AS FLOAT64) AS price_try,
    CAST(quantity AS FLOAT64) AS quantity_mwh,

    {% if 'buyerOrganizationId' in silver_cols %}
    CAST(buyerOrganizationId  AS INT64) AS buyer_organization_id,
    {% else %}
    CAST(NULL AS INT64) AS buyer_organization_id,  -- column absent in current Silver; rebuild after Bronze backfill
    {% endif %}

    {% if 'sellerOrganizationId' in silver_cols %}
    CAST(sellerOrganizationId AS INT64) AS seller_organization_id
    {% else %}
    CAST(NULL AS INT64) AS seller_organization_id  -- column absent in current Silver; rebuild after Bronze backfill
    {% endif %}

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