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
    CAST(price    AS NUMERIC) AS price_try,
    CAST(quantity AS NUMERIC) AS quantity_mwh,

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

FROM {{ source('silver', 'idm_transactions') }}

{% if is_incremental() %} WHERE DATE(date) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}