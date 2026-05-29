{{ config(
    materialized='incremental',
    unique_key=['id'],
    incremental_strategy='merge',
    partition_by={
      "field": "trade_date",
      "data_type": "date"
    }
) }}

WITH raw_idm AS (
    SELECT * FROM {{ source('silver', 'idm_transactions') }}
)

SELECT
    CAST(id AS STRING) AS transaction_id,
    CAST(date AS DATE) AS trade_date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS trade_timestamp,
    CAST(contractName AS STRING) AS contract_name,
    CAST(price AS FLOAT64) AS transaction_price_try,
    CAST(quantity AS FLOAT64) AS transaction_quantity_mwh,
    CAST(buyerOrganizationId AS INT64) AS buyer_organization_id,
    CAST(sellerOrganizationId AS INT64) AS seller_organization_id
FROM raw_idm

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(trade_date) FROM {{ this }})
{% endif %}