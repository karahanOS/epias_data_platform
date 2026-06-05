{{ config(materialized='incremental', unique_key=['id'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(id AS STRING) AS id,
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(contractName AS STRING) AS contract_name,
    CAST(price AS NUMERIC) AS price_try,
    CAST(quantity AS NUMERIC) AS quantity_mwh
FROM {{ source('silver', 'idm_transactions') }}

{% if is_incremental() %} WHERE DATE(date) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}