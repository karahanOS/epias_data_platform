{{ config(
    materialized='table',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

SELECT
    date AS trade_date,
    hour,
    contract_name,
    COUNT(id) AS total_transaction_count,
    AVG(price_try) AS avg_transaction_price_try,
    SUM(quantity_mwh) AS total_volume_mwh,
    SUM(price_try * quantity_mwh) AS total_transaction_value_try
FROM {{ ref('stg_idm_transactions') }}
GROUP BY 1, 2, 3