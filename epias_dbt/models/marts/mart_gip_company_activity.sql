{{ config(
    materialized='table',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

WITH idm AS (
    SELECT * FROM {{ ref('stg_idm_transactions') }}
),
participants AS (
    SELECT organization_id, organization_name FROM {{ ref('stg_participants') }}
)

SELECT
    i.trade_date,
    i.hour,
    i.contract_name,
    i.transaction_price_try,
    i.transaction_quantity_mwh,
    (i.transaction_price_try * i.transaction_quantity_mwh) AS transaction_volume_try,
    i.buyer_organization_id,
    COALESCE(pb.organization_name, 'Bilinmeyen Alıcı') AS buyer_company_name,
    i.seller_organization_id,
    COALESCE(ps.organization_name, 'Bilinmeyen Satıcı') AS seller_company_name
FROM idm i
LEFT JOIN participants pb ON i.buyer_organization_id = pb.organization_id
LEFT JOIN participants ps ON i.seller_organization_id = ps.organization_id