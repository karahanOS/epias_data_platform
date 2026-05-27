{{ config(
    materialized='table',
    partition_by={
      "field": "trade_date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH idm AS (
    SELECT * FROM {{ source('silver', 'idm_transactions') }}
),

-- Katılımcılar tablosu günlük snapshot olduğu için en güncel halini alıyoruz
participants AS (
    SELECT 
        organizationId,
        organizationName
    FROM {{ source('silver', 'participants') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY organizationId ORDER BY year DESC, month DESC, day DESC) = 1
),

enriched_idm AS (
    SELECT
        i.date AS trade_date,
        i.contractName,
        i.price AS transaction_price_try,
        i.quantity AS transaction_quantity_mwh,
        (i.price * i.quantity) AS transaction_volume_try,
        
        -- Alıcı Şirket Bilgileri
        i.buyerOrganizationId,
        COALESCE(pb.organizationName, 'Bilinmeyen Şirket') AS buyer_company_name,
        
        -- Satıcı Şirket Bilgileri
        i.sellerOrganizationId,
        COALESCE(ps.organizationName, 'Bilinmeyen Şirket') AS seller_company_name

    FROM idm i
    LEFT JOIN participants pb ON i.buyerOrganizationId = pb.organizationId
    LEFT JOIN participants ps ON i.sellerOrganizationId = ps.organizationId
)

SELECT * FROM enriched_idm