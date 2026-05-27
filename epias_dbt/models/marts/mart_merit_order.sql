{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH supply_demand AS (
    SELECT 
        date,
        price AS bid_offer_price_try,
        supply AS cumulative_supply_mwh,
        demand AS cumulative_demand_mwh
    FROM {{ source('silver', 'supply_demand') }}
),

pricing AS (
    SELECT 
        date, 
        marketTradePrice AS ptf_try 
    FROM {{ source('silver', 'pricing') }}
),

merit_order_curve AS (
    SELECT
        sd.date,
        sd.bid_offer_price_try,
        sd.cumulative_supply_mwh,
        sd.cumulative_demand_mwh,
        p.ptf_try,
        
        -- Teklifin durumu: Kendi teklif fiyatı PTF'den küçük veya eşitse üretici malını satar (In Merit)
        -- PTF'den yüksek fiyat çeken doğalgaz/kömür santralleri ise dışarıda kalır (Out of Merit)
        CASE 
            WHEN sd.bid_offer_price_try <= p.ptf_try THEN 'In Merit (Eşleşti)'
            ELSE 'Out of Merit (Eşleşmedi)'
        END AS supply_status,
        
        -- Arz ve Talep Arasındaki Fark (Piyasa Derinliği)
        (sd.cumulative_supply_mwh - sd.cumulative_demand_mwh) AS market_depth_mwh

    FROM supply_demand sd
    LEFT JOIN pricing p ON sd.date = p.date
)

SELECT * FROM merit_order_curve