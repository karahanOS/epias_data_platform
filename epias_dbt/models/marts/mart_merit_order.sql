{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH supply_demand AS (
    SELECT * FROM {{ ref('stg_supply_demand_curve') }}
),
pricing AS (
    SELECT * FROM {{ ref('stg_pricing') }}
)

SELECT
    sd.date,
    sd.bid_offer_price_try,
    sd.cumulative_supply_mwh,
    sd.cumulative_demand_mwh,
    p.ptf_try,
    CASE 
        WHEN sd.bid_offer_price_try <= p.ptf_try THEN 'In Merit (Eşleşti)'
        ELSE 'Out of Merit (Eşleşmedi)'
    END AS supply_status
FROM supply_demand sd
LEFT JOIN pricing p ON sd.date = p.date