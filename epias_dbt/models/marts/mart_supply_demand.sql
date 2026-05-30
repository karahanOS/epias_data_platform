{{ config(materialized='table') }}

SELECT
    date,
    bid_offer_price_try,
    cumulative_supply_mwh,
    cumulative_demand_mwh,
    (cumulative_supply_mwh - cumulative_demand_mwh) AS net_supply_mwh
FROM {{ ref('stg_supply_demand_curve') }}