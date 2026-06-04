{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH dam_clearing AS (
    SELECT * FROM {{ ref('stg_dam_clearing') }}
),
pricing AS (
    SELECT * FROM {{ ref('stg_pricing') }}
)

SELECT
    dc.date,
    dc.hour,
    dc.matched_bids_mwh AS total_buy_mwh,
    dc.matched_offers_mwh AS total_sell_mwh,
    p.ptf_try,
    (dc.matched_bids_mwh * p.ptf_try) AS market_volume_try,
    (dc.matched_bids_mwh * p.ptf_try) / NULLIF(dc.matched_bids_mwh, 0) AS average_price_try
FROM dam_clearing dc
LEFT JOIN pricing p ON dc.date = p.date AND dc.hour = p.hour