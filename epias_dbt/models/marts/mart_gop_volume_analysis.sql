{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH dam_clearing AS (
    SELECT * FROM {{ source('silver', 'dam_clearing') }}
),

pricing AS (
    SELECT * FROM {{ source('silver', 'pricing') }}
),

joined_data AS (
    SELECT
        dc.date,
        dc.matchedBidsQuantity AS total_buy_mwh,
        dc.matchedOffersQuantity AS total_sell_mwh,
        dc.blockBidQuantity AS block_buy_mwh,
        dc.blockOfferQuantity AS block_sell_mwh,
        p.marketTradePrice AS ptf_try,
        -- Piyasa Parasal Hacmi (Eşleşen Miktar * PTF)
        (dc.matchedBidsQuantity * p.marketTradePrice) AS market_volume_try
    FROM dam_clearing dc
    LEFT JOIN pricing p 
        ON dc.date = p.date
)

SELECT
    date,
    total_buy_mwh,
    total_sell_mwh,
    block_buy_mwh,
    block_sell_mwh,
    ptf_try,
    market_volume_try,
    -- Analitik Metrikler (Blok Tekliflerin Toplama Oranı)
    -- Blok teklifler piyasa manipülasyonu veya katı üretim tesisleri hakkında bilgi verir
    SAFE_DIVIDE(block_buy_mwh, total_buy_mwh) * 100 AS block_buy_percentage,
    SAFE_DIVIDE(block_sell_mwh, total_sell_mwh) * 100 AS block_sell_percentage
FROM joined_data