{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH lep AS (
    -- Yarının Saatlik Tüketim Tahmini
    SELECT date, lep AS forecasted_demand_mwh 
    FROM {{ source('silver', 'load_estimation') }}
),

res_forecast AS (
    -- Yarının Yenilenebilir Üretim Tahmini
    SELECT 
        date, 
        total AS forecasted_res_mwh,
        wind AS forecasted_wind_mwh,
        solar AS forecasted_solar_mwh
    FROM {{ source('silver', 'res_forecast') }}
),

pib AS (
    -- Fiyattan Bağımsız Katı Talep
    SELECT date, priceIndependentBidAmount AS price_independent_bid_mwh
    FROM {{ source('silver', 'price_ind_bid') }}
),

pricing AS (
    -- Fiyatı referans olarak yanına koyuyoruz
    SELECT date, marketTradePrice AS ptf_try 
    FROM {{ source('silver', 'pricing') }}
)

SELECT
    l.date,
    p.ptf_try,
    l.forecasted_demand_mwh,
    r.forecasted_res_mwh,
    r.forecasted_wind_mwh,
    r.forecasted_solar_mwh,
    pb.price_independent_bid_mwh,
    
    -- KRİTİK FEATURE 1: Beklenen Kalan Yük (Forecasted Residual Load)
    -- Termik santrallerin yarın karşılaması gerekecek olan net talep
    (l.forecasted_demand_mwh - r.forecasted_res_mwh) AS forecasted_residual_load_mwh,
    
    -- KRİTİK FEATURE 2: Katı Talep Oranı
    -- Fiyat ne olursa olsun alınacak elektriğin, toplam talebe oranı. 
    -- Bu oran artarsa PTF kesinlikle tavan yapar.
    SAFE_DIVIDE(pb.price_independent_bid_mwh, l.forecasted_demand_mwh) * 100 AS strict_demand_pct

FROM lep l
LEFT JOIN res_forecast r ON l.date = r.date
LEFT JOIN pib pb ON l.date = pb.date
LEFT JOIN pricing p ON l.date = p.date