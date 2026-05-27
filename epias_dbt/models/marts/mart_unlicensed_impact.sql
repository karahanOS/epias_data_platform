{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH unlicensed_hourly AS (
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        SUM(total) AS total_unlicensed_mwh,
        SUM(solar) AS solar_unlicensed_mwh,
        SUM(wind) AS wind_unlicensed_mwh,
        SUM(biomass) AS biomass_unlicensed_mwh
    FROM {{ source('silver', 'unlicensed') }}
    GROUP BY 1
),

pricing_hourly AS (
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        AVG(marketTradePrice) AS ptf_try
    FROM {{ source('silver', 'pricing') }}
    GROUP BY 1
),

unlicensed_impact AS (
    SELECT
        u.date,
        u.total_unlicensed_mwh,
        u.solar_unlicensed_mwh,
        u.wind_unlicensed_mwh,
        u.biomass_unlicensed_mwh,
        p.ptf_try,
        
        -- Finansal Etki: Lisanssız üretim PTF'den satılsaydı yaratacağı hacim
        (u.total_unlicensed_mwh * p.ptf_try) AS estimated_market_value_try,
        
        -- Güneşin toplam lisanssız içindeki payı (Gündüz saatlerindeki fiyat baskısını açıklar)
        SAFE_DIVIDE(u.solar_unlicensed_mwh, u.total_unlicensed_mwh) * 100 AS solar_dominance_pct

    FROM unlicensed_hourly u
    LEFT JOIN pricing_hourly p ON u.date = p.date
)

SELECT * FROM unlicensed_impact