{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH idm_hourly AS (
    -- GİP işlemlerini saatlik bazda kümülatif hale getiriyoruz
    SELECT
        date,
        SUM(quantity_mwh) AS total_gip_volume_mwh,
        -- Ağırlıklı Ortalama Fiyat (WAP)
        SAFE_DIVIDE(SUM(price_try * quantity_mwh), SUM(quantity_mwh)) AS gip_weighted_avg_price_try,
        COUNT(1) AS total_transaction_count
    FROM {{ ref('stg_idm_transactions') }}
    GROUP BY 1
),

weather_hourly AS (
    SELECT
        date,
        AVG(temperature_celsius) AS avg_temperature_c,
        AVG(wind_speed_kmh) AS avg_windspeed_kmh,
        AVG(shortwave_radiation) AS avg_solar_radiation_wm2
    FROM {{ ref('stg_weather') }}
    GROUP BY 1
),

pricing_hourly AS (
    SELECT
        date,
        AVG(ptf_try) AS ptf_try
    FROM {{ ref('stg_pricing') }}
    GROUP BY 1
)

SELECT
    i.date,
    i.total_gip_volume_mwh,
    i.gip_weighted_avg_price_try,
    i.total_transaction_count,
    p.ptf_try,
    -- Finansal Metrik: GİP ve PTF arasındaki fiyat makası
    (i.gip_weighted_avg_price_try - p.ptf_try) AS gip_ptf_spread_try,
    w.avg_temperature_c,
    w.avg_windspeed_kmh,
    w.avg_solar_radiation_wm2
FROM idm_hourly i
LEFT JOIN weather_hourly w ON i.date = w.date
LEFT JOIN pricing_hourly p ON i.date = p.date