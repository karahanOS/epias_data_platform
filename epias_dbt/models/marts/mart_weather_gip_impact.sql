{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH idm_hourly AS (
    -- GİP işlemlerini saatlik bazda kümülatif hale getiriyoruz
    SELECT
        TIMESTAMP_TRUNC(trade_timestamp, HOUR) AS date,
        SUM(transaction_quantity_mwh) AS total_gip_volume_mwh,
        -- Ağırlıklı Ortalama Fiyat (WAP)
        SAFE_DIVIDE(SUM(transaction_price_try * transaction_quantity_mwh), SUM(transaction_quantity_mwh)) AS gip_weighted_avg_price_try,
        COUNT(1) AS total_transaction_count
    FROM {{ ref('stg_idm_transactions') }}
    GROUP BY 1
),

weather_hourly AS (
    SELECT
        TIMESTAMP_TRUNC(date_timestamp, HOUR) AS date,
        AVG(temperature_2m) AS avg_temperature_c,
        AVG(wind_speed_10m) AS avg_windspeed_kmh,
        AVG(shortwave_radiation) AS avg_solar_radiation_wm2
    FROM {{ ref('stg_weather') }}
    GROUP BY 1
),

pricing_hourly AS (
    SELECT
        TIMESTAMP_TRUNC(date_timestamp, HOUR) AS date,
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