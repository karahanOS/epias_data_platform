{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH idm_hourly AS (
    -- GİP işlemlerini saatlik bazda kümülatif hale getiriyoruz (Hacim ve Ağırlıklı Ortalama Fiyat)
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        SUM(quantity) AS total_gip_volume_mwh,
        -- Ağırlıklı Ortalama Fiyat (WAP) Formülü: Sum(Fiyat * Miktar) / Sum(Miktar)
        SAFE_DIVIDE(SUM(price * quantity), SUM(quantity)) AS gip_weighted_avg_price_try,
        COUNT(1) AS total_transaction_count
    FROM {{ source('silver', 'idm_transactions') }}
    GROUP BY 1
),

weather_hourly AS (
    -- Open-Meteo'dan gelen şehir bazlı verileri Türkiye geneli saatlik ortalama olarak alıyoruz
    -- (İleride bölge bazlı ağırlıklandırma yapılabilir)
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        AVG(temperature_2m) AS avg_temperature_c,
        AVG(windspeed_10m) AS avg_windspeed_kmh,
        AVG(shortwave_radiation) AS avg_solar_radiation_wm2,
        AVG(cloudcover) AS avg_cloudcover_percent
    FROM {{ source('silver', 'weather') }}
    GROUP BY 1
),

pricing_hourly AS (
    -- Referans olarak o saatin GÖP fiyatını (PTF) çekiyoruz
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        AVG(marketTradePrice) AS ptf_try
    FROM {{ source('silver', 'pricing') }}
    GROUP BY 1
),

joined_impact AS (
    -- GİP, Hava Durumu ve PTF'yi saat (date) bazında birleştiriyoruz
    SELECT
        i.date,
        i.total_gip_volume_mwh,
        i.gip_weighted_avg_price_try,
        i.total_transaction_count,
        p.ptf_try,
        
        -- Finansal Metrik: GİP ve PTF arasındaki fiyat makası
        -- Spread pozitifse: GİP'te elektrik GÖP'ten daha pahalıya satılmış (Piyasada açık var)
        -- Spread negatifse: GİP'te elektrik daha ucuza satılmış (Piyasada fazla var)
        (i.gip_weighted_avg_price_try - p.ptf_try) AS gip_ptf_spread_try,
        
        w.avg_temperature_c,
        w.avg_windspeed_kmh,
        w.avg_solar_radiation_wm2,
        w.avg_cloudcover_percent

    FROM idm_hourly i
    LEFT JOIN weather_hourly w ON i.date = w.date
    LEFT JOIN pricing_hourly p ON i.date = p.date
)

SELECT * FROM joined_impact