{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH aic AS (
    -- Emre Amade Kapasite (Fiziksel olarak üretilebilecek maksimum güç)
    SELECT date, total AS total_available_capacity_mwh 
    FROM {{ source('silver', 'aic') }}
),

outages AS (
    -- Saatlik Toplam Santral Arızaları/Bakımları
    SELECT 
        TIMESTAMP_TRUNC(date, HOUR) AS date, 
        SUM(outageCapacity) AS total_outage_mwh 
    FROM {{ source('silver', 'outages') }}
    GROUP BY 1
),

dams AS (
    -- Günlük Baraj Su Seviyesi (Ortalama)
    SELECT 
        TIMESTAMP_TRUNC(date, DAY) AS day_date, 
        AVG(waterLevel) AS avg_water_level_m,
        SUM(activeVolume) AS total_active_volume_hm3
    FROM {{ source('silver', 'dams') }}
    GROUP BY 1
),

pricing AS (
    SELECT date, marketTradePrice AS ptf_try FROM {{ source('silver', 'pricing') }}
)

SELECT
    a.date,
    p.ptf_try,
    a.total_available_capacity_mwh,
    COALESCE(o.total_outage_mwh, 0) AS total_outage_mwh,
    d.avg_water_level_m,
    d.total_active_volume_hm3,
    
    -- KRİTİK FEATURE 3: Sistem Arz Stres İndeksi
    -- Arızalı kapasitenin, mevcut kapasiteye oranı. %5'i geçerse tehlike çanları çalar.
    SAFE_DIVIDE(COALESCE(o.total_outage_mwh, 0), a.total_available_capacity_mwh) * 100 AS supply_stress_pct

FROM aic a
LEFT JOIN outages o ON a.date = o.date
-- Barajlar günlük veri olduğu için HOUR -> DAY dönüşümü ile JOIN atıyoruz
LEFT JOIN dams d ON TIMESTAMP_TRUNC(a.date, DAY) = d.day_date
LEFT JOIN pricing p ON a.date = p.date