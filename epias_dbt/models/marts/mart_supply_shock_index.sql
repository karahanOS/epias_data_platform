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
    SELECT date, total_available_capacity_mwh 
    FROM {{ ref('stg_aic') }}
),

outages AS (
    -- Saatlik Toplam Santral Arızaları/Bakımları
    SELECT 
        TIMESTAMP_TRUNC(date_timestamp, HOUR) AS date, 
        SUM(outage_capacity_mwh) AS total_outage_mwh 
    FROM {{ ref('stg_outages') }}
    GROUP BY 1
),

dams AS (
    -- Günlük Baraj Su Seviyesi (Ortalama)
    SELECT 
        date AS day_date, 
        AVG(water_level_m) AS avg_water_level_m,
        SUM(active_volume_hm3) AS total_active_volume_hm3
    FROM {{ ref('stg_dams') }}
    GROUP BY 1
),

pricing AS (
    SELECT date, ptf_try FROM {{ ref('stg_pricing') }}
)

SELECT
    a.date,
    p.ptf_try,
    a.total_available_capacity_mwh,
    COALESCE(o.total_outage_mwh, 0) AS total_outage_mwh,
    d.avg_water_level_m,
    d.total_active_volume_hm3,
    
    -- Arz Şoku İndeksi (Kapasitenin yüzde kaçı arızalı?)
    SAFE_DIVIDE(COALESCE(o.total_outage_mwh, 0), a.total_available_capacity_mwh) * 100 AS supply_shock_index_pct
FROM aic a
LEFT JOIN pricing p ON a.date = p.date
LEFT JOIN outages o ON a.date = o.date
LEFT JOIN dams d ON CAST(a.date AS DATE) = d.day_date