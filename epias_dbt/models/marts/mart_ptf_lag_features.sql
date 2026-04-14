-- models/marts/mart_ptf_lag_features.sql
{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_ml_features') }}
    {% if is_incremental() %}
      
      WHERE date >= DATE_SUB((SELECT MAX(date) FROM {{ this }}), INTERVAL 7 DAY)
    {% endif %}
),

with_lags AS (
    SELECT
        *,
        -- 1 saat önceki PTF
        LAG(ptf, 1) OVER (ORDER BY date, hour) AS ptf_lag_1h,
        -- 24 saat önceki PTF (Dün aynı saat)
        LAG(ptf, 24) OVER (ORDER BY date, hour) AS ptf_lag_24h,
        -- 168 saat önceki PTF (Geçen hafta aynı saat)
        LAG(ptf, 168) OVER (ORDER BY date, hour) AS ptf_lag_168h,
        -- Son 24 saatin ortalaması
        AVG(ptf) OVER (
            ORDER BY date, hour 
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS ptf_rolling_avg_24h
    FROM base
)

SELECT * FROM with_lags
{% if is_incremental() %}
  -- Sadece yeni hesaplanan günleri ana tabloya ekle/güncelle
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}