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
    -- NOT: LAG(x, 168) ve ROWS BETWEEN 168 PRECEDING pencere fonksiyonları,
    -- hesaplanan her satırdan önce tam 168 (gerçek) satır bulunmasını gerektirir.
    -- Incremental çalışmada base'i sadece son birkaç güne kısıtlamak (örn. 7 gün),
    -- pencerenin kendisini "yetersiz geçmiş" haline getirir — bu da en güncel
    -- günlerde ptf_lag_168h/ptf_rolling_avg_168h'nin sistematik NULL kalmasına
    -- neden olur (ptf_inference.py'deki dropna bu NULL satırları elediği için
    -- inference her seferinde eski/rastgele bir satırı "en güncel" sanıyordu).
    -- Kaynak tablo (mart_ml_features) büyük değil; pencere hesaplarını her
    -- incremental çalışmada TAM geçmiş üzerinden yapmak güvenli ve ucuz —
    -- MERGE hedefi zaten aşağıdaki son WHERE ile tek güne sınırlanıyor.
    SELECT * FROM {{ ref('mart_ml_features') }}
),

with_lags AS (
    SELECT
        *,
        -- Datetime sütunu (trainer için datetime index)
        TIMESTAMP_ADD(
            TIMESTAMP(date, 'Asia/Istanbul'),
            INTERVAL CAST(hour AS INT64) HOUR
        ) AS datetime,

        -- PTF lag özellikleri
        LAG(ptf_try, 1)   OVER (ORDER BY date, hour) AS ptf_lag_1h,
        LAG(ptf_try, 24)  OVER (ORDER BY date, hour) AS ptf_lag_24h,
        LAG(ptf_try, 168) OVER (ORDER BY date, hour) AS ptf_lag_168h,

        -- SMF lag özellikleri (leakage-free: gelecekteki SMF bilinmiyor)
        -- T-24h: dünün aynı saatinin dengeleme fiyatı → yarının fiyat baskısı sinyali
        -- T-168h: geçen haftanın aynı saati → haftalık mevsimsel SMF deseni
        LAG(smf_try, 24)  OVER (ORDER BY date, hour) AS smf_try_lag_24h,
        LAG(smf_try, 168) OVER (ORDER BY date, hour) AS smf_try_lag_168h,

        -- Son 24 saatlik istatistikler (önceki gün aynı saat profili)
        AVG(ptf_try) OVER (
            ORDER BY date, hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS ptf_rolling_avg_24h,
        MAX(ptf_try) OVER (
            ORDER BY date, hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS ptf_rolling_max_24h,
        MIN(ptf_try) OVER (
            ORDER BY date, hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS ptf_rolling_min_24h,

        -- Son 168 saatlik (7 gün) hareketli ortalama — haftalık mevsimsellik
        AVG(ptf_try) OVER (
            ORDER BY date, hour
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
        ) AS ptf_rolling_avg_168h
    FROM base
)

SELECT * FROM with_lags
{% if is_incremental() %}
  -- Sadece yeni hesaplanan günleri ana tabloya ekle/güncelle
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}