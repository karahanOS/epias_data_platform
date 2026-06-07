{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
  )
}}

-- mart_regulation_depth: DGP düzenleme derinliği analizi
-- Yukarı/aşağı yönlü düzenleme emirlerini kodlu seviyelere göre analiz eder.
--
-- Terminoloji:
--   zero_coded  → Kod-0: serbest fiyatlı teklifler (en ucuz, önce aktivasyon)
--   one_coded   → Kod-1: tarifeli teklifler
--   two_coded   → Kod-2: zorunlu aktivasyon (en pahalı)
--   delivered   → Gerçekte aktive edilen hacim
--   net_position → up_net - down_net (pozitif = sistem açık, negatif = sistem fazla)

WITH up AS (
    SELECT
        date,
        hour,
        up_regulation_zero_mwh,
        up_regulation_one_mwh,
        up_regulation_two_mwh,
        up_regulation_delivered_mwh,
        net_mwh AS up_net_mwh,
        -- Toplam teklif hacmi
        COALESCE(up_regulation_zero_mwh, 0)
            + COALESCE(up_regulation_one_mwh, 0)
            + COALESCE(up_regulation_two_mwh, 0) AS up_total_offered_mwh,
        -- Aktivasyon oranı: teklif edilen hacmin ne kadarı gerçekleşti?
        SAFE_DIVIDE(
            up_regulation_delivered_mwh,
            COALESCE(up_regulation_zero_mwh, 0)
                + COALESCE(up_regulation_one_mwh, 0)
                + COALESCE(up_regulation_two_mwh, 0)
        ) AS up_activation_rate
    FROM {{ ref('stg_order_up') }}
),

down AS (
    SELECT
        date,
        hour,
        down_regulation_zero_mwh,
        down_regulation_one_mwh,
        down_regulation_two_mwh,
        down_regulation_delivered_mwh,
        net_mwh AS down_net_mwh,
        COALESCE(down_regulation_zero_mwh, 0)
            + COALESCE(down_regulation_one_mwh, 0)
            + COALESCE(down_regulation_two_mwh, 0) AS down_total_offered_mwh,
        SAFE_DIVIDE(
            down_regulation_delivered_mwh,
            COALESCE(down_regulation_zero_mwh, 0)
                + COALESCE(down_regulation_one_mwh, 0)
                + COALESCE(down_regulation_two_mwh, 0)
        ) AS down_activation_rate
    FROM {{ ref('stg_order_down') }}
)

SELECT
    COALESCE(u.date, d.date)   AS date,
    COALESCE(u.hour, d.hour)   AS hour,

    -- Yukarı yönlü (Up Regulation)
    u.up_regulation_zero_mwh,
    u.up_regulation_one_mwh,
    u.up_regulation_two_mwh,
    u.up_regulation_delivered_mwh,
    u.up_total_offered_mwh,
    u.up_activation_rate,
    u.up_net_mwh,

    -- Aşağı yönlü (Down Regulation)
    d.down_regulation_zero_mwh,
    d.down_regulation_one_mwh,
    d.down_regulation_two_mwh,
    d.down_regulation_delivered_mwh,
    d.down_total_offered_mwh,
    d.down_activation_rate,
    d.down_net_mwh,

    -- Net pozisyon: pozitif → sistem açıkta (yukarı baskı), negatif → fazla
    COALESCE(u.up_net_mwh, 0) - COALESCE(d.down_net_mwh, 0) AS net_system_position_mwh,

    -- Toplam piyasa aktivitesi
    COALESCE(u.up_total_offered_mwh, 0) + COALESCE(d.down_total_offered_mwh, 0) AS total_regulation_market_mwh,

    -- Asimetri oranı: >1 → yukarı baskı ağır basan, <1 → aşağı baskı ağır basan
    SAFE_DIVIDE(
        COALESCE(u.up_total_offered_mwh, 0),
        COALESCE(d.down_total_offered_mwh, 0)
    ) AS up_down_asymmetry_ratio

FROM up u
FULL OUTER JOIN down d
    ON u.date = d.date AND u.hour = d.hour

{% if is_incremental() %}
WHERE COALESCE(u.date, d.date) >= (SELECT DATE_SUB(MAX(date), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
