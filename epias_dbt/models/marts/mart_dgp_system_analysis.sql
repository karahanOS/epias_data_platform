{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_dgp_system_analysis: Saatlik sistem DGP (Dengeleme Güç Piyasası) metrikleri
--
-- ⚠️ EPIAS /markets/bpm/data/order-summary-up ve order-summary-down endpoint'leri
--    sistem geneli aggregate veri döndürür.  Organizasyon (şirket) bazlı DGP verisi
--    EPIAS balance-power/organization endpoint'inden çekilebilir ancak bu endpoint
--    farklı API yetkisi gerektirir ve şu an Bronze katmanında mevcut değildir.
--    Bu mart SADECE sistem düzeyinde YAL/YAT hacimlerini içerir.
--
-- Kolonlar:
--   yal_*              — Yük Alma  (upward regulation)  : sistem enerji açığı
--   yat_*              — Yük Atma  (downward regulation) : sistem enerji fazlası
--   net_dgp_mwh > 0   → sistem açık (arz < talep); enerji ithal edildi
--   net_dgp_mwh < 0   → sistem fazla (arz > talep); enerji ihraç edildi
--   zero_price_ratio_* — bedelsiz regülasyon oranı; yüksek değer zorunlu kaynak sinyali
-- ─────────────────────────────────────────────────────────────────────────────

WITH yal AS (
    SELECT
        date,
        hour,
        up_regulation_delivered_mwh  AS yal_delivered_mwh,
        up_regulation_zero_mwh       AS yal_zero_price_mwh,
        up_regulation_one_mwh        AS yal_one_coded_mwh,
        up_regulation_two_mwh        AS yal_two_coded_mwh,
        net_mwh                      AS yal_net_mwh
    FROM {{ ref('stg_order_up') }}
),
yat AS (
    SELECT
        date,
        hour,
        down_regulation_delivered_mwh AS yat_delivered_mwh,
        down_regulation_zero_mwh      AS yat_zero_price_mwh,
        down_regulation_one_mwh       AS yat_one_coded_mwh,
        down_regulation_two_mwh       AS yat_two_coded_mwh,
        net_mwh                       AS yat_net_mwh
    FROM {{ ref('stg_order_down') }}
)

SELECT
    COALESCE(u.date, d.date)                              AS date,
    COALESCE(u.hour, d.hour)                              AS hour,

    -- ── YAL (Yük Alma — Upward Regulation) ──────────────────────────────
    COALESCE(u.yal_delivered_mwh,  0.0)                  AS yal_delivered_mwh,
    COALESCE(u.yal_zero_price_mwh, 0.0)                  AS yal_zero_price_mwh,
    COALESCE(u.yal_one_coded_mwh,  0.0)                  AS yal_one_coded_mwh,
    COALESCE(u.yal_two_coded_mwh,  0.0)                  AS yal_two_coded_mwh,
    COALESCE(u.yal_net_mwh,        0.0)                  AS yal_net_mwh,

    -- ── YAT (Yük Atma — Downward Regulation) ────────────────────────────
    COALESCE(d.yat_delivered_mwh,  0.0)                  AS yat_delivered_mwh,
    COALESCE(d.yat_zero_price_mwh, 0.0)                  AS yat_zero_price_mwh,
    COALESCE(d.yat_one_coded_mwh,  0.0)                  AS yat_one_coded_mwh,
    COALESCE(d.yat_two_coded_mwh,  0.0)                  AS yat_two_coded_mwh,
    COALESCE(d.yat_net_mwh,        0.0)                  AS yat_net_mwh,

    -- ── Net DGP Pozisyonu ─────────────────────────────────────────────────
    COALESCE(u.yal_delivered_mwh, 0.0)
        - COALESCE(d.yat_delivered_mwh, 0.0)             AS net_dgp_mwh,

    -- ── Bedelsiz Regülasyon Oranları (Zorunlu Kaynak Göstergesi) ─────────
    SAFE_DIVIDE(
        COALESCE(u.yal_zero_price_mwh, 0.0),
        NULLIF(COALESCE(u.yal_delivered_mwh, 0.0), 0.0)
    )                                                     AS zero_price_ratio_yal,
    SAFE_DIVIDE(
        COALESCE(d.yat_zero_price_mwh, 0.0),
        NULLIF(COALESCE(d.yat_delivered_mwh, 0.0), 0.0)
    )                                                     AS zero_price_ratio_yat,

    -- ── Toplam Regülasyon Aktivitesi ──────────────────────────────────────
    COALESCE(u.yal_delivered_mwh, 0.0)
        + COALESCE(d.yat_delivered_mwh, 0.0)             AS total_regulation_mwh

FROM yal u
FULL OUTER JOIN yat d ON u.date = d.date AND u.hour = d.hour
