{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_cross_market_spread: GÖP → GİP → DGP Üç Piyasa Arbitraj Analizi
--
-- Literatür temeli:
--   Maciejowska, Nitka & Weron (Energies) — "Day-Ahead vs. Intraday:
--     Forecasting the Price Spread to Maximize Economic Benefits"
--     → GİP-GÖP spread tahmini, kademeli fiyat yapısının tespiti
--
--   Wozabal & Rameseder (EJOR) — "Optimal bidding of a virtual power plant
--     on the Spanish day-ahead and intraday market"
--     → Üç piyasa portföy optimizasyon sinyalleri
--
--   Ferreira, Sousa & Lagarto — "Optimal bidding strategies for electricity
--     generation assets in the Day-Ahead, Intraday, and Balancing Markets"
--     → Sistem yönü × spread kombinasyonu → işlem fırsatı skoru
--
-- Piyasa Mantığı (Türkiye):
--   GÖP (Gün Öncesi)    = PTF    — gün öncesi kapanış fiyatı
--   GİP (Gün İçi)       = IDM    — gerçek zamana yakın işlem
--   DGP (Dengeleme)     = SMF    — sistem dengelenme fiyatı
--
-- Spread Yorumu:
--   GİP - GÖP > 0  → Gün içi daha pahalı; GÖP'te al, GİP'te sat fırsatı
--   GİP - GÖP < 0  → GÖP daha pahalı; öngörüde GÖP'te fazla teklif verilmemiş
--   SMF - GÖP > 0  → Balans piyasası daha pahalı; sistem açık kalmış
--   SMF - GİP > 0  → Balans, GİP'ten de pahalı; portföy GİP'te kapamamış
-- ─────────────────────────────────────────────────────────────────────────────

WITH
-- ── GÖP: Saatlik PTF ──────────────────────────────────────────────────────
gop AS (
    SELECT date, hour, ptf_try
    FROM {{ ref('stg_pricing') }}
),

-- ── GİP: Saatlik ortalama işlem fiyatı & hacmi ───────────────────────────
-- mart_gip_company_activity contract bazında; saatlik ağırlıklı ortalama için
-- stg_idm_transactions'dan direkt alıyoruz
gip AS (
    SELECT
        date,
        hour,
        SAFE_DIVIDE(
            SUM(price_try * quantity_mwh),
            SUM(quantity_mwh)
        )                       AS gip_vwap_try,        -- hacim ağırlıklı ort.
        AVG(price_try)          AS gip_avg_price_try,
        SUM(quantity_mwh)       AS gip_total_volume_mwh,
        COUNT(id)               AS gip_transaction_count,
        MIN(price_try)          AS gip_min_price_try,
        MAX(price_try)          AS gip_max_price_try,
        MAX(price_try) - MIN(price_try) AS gip_price_range_try
    FROM {{ ref('stg_idm_transactions') }}
    GROUP BY 1, 2
),

-- ── DGP Proxy: SMF ───────────────────────────────────────────────────────
dgp AS (
    SELECT date, hour, smf_try
    FROM {{ ref('stg_smf') }}
),

-- ── Sistem Yönü & İmbalans ────────────────────────────────────────────────
sys AS (
    SELECT
        sd.date,
        sd.hour,
        sd.system_direction,
        imb.net_imbalance_mwh,
        imb.total_generation_mwh,
        imb.total_consumption_mwh
    FROM {{ ref('stg_system_direction') }} sd
    LEFT JOIN {{ ref('stg_imbalance') }} imb
        ON sd.date = imb.date AND sd.hour = imb.hour
),

-- ── DGP Regülasyon Hacimleri (Sistem geneli YAL / YAT) ───────────────────
-- Pozitif YAL teslimi → sistem açık kalmış → SMF yüksek
-- Pozitif YAT teslimi → sistem fazla → SMF düşük
dgp_vol AS (
    SELECT
        u.date,
        u.hour,
        COALESCE(u.up_regulation_delivered_mwh, 0)   AS yal_delivered_mwh,
        COALESCE(d.down_regulation_delivered_mwh, 0) AS yat_delivered_mwh,
        COALESCE(u.up_regulation_delivered_mwh, 0)
            - COALESCE(d.down_regulation_delivered_mwh, 0) AS net_dgp_mwh
    FROM {{ ref('stg_order_up') }} u
    LEFT JOIN {{ ref('stg_order_down') }} d
        ON u.date = d.date AND u.hour = d.hour
)

SELECT
    g.date,
    g.hour,

    -- ── Ham Fiyatlar ──────────────────────────────────────────────────────
    g.ptf_try                               AS gop_ptf_try,
    gi.gip_vwap_try,
    gi.gip_avg_price_try,
    d.smf_try                               AS dgp_smf_try,

    -- ── Üç Piyasa Spread'leri (Maciejowska et al.) ────────────────────────
    -- Pozitif → sağ piyasa daha pahalı
    (gi.gip_vwap_try - g.ptf_try)           AS gip_gop_spread_try,
    (d.smf_try      - g.ptf_try)            AS smf_gop_spread_try,
    (d.smf_try      - gi.gip_vwap_try)      AS smf_gip_spread_try,

    -- ── Spread Yönü & Büyüklüğü ──────────────────────────────────────────
    ABS(gi.gip_vwap_try - g.ptf_try)        AS gip_gop_spread_abs,
    SAFE_DIVIDE(
        ABS(gi.gip_vwap_try - g.ptf_try),
        g.ptf_try
    ) * 100                                 AS gip_gop_spread_pct,

    -- ── Kademeli Fiyat Yapısı Tespiti ─────────────────────────────────────
    -- Wozabal: her iki spread aynı yöndeyse "escalating/descending cascade"
    CASE
        WHEN gi.gip_vwap_try > g.ptf_try
             AND d.smf_try   > gi.gip_vwap_try
            THEN 'ESCALATING'         -- GÖP < GİP < DGP → sistem giderek açıldı
        WHEN gi.gip_vwap_try < g.ptf_try
             AND d.smf_try   < gi.gip_vwap_try
            THEN 'DESCENDING'         -- GÖP > GİP > DGP → sistem kapandı
        WHEN gi.gip_vwap_try > g.ptf_try
             AND d.smf_try   < gi.gip_vwap_try
            THEN 'GIP_PEAK'           -- GİP zirvede; GÖP ve DGP ucuz
        WHEN gi.gip_vwap_try < g.ptf_try
             AND d.smf_try   > gi.gip_vwap_try
            THEN 'GIP_TROUGH'         -- GİP en ucuz; fırsatçı alım noktası
        ELSE 'FLAT'
    END                                     AS price_cascade,

    -- ── Arbitraj Sinyal Skoru (Ferreira et al.) ───────────────────────────
    -- Sistem açık (ENERGY_DEFICIT) + fiyat kademeli yükseliyor:
    --   → GÖP'te erken teklif optimal; skor yükseldikçe fırsat artar
    -- Basit skor: spread büyüklüğü × sistem baskısı
    CASE
        WHEN s.system_direction = 'ENERGY_DEFICIT'
             AND gi.gip_vwap_try > g.ptf_try
            THEN  ABS(gi.gip_vwap_try - g.ptf_try) / NULLIF(g.ptf_try, 0)
        WHEN s.system_direction = 'ENERGY_SURPLUS'
             AND gi.gip_vwap_try < g.ptf_try
            THEN  ABS(gi.gip_vwap_try - g.ptf_try) / NULLIF(g.ptf_try, 0)
        ELSE 0
    END                                     AS arbitrage_opportunity_score,

    -- ── Sistem Yönü & İmbalans ────────────────────────────────────────────
    s.system_direction,
    COALESCE(s.net_imbalance_mwh, 0)        AS net_imbalance_mwh,

    -- ── DGP Regülasyon Baskısı ────────────────────────────────────────────
    COALESCE(dv.yal_delivered_mwh, 0)       AS yal_delivered_mwh,
    COALESCE(dv.yat_delivered_mwh, 0)       AS yat_delivered_mwh,
    COALESCE(dv.net_dgp_mwh, 0)            AS net_dgp_mwh,

    -- ── GİP Piyasa Derinliği ──────────────────────────────────────────────
    COALESCE(gi.gip_total_volume_mwh, 0)    AS gip_total_volume_mwh,
    COALESCE(gi.gip_transaction_count, 0)   AS gip_transaction_count,
    gi.gip_min_price_try,
    gi.gip_max_price_try,
    gi.gip_price_range_try

FROM gop g
LEFT JOIN gip    gi  ON g.date = gi.date AND g.hour = gi.hour
LEFT JOIN dgp    d   ON g.date = d.date  AND g.hour = d.hour
LEFT JOIN sys    s   ON g.date = s.date  AND g.hour = s.hour
LEFT JOIN dgp_vol dv ON g.date = dv.date AND g.hour = dv.hour
