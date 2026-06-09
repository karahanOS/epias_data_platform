{{ config(
    materialized='table',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_gip_company_analysis: "Hangi şirket GİP piyasasını kullanıyor?"
--
-- ⚠️  DATA AVAILABILITY NOTE (2026-06-09):
-- EPİAŞ /v1/markets/idm/data/transaction-history does NOT return
-- buyerOrganizationId / sellerOrganizationId.  stg_idm_transactions falls back
-- to CAST(NULL AS INT64) for both columns when they are absent from Silver.
-- As a result the WHERE ... IS NOT NULL clauses in both CTEs produce 0 rows and
-- this mart is always empty until EPİAŞ exposes participant IDs on that endpoint
-- (or a separate company-level IDM endpoint is wired up in epias_client.py).
-- The dashboard's GİP page already handles the 0-row case gracefully.
--
-- Her IDM (GİP) işleminin hem alıcı (buyer) hem satıcı (seller) tarafını
-- stg_participants ile çözümleyerek şirket adını ve net pozisyonunu hesaplar.
--
-- Kolon açıklamaları:
--   total_buy_mwh    — şirketin o gün aldığı toplam enerji (MWh)
--   total_sell_mwh   — şirketin o gün sattığı toplam enerji (MWh)
--   net_position_mwh — net_buy - net_sell; pozitif → net alıcı (talep fazlası),
--                      negatif → net satıcı (üretim fazlası)
--   buy_ratio        — alış hacminin toplam hacme oranı [0–1]
--   total_volume_mwh — alış + satış (her iki taraf toplam aktivite)
--   wap_buy_try      — ağırlıklı ortalama alış fiyatı (TL/MWh)
--   wap_sell_try     — ağırlıklı ortalama satış fiyatı (TL/MWh)
-- ─────────────────────────────────────────────────────────────────────────────

WITH buyer_agg AS (
    SELECT
        t.date                                                       AS trade_date,
        t.buyer_organization_id                                      AS organization_id,
        SUM(t.quantity_mwh)                                          AS buy_mwh,
        COUNT(*)                                                     AS buy_count,
        SAFE_DIVIDE(
            SUM(t.price_try * t.quantity_mwh),
            SUM(t.quantity_mwh)
        )                                                            AS wap_buy_try
    FROM {{ ref('stg_idm_transactions') }} t
    WHERE t.buyer_organization_id IS NOT NULL
    GROUP BY 1, 2
),

seller_agg AS (
    SELECT
        t.date                                                       AS trade_date,
        t.seller_organization_id                                     AS organization_id,
        SUM(t.quantity_mwh)                                          AS sell_mwh,
        COUNT(*)                                                     AS sell_count,
        SAFE_DIVIDE(
            SUM(t.price_try * t.quantity_mwh),
            SUM(t.quantity_mwh)
        )                                                            AS wap_sell_try
    FROM {{ ref('stg_idm_transactions') }} t
    WHERE t.seller_organization_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    COALESCE(b.trade_date,  s.trade_date)                            AS trade_date,
    COALESCE(b.organization_id, s.organization_id)                   AS organization_id,
    p.organization_name,
    p.organization_code,

    -- Alış tarafı
    COALESCE(b.buy_mwh,   0.0)                                       AS total_buy_mwh,
    COALESCE(b.buy_count, 0)                                         AS buy_transaction_count,
    b.wap_buy_try,

    -- Satış tarafı
    COALESCE(s.sell_mwh,   0.0)                                      AS total_sell_mwh,
    COALESCE(s.sell_count, 0)                                        AS sell_transaction_count,
    s.wap_sell_try,

    -- Net pozisyon & özet metrikler
    COALESCE(b.buy_mwh, 0.0) - COALESCE(s.sell_mwh, 0.0)           AS net_position_mwh,
    COALESCE(b.buy_mwh, 0.0) + COALESCE(s.sell_mwh, 0.0)           AS total_volume_mwh,
    COALESCE(b.buy_count, 0)  + COALESCE(s.sell_count, 0)           AS total_transaction_count,
    SAFE_DIVIDE(
        COALESCE(b.buy_mwh, 0.0),
        NULLIF(COALESCE(b.buy_mwh, 0.0) + COALESCE(s.sell_mwh, 0.0), 0)
    )                                                                AS buy_ratio

FROM buyer_agg  b
FULL OUTER JOIN seller_agg s
    ON b.trade_date = s.trade_date AND b.organization_id = s.organization_id
LEFT JOIN {{ ref('stg_participants') }} p
    ON COALESCE(b.organization_id, s.organization_id) = p.organization_id
