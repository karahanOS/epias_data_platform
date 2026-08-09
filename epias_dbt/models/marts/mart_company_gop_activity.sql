{{ config(
    materialized='table',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_company_gop_activity: "Hangi şirket GÖP piyasasını ne kadar kullanıyor?"
--
-- ADR-0007 Faz 1 (plans/07-company-level-market-activity-kgup.md): GİP'te
-- (Gün İçi Piyasası) hiçbir EPİAŞ endpoint'i işlemi bir şirkete atfetmiyor —
-- 16 GİP endpoint'i tek tek doğrulandı, hiçbirinde organizasyon alanı yok
-- (bkz. mart_gip_company_analysis.sql, kalıcı olarak boş). GÖP'te ise
-- clearing-quantity endpoint'i organizationId filtresini destekliyor — bu
-- mart o veriye dayanıyor. GİP değil, GÖP (Gün Öncesi Piyasası) aktivitesi.
--
-- Kolon açıklamaları:
--   total_bids_mwh    — şirketin o gün GÖP'te eşleşen toplam alış miktarı
--   total_offers_mwh  — şirketin o gün GÖP'te eşleşen toplam satış miktarı
--   net_position_mwh  — alış - satış; pozitif → net alıcı (talep fazlası),
--                        negatif → net satıcı (üretim fazlası)
--   total_volume_mwh  — alış + satış (o günkü toplam GÖP aktivitesi)
--   active_hours       — o gün en az bir taraftan sıfırdan farklı hacim
--                         gördüğü saat sayısı (0-24) — çoğu şirket çoğu gün
--                         için pasif olabilir, bu gürültü değil gerçek sinyal
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date                                                     AS trade_date,
    organization_id,
    organization_name,
    SUM(matched_bids_mwh)                                    AS total_bids_mwh,
    SUM(matched_offers_mwh)                                  AS total_offers_mwh,
    SUM(matched_bids_mwh) - SUM(matched_offers_mwh)          AS net_position_mwh,
    SUM(matched_bids_mwh) + SUM(matched_offers_mwh)          AS total_volume_mwh,
    COUNTIF(matched_bids_mwh > 0 OR matched_offers_mwh > 0)  AS active_hours
FROM {{ ref('stg_dam_clearing_by_org') }}
GROUP BY 1, 2, 3
