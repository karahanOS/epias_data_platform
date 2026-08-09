{{ config(
    materialized='table',
    partition_by={"field": "trade_date", "data_type": "date"}
) }}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_company_production_activity: "Hangi şirket ne kadar üretim planlıyor?"
--
-- ADR-0007 Faz 2 (plans/07-company-level-market-activity-kgup.md): şirket bazlı
-- KGÜP (Kesinleşmiş Günlük Üretim Planı) aktivitesi — üretim planlama tarafı.
-- Faz 1'in mart_company_gop_activity'si (GÖP alım-satım hacmi) ile birlikte
-- okunmalı; ikisi farklı piyasa katmanlarını temsil ediyor (üretim planı vs
-- gün öncesi piyasa ticareti), GİP'in kendisi değil (bkz. ADR — hiçbir EPİAŞ
-- endpoint'i GİP işlemini bir şirkete atfetmiyor).
--
-- Kolon açıklamaları:
--   active_uevcb_count — o gün üretim planı bildirilen santral/birim sayısı
--   total_kgup_mwh      — şirketin o günkü toplam kesinleşmiş üretim planı
--   diğer yakıt kolonları — kaynak tipine göre kırılım
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date                                                          AS trade_date,
    k.org_id,
    p.organization_name,
    COUNT(DISTINCT k.uevcb_id)                                    AS active_uevcb_count,
    SUM(k.total_kgup_mwh)                                         AS total_kgup_mwh,
    SUM(k.natural_gas_mwh)                                        AS natural_gas_mwh,
    SUM(k.wind_mwh)                                                AS wind_mwh,
    SUM(k.dam_hydro_mwh + k.river_hydro_mwh)                      AS hydro_mwh,
    SUM(k.solar_mwh)                                               AS solar_mwh,
    SUM(k.lignite_mwh + k.hard_coal_mwh + k.imported_coal_mwh)    AS coal_mwh
FROM {{ ref('stg_kgup_bulk_by_org') }} k
LEFT JOIN {{ ref('stg_participants') }} p
    ON k.org_id = p.organization_id
GROUP BY 1, 2, 3
