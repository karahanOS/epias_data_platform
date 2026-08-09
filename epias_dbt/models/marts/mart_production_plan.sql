{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- Üretim Planı Karşılaştırması: KGÜP (gün öncesi kesinleşen) vs KUDÜP (uzlaştırma
-- dönemi kesinleşen)
--
-- İsimlendirme notu: bu model önceden "BGÜP vs KGÜP" olarak etiketlenmişti. EPİAŞ'ın
-- kendi Şeffaflık Platformu dokümantasyonu karşılaştırıldığında ikisi de yanlış
-- çıktı (bkz. plans/07-company-level-market-activity-kgup.md):
--   - stg_dpp (get_dpp() / POST .../dpp)   → EPİAŞ 5.71 "Kesinleşmiş Günlük Üretim
--     Planı (KGÜP)" — "Beyan" değil.
--   - stg_sbfgp (get_sbfgp() / POST .../sbfgp) → EPİAŞ 5.83 "Kesinleştirilmiş
--     Uzlaştırma Dönemi Üretim Planı (KUDÜP)" — sadece "KGÜP" değil, KGÜP'ten
--     SONRAKİ ayrı bir katman.
-- Gerçek BGÜP karşılığı (katılımcının ilk bildirdiği değer) hiç wire edilmedi:
-- POST /v1/generation/data/dpp-first-version (EPİAŞ 5.73).
--
-- KGÜP  (stg_dpp)  : Gün öncesi TEİAŞ tarafından kesinleştirilen üretim planı
-- KUDÜP (stg_sbfgp): GİP kapanışı sonrası, DUY 69. madde kapsamında güncellenen,
--                     uzlaştırmaya esas nihai plan
--
-- Fark (KUDÜP - KGÜP) = Intraday Revizyon:
--   Bu, önceki "KGÜP - BGÜP" etiketiyle aynı alttaki sinyaldir (aynı iki API
--   çağrısı, aynı hesaplama) — sadece iki tarafın adı yanlıştı, delta'nın kendisi
--   ve iş anlamı (KGÜP kesinleştikten sonra, GİP'te ne kadar ek alım/satım
--   yapıldığı) değişmedi:
--   Pozitif → GİP'te ek alım yapıldı (talep arttı / üretim düştü)
--   Negatif → GİP'te satış yapıldı (talep azaldı / üretim arttı)
--
-- Kullanım: "Kesinleşmiş üretim planlamasında ne kadar üretim var?" (Q4)
--           + GİP piyasasında ne kadar revizyon yapıldığı (Q5 bağlantısı)

WITH kgup AS (
    SELECT date, hour,
           total_planned_mwh, natural_gas_mwh, wind_mwh, lignite_mwh,
           hard_coal_mwh, imported_coal_mwh, dam_hydro_mwh, river_hydro_mwh,
           solar_mwh, geothermal_mwh, biomass_mwh
    FROM {{ ref('stg_dpp') }}
),
kudup AS (
    SELECT date, hour,
           total_kgup_mwh, natural_gas_mwh, wind_mwh, lignite_mwh,
           hard_coal_mwh, imported_coal_mwh, dam_hydro_mwh, river_hydro_mwh,
           solar_mwh, geothermal_mwh, biomass_mwh
    FROM {{ ref('stg_sbfgp') }}
)

SELECT
    COALESCE(u.date, k.date)   AS date,
    COALESCE(u.hour, k.hour)   AS hour,

    -- KGÜP değerleri (gün öncesi kesinleşen)
    k.total_planned_mwh        AS kgup_total_mwh,
    k.wind_mwh                 AS kgup_wind_mwh,
    k.solar_mwh                AS kgup_solar_mwh,
    k.dam_hydro_mwh            AS kgup_hydro_mwh,

    -- KUDÜP değerleri (uzlaştırma dönemi kesinleşen)
    u.total_kgup_mwh           AS kudup_total_mwh,
    u.wind_mwh                 AS kudup_wind_mwh,
    u.solar_mwh                AS kudup_solar_mwh,
    u.dam_hydro_mwh            AS kudup_hydro_mwh,

    -- Intraday revizyon (KUDÜP - KGÜP)
    (u.total_kgup_mwh - k.total_planned_mwh)  AS intraday_revision_mwh,
    (u.wind_mwh - k.wind_mwh)                 AS wind_revision_mwh,
    (u.solar_mwh - k.solar_mwh)               AS solar_revision_mwh,
    (u.dam_hydro_mwh - k.dam_hydro_mwh)       AS hydro_revision_mwh,

    -- Revizyon yönü
    CASE
        WHEN (u.total_kgup_mwh - k.total_planned_mwh) > 50  THEN 'GIP_Alim'
        WHEN (u.total_kgup_mwh - k.total_planned_mwh) < -50 THEN 'GIP_Satis'
        ELSE 'Denge'
    END AS revision_direction,

    -- Revizyon büyüklüğü (KGÜP'e göre %)
    SAFE_DIVIDE(
        ABS(u.total_kgup_mwh - k.total_planned_mwh),
        k.total_planned_mwh
    ) * 100                    AS revision_pct

FROM kudup u
FULL OUTER JOIN kgup k ON u.date = k.date AND u.hour = k.hour
