{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- Üretim Planı Karşılaştırması: BGÜP (Beyan) vs KGÜP (Kesinleşmiş)
--
-- BGÜP (stg_dpp)  : Gün öncesinde şirketlerin TEIAŞ'a bildirdiği plan
-- KGÜP (stg_sbfgp): GİP kapanışı sonrası güncellenen nihai kesinleşmiş plan
--
-- Fark (KGÜP - BGÜP) = Intraday Revizyon:
--   Pozitif → GİP'te ek alım yapıldı (talep arttı / üretim düştü)
--   Negatif → GİP'te satış yapıldı (talep azaldı / üretim arttı)
--
-- Kullanım: "Kesinleşmiş üretim planlamasında ne kadar üretim var?" (Q4)
--           + GİP piyasasında ne kadar revizyon yapıldığı (Q5 bağlantısı)

WITH bgup AS (
    SELECT date, hour,
           total_planned_mwh, natural_gas_mwh, wind_mwh, lignite_mwh,
           hard_coal_mwh, imported_coal_mwh, dam_hydro_mwh, river_hydro_mwh,
           solar_mwh, geothermal_mwh, biomass_mwh
    FROM {{ ref('stg_dpp') }}
),
kgup AS (
    SELECT date, hour,
           total_kgup_mwh, natural_gas_mwh, wind_mwh, lignite_mwh,
           hard_coal_mwh, imported_coal_mwh, dam_hydro_mwh, river_hydro_mwh,
           solar_mwh, geothermal_mwh, biomass_mwh
    FROM {{ ref('stg_sbfgp') }}
)

SELECT
    COALESCE(k.date, b.date)   AS date,
    COALESCE(k.hour, b.hour)   AS hour,

    -- BGÜP değerleri
    b.total_planned_mwh        AS bgup_total_mwh,
    b.wind_mwh                 AS bgup_wind_mwh,
    b.solar_mwh                AS bgup_solar_mwh,
    b.dam_hydro_mwh            AS bgup_hydro_mwh,

    -- KGÜP değerleri
    k.total_kgup_mwh           AS kgup_total_mwh,
    k.wind_mwh                 AS kgup_wind_mwh,
    k.solar_mwh                AS kgup_solar_mwh,
    k.dam_hydro_mwh            AS kgup_hydro_mwh,

    -- Intraday revizyon (KGÜP - BGÜP)
    (k.total_kgup_mwh - b.total_planned_mwh)  AS intraday_revision_mwh,
    (k.wind_mwh - b.wind_mwh)                 AS wind_revision_mwh,
    (k.solar_mwh - b.solar_mwh)               AS solar_revision_mwh,
    (k.dam_hydro_mwh - b.dam_hydro_mwh)       AS hydro_revision_mwh,

    -- Revizyon yönü
    CASE
        WHEN (k.total_kgup_mwh - b.total_planned_mwh) > 50  THEN 'GIP_Alim'
        WHEN (k.total_kgup_mwh - b.total_planned_mwh) < -50 THEN 'GIP_Satis'
        ELSE 'Denge'
    END AS revision_direction,

    -- Revizyon büyüklüğü (BGÜP'e göre %)
    SAFE_DIVIDE(
        ABS(k.total_kgup_mwh - b.total_planned_mwh),
        b.total_planned_mwh
    ) * 100                    AS revision_pct

FROM kgup k
FULL OUTER JOIN bgup b ON k.date = b.date AND k.hour = b.hour
