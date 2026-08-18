{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'org_id', 'uevcb_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ADR-0007 Faz 2 (plans/07-company-level-market-activity-kgup.md): UEVÇB +
-- şirket bazlı KGÜP (Kesinleşmiş Günlük Üretim Planı). EPİAŞ'ın kendi resmi
-- başlığı "Uevçb Bazlı Toplu KGÜP Listeleme Servisi" (dpp-bulk endpoint'i) —
-- kod tabanında daha önce (get_dpp_bulk) "BGÜP" deniyordu, yanlıştı (bkz. ADR).
-- Şirket adı için stg_participants'a JOIN edilmeli (mart katmanında) —
-- burada sadece org_id + uevcb_name (santral adı) taşınıyor.

-- 2026-08-19 DÜZELTME: s.date UTC bir TIMESTAMP; Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini yanlış tarihe
-- etiketliyordu (bkz. stg_pricing.sql'in aynı notu).
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')  AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)  AS hour,
    CAST(orgId     AS INT64)  AS org_id,
    CAST(uevcbId   AS INT64)  AS uevcb_id,
    CAST(uevcbName AS STRING) AS uevcb_name,
    CAST(toplam      AS FLOAT64) AS total_kgup_mwh,
    CAST(dogalgaz    AS FLOAT64) AS natural_gas_mwh,
    CAST(ruzgar      AS FLOAT64) AS wind_mwh,
    CAST(linyit      AS FLOAT64) AS lignite_mwh,
    CAST(tasKomur    AS FLOAT64) AS hard_coal_mwh,
    CAST(ithalKomur  AS FLOAT64) AS imported_coal_mwh,
    CAST(fuelOil     AS FLOAT64) AS fueloil_mwh,
    CAST(jeotermal   AS FLOAT64) AS geothermal_mwh,
    CAST(barajli     AS FLOAT64) AS dam_hydro_mwh,
    CAST(nafta       AS FLOAT64) AS naphtha_mwh,
    CAST(biokutle    AS FLOAT64) AS biomass_mwh,
    CAST(akarsu      AS FLOAT64) AS river_hydro_mwh,
    CAST(gunes       AS FLOAT64) AS solar_mwh,
    CAST(diger       AS FLOAT64) AS other_mwh
FROM {{ source('silver', 'kgup_bulk_by_org') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Aynı cross-Hive-partition-boundary sınıfı (bkz. stg_dam_clearing_by_org.sql).
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'), CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64), CAST(orgId AS INT64), CAST(uevcbId AS INT64)
  ORDER BY s.date DESC
) = 1
