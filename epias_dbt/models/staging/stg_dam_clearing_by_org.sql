{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'organization_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- ADR-0007 Faz 1 (plans/07-company-level-market-activity-kgup.md): GÖP şirket
-- bazlı saatlik eşleşme miktarı. Kaynak: epias_gop_company_activity_daily DAG'ı
-- (hourly medallion pipeline'ın DIŞINDA, günde bir kez 11:30 UTC'de çalışır —
-- bkz. dags/epias_gop_company_activity_dag.py'nin docstring'i).
-- matched_bids_mwh/matched_offers_mwh her zaman eşit (uniform-price açık
-- artırma) ama şirket bazında bu, o şirketin o saatteki GÖP pozisyon
-- büyüklüğüdür — çoğu şirket çoğu saat için 0 döner (pasif), bu gürültü değil.
--
-- 2026-08-19 DÜZELTME (bkz. stg_pricing.sql'in aynı notu): s.date UTC bir
-- TIMESTAMP (parse_epias_timestamp ile yazılıyor); Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini (UTC 21-23, önceki
-- takvim günü) yanlış tarihe etiketliyordu.

SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(organizationId   AS INT64)  AS organization_id,
    CAST(organizationName AS STRING) AS organization_name,
    CAST(matchedBids   AS FLOAT64) AS matched_bids_mwh,
    CAST(matchedOffers AS FLOAT64) AS matched_offers_mwh
FROM {{ source('silver', 'dam_clearing_by_org') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Aynı cross-Hive-partition-boundary sınıfı (bkz. stg_dam_clearing.sql) —
-- burada anahtar (date, hour, organization_id).
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul'),
               CAST(organizationId AS INT64)
  ORDER BY s.date DESC
) = 1
