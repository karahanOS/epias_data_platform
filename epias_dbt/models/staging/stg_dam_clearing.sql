{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- 2026-08-19 DÜZELTME (bkz. stg_dam_clearing_by_org.sql'in aynı notu, ve
-- stg_pricing.sql'in orijinal deseni): s.date UTC bir TIMESTAMP; Asia/Istanbul'a
-- çevirmeden çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini yanlış
-- tarihe etiketliyordu. hour de aynı desene uyumlu olsun diye timestamp'ten
-- yeniden türetildi (ham hour string'i zaten aynı değeri veriyordu, kardeş
-- dosyada doğrulandı — bu sadece bir değer değişikliği değil, tutarlılık).

SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(matchedBids AS FLOAT64) AS matched_bids_mwh,
    CAST(matchedOffers AS FLOAT64) AS matched_offers_mwh
FROM {{ source('silver', 'dam_clearing') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- See stg_pricing.sql: adjacent Hive day-partitions can both carry the same
-- boundary slice, duplicating a (date,hour) row. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'),
               EXTRACT(HOUR FROM CAST(s.date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul')
  ORDER BY s.date DESC
) = 1