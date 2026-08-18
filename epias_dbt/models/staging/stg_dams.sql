{{ config(
    materialized='incremental',
    unique_key=['date', 'dam_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- 2026-08-19 DÜZELTME: s.date UTC bir TIMESTAMP; Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE), "21:00 UTC'nin önceki günü" gibi bir zaman
-- damgasını (= 00:00 TRT, o günün BAŞLANGICI) yanlışlıkla bir önceki takvim
-- gününe etiketliyordu — 3 saatlik nadir bir sınır durumu değil, dams'ın her
-- günkü "as of" zaman damgası tutarlı biçimde bu aralıkta olduğu için
-- SİSTEMATİK bir 1-gün kayması (bkz. stg_pricing.sql'in aynı sınıf notu).
SELECT
    DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') AS date,
    CAST(damId AS INT64) AS dam_id,
    CAST(basinName AS STRING) AS basin_name,
    CAST(damName AS STRING) AS dam_name,
    CAST(activeVolume AS FLOAT64) AS active_volume
FROM {{ source('silver', 'dams') }} AS s

{% if is_incremental() %}
  WHERE DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Dams reports "as of" a lagged UTC timestamp (e.g. 21:00 UTC of the previous
-- day) that can land in BOTH that day's Hive partition and the next day's own
-- fetch, duplicating a (date, dam_id) row with identical values. Self-heal here.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY DATE(CAST(s.date AS TIMESTAMP), 'Asia/Istanbul'), CAST(damId AS INT64)
  ORDER BY s.date DESC
) = 1