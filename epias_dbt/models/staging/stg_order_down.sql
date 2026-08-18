{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Silver order_down is company-level (one row per organizationId per hour).
-- SUM aggregates all companies to system-level totals per (date, hour).
-- 2026-08-19 DÜZELTME: date UTC bir TIMESTAMP; Asia/Istanbul'a çevirmeden
-- çıplak CAST(... AS DATE) her günün ilk 3 TRT saatini yanlış tarihe
-- etiketliyordu (bkz. stg_pricing.sql'in aynı notu).
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    SUM(CAST(downRegulationZeroCoded AS FLOAT64)) AS down_regulation_zero_mwh,
    SUM(CAST(downRegulationOneCoded AS FLOAT64)) AS down_regulation_one_mwh,
    SUM(CAST(downRegulationTwoCoded AS FLOAT64)) AS down_regulation_two_mwh,
    SUM(CAST(downRegulationDelivered AS FLOAT64)) AS down_regulation_delivered_mwh,
    SUM(CAST(net AS FLOAT64)) AS net_mwh
FROM {{ source('silver', 'order_down') }}

{% if is_incremental() %} WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }}) {% endif %}

GROUP BY 1, 2