{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- NOTE: bronze_to_silver_res_forecast.py casts every non-date column to DOUBLE,
-- which turns the 'time' string column into NULL for all rows.  We therefore derive
-- the hour from the UTC timestamp in the `date` column, then convert to Turkish local
-- time so the key convention matches every other hourly staging model.
-- (The previous comment claiming UTC hours "align correctly" with stg_load_estimation
-- was wrong — stg_load_estimation uses Turkish hours 1–24 from the `time` field.)
-- Kaynak veri kategori bazlı (rüzgar/güneş/jeotermal vb.) — saatte ~6 satır.
-- SUM + GROUP BY ile saatlik aggregate alıyoruz (unique_key='date,hour' ile tutarlı).
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    SUM(CAST(forecast   AS FLOAT64)) AS forecasted_res_mwh,
    SUM(CAST(generation AS FLOAT64)) AS actual_res_generation_mwh
FROM {{ source('silver', 'res_forecast') }}

{% if is_incremental() %}
WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}

GROUP BY 1, 2