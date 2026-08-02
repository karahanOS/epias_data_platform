{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- NOTE: bronze_to_silver_res_forecast.py casts every non-date column to DOUBLE,
-- which turns the 'time' string column into NULL for all rows.  We therefore derive
-- the hour from the UTC timestamp in the `date` column, then convert to Turkish local
-- time so the key convention matches every other hourly staging model.
-- (The previous comment claiming UTC hours "align correctly" with stg_load_estimation
-- was wrong — stg_load_estimation uses Turkish hours 1–24 from the `time` field.)
--
-- Source rows are 15-minute readings (4 per hour: :00/:15/:30/:45), NOT
-- category splits — `forecast`/`generation` are power-like readings (~8-10k,
-- matching Turkey's national wind fleet output in MW), not per-quarter energy.
-- Confirmed via raw Silver sample: 4 consecutive quarter-hours average ~8.4k,
-- consistent with stg_generation's independently-sourced hourly wind_generation_mwh
-- (~9-11k). AVG (not SUM) is therefore the correct hourly rollup — SUM was
-- inflating both columns ~4x, which fed mart_renewable_deep.wind_forecast_error
-- and produced a histogram skewed entirely negative by tens of thousands of MWh.
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    AVG(CAST(forecast   AS FLOAT64)) AS forecasted_res_mwh,
    AVG(CAST(generation AS FLOAT64)) AS actual_res_generation_mwh
FROM {{ source('silver', 'res_forecast') }}

{% if is_incremental() %}
WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}

GROUP BY 1, 2