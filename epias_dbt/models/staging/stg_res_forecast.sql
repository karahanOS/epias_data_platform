{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- NOTE: bronze_to_silver_res_forecast.py casts every non-date column to DOUBLE,
-- which turns the 'time' string column into NULL for all rows.
-- Using EXTRACT(HOUR FROM date) matches stg_pricing.sql's approach and produces
-- consistent UTC hours that align correctly with stg_load_estimation.
SELECT
    CAST(date AS DATE) AS date,
    EXTRACT(HOUR FROM date) AS hour,
    CAST(forecast AS FLOAT64) AS forecasted_res_mwh,
    CAST(generation AS FLOAT64) AS actual_res_generation_mwh
FROM {{ source('silver', 'res_forecast') }}

{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}