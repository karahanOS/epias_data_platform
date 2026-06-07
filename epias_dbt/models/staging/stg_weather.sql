{{ config(materialized='incremental', unique_key=['date', 'hour', 'city_name'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

-- Silver weather.date is a UTC TIMESTAMP.  Convert to Turkish local date/hour so
-- hourly weather rows align with all other staging models (stg_pricing, stg_imbalance,
-- stg_generation, etc.) when JOINed on (date, hour).
SELECT
    DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul')                          AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP) AT TIME ZONE 'Asia/Istanbul') AS hour,
    CAST(city AS STRING) AS city_name,
    CAST(temperature_2m AS FLOAT64) AS temperature_celsius,
    CAST(wind_speed_10m AS FLOAT64) AS wind_speed_kmh,
    CAST(shortwave_radiation AS FLOAT64) AS shortwave_radiation,
    CAST(relative_humidity_2m AS FLOAT64) AS relative_humidity
FROM {{ source('silver', 'weather') }}

{% if is_incremental() %}
  WHERE DATE(CAST(date AS TIMESTAMP), 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }})
{% endif %}