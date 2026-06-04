{{ config(materialized='incremental', unique_key=['date', 'hour', 'city_name'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(date AS DATE) AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    CAST(city AS STRING) AS city_name,
    CAST(temperature_2m AS FLOAT64) AS temperature_celsius,
    CAST(wind_speed_10m AS FLOAT64) AS wind_speed_kmh,
    CAST(shortwave_radiation AS FLOAT64) AS shortwave_radiation,
    CAST(relative_humidity_2m AS FLOAT64) AS relative_humidity
FROM {{ source('silver', 'weather') }}

{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}