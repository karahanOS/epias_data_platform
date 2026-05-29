{{ config(
    materialized='incremental',
    unique_key=['date', 'city'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'weather') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(city AS STRING) AS city,
    CAST(temperature_2m AS FLOAT64) AS temperature_2m,
    CAST(wind_speed_10m AS FLOAT64) AS windspeed_10m,
    CAST(shortwave_radiation AS FLOAT64) AS shortwave_radiation,
    CAST(cloudcover AS FLOAT64) AS cloudcover
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}