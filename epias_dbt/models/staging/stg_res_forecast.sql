{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

WITH raw_res AS (
    SELECT * FROM {{ source('silver', 'res_forecast') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(total AS FLOAT64) AS forecasted_total_res_mwh,
    CAST(wind AS FLOAT64) AS forecasted_wind_mwh,
    CAST(solar AS FLOAT64) AS forecasted_solar_mwh
FROM raw_res

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}