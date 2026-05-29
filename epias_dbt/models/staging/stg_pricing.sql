{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

WITH raw_pricing AS (
    SELECT * FROM {{ source('silver', 'pricing') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(marketTradePrice AS FLOAT64) AS ptf_try,
    CAST(priceUsd AS FLOAT64) AS ptf_usd,
    CAST(priceEur AS FLOAT64) AS ptf_eur
FROM raw_pricing

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}