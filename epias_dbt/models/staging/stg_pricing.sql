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
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    CAST(price AS FLOAT64) AS ptf_try,
    CAST(priceUsd AS FLOAT64) AS ptf_usd,
    CAST(priceEur AS FLOAT64) AS ptf_eur
FROM {{ source('silver', 'pricing') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}