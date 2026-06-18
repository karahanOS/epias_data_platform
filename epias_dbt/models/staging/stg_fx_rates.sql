{{ config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(date AS DATE)                  AS date,
    CAST(usdtry AS FLOAT64)             AS usdtry,
    CAST(is_ffilled AS BOOL)            AS is_ffilled
FROM {{ source('silver', 'fx_rates') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
