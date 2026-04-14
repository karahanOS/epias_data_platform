{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
  )
}}

SELECT
    CONCAT(CAST(date AS STRING), ' ', hour) as join_key,
    date,
    hour,
    total_generation,
    renewable_ratio,
    fossil_ratio,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month
FROM {{ source('epias_gold', 'gold_generation_mix_price_impact') }}
{% if is_incremental() %}
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}