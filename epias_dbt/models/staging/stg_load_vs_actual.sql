 -- 💡 Veri tekilliğini bu iki anahtar sağlar
{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'], 
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
  )
}}

SELECT * FROM {{ source('epias', 'silver_consumption') }}
{% if is_incremental() %}
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}