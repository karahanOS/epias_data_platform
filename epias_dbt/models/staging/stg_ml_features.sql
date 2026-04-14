-- models/staging/stg_ml_features.sql
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

SELECT 
    * FROM {{ source('epias_gold', 'gold_ml_features') }}

{% if is_incremental() %}
  -- Sadece yeni gelen veya güncellenen veriyi al
  WHERE date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}