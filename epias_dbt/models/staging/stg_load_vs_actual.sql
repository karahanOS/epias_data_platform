{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

-- 💡 "00:00" metninin ilk 2 karakterini (00) alıp INT64'e çeviriyoruz!
SELECT 
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour, 
    CAST(t.consumption AS FLOAT64) AS actual_consumption
FROM {{ source('silver', 'consumption') }} AS t

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}