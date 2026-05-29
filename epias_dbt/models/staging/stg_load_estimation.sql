{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={
      "field": "date",
      "data_type": "date"
    }
) }}

WITH raw_load AS (
    SELECT * FROM {{ source('silver', 'load_estimation') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(lep AS FLOAT64) AS forecasted_load_mwh
FROM raw_load

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}