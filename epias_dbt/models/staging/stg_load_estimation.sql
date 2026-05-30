{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour,
    CAST(lep AS FLOAT64) AS forecasted_load_mwh
FROM {{ source('silver', 'load_estimation') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}