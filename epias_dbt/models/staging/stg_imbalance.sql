{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'imbalance') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(imbalanceQuantity AS FLOAT64) AS net_imbalance_mwh
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}