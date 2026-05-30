{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'organization_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'order_down') }}
)

SELECT
    CAST(date AS DATE) AS date,
    EXTRACT(HOUR FROM CAST(date AS TIMESTAMP)) AS hour,
    CAST(organization_id AS STRING) AS organization_id, 
    CAST(organization_short_name AS STRING) AS organization_short_name
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}