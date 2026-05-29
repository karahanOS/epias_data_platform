{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'organization_id'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'order_up') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(hour AS INT64) AS hour,
    CAST(organizationId AS INT64) AS organization_id,
    CAST(upRegulationDelivered AS FLOAT64) AS up_regulation_delivered_mwh,
    CAST(net AS FLOAT64) AS net_price_try
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}