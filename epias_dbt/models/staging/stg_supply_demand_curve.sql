{{ config(
    materialized='incremental',
    unique_key=['date', 'bid_offer_price_try'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'supply_demand') }}
)

SELECT
    CAST(date AS DATE) AS date,
    CAST(price AS FLOAT64) AS bid_offer_price_try,
    CAST(supply AS FLOAT64) AS cumulative_supply_mwh,
    CAST(demand AS FLOAT64) AS cumulative_demand_mwh
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}