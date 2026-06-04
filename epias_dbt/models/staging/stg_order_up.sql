{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(upRegulationZeroCoded AS FLOAT64) AS up_regulation_zero_mwh,
    CAST(upRegulationOneCoded AS FLOAT64) AS up_regulation_one_mwh,
    CAST(upRegulationTwoCoded AS FLOAT64) AS up_regulation_two_mwh,
    CAST(upRegulationDelivered AS FLOAT64) AS up_regulation_delivered_mwh,
    CAST(net AS FLOAT64) AS net_mwh
FROM {{ source('silver', 'order_up') }}

{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}