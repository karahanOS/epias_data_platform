{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(date AS DATE) AS date,
    CAST(SUBSTR(CAST(hour AS STRING), 1, 2) AS INT64) AS hour,
    CAST(downRegulationZeroCoded AS FLOAT64) AS down_regulation_zero_mwh,
    CAST(downRegulationOneCoded AS FLOAT64) AS down_regulation_one_mwh,
    CAST(downRegulationTwoCoded AS FLOAT64) AS down_regulation_two_mwh,
    CAST(downRegulationDelivered AS FLOAT64) AS down_regulation_delivered_mwh,
    CAST(net AS FLOAT64) AS net_mwh
FROM {{ source('silver', 'order_down') }}

{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}