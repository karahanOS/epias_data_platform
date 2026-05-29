{{ config(materialized='incremental', unique_key=['date', 'hour'], partition_by={"field": "date", "data_type": "date"}) }}
WITH source AS (SELECT * FROM {{ source('silver', 'system_direction') }})
SELECT CAST(date AS DATE) AS date, CAST(hour AS INT64) AS hour, CAST(systemDirection AS STRING) AS system_direction FROM source
{% if is_incremental() %} WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }}) {% endif %}