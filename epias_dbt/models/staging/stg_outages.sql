{{ config(
    materialized='incremental',
    unique_key=['id'], -- Arıza ID'si tekildir
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'outages') }}
)

SELECT
    CAST(id AS STRING) AS outage_id,
    CAST(date AS DATE) AS date,
    CAST(date AS TIMESTAMP) AS date_timestamp,
    CAST(organizationId AS INT64) AS organization_id,
    CAST(uevcbId AS INT64) AS uevcb_id,
    CAST(outageCapacity AS FLOAT64) AS outage_capacity_mwh,
    CAST(reason AS STRING) AS outage_reason
FROM source

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}