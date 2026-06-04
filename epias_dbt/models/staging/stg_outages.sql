{{ config(materialized='incremental', unique_key=['id'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

SELECT
    CAST(id AS STRING) AS id,
    CAST(CAST(caseStartDate AS TIMESTAMP) AS DATE) AS date, -- Önce TIMESTAMP, sonra DATE
    CAST(caseStartDate AS TIMESTAMP) AS start_time,
    CAST(caseEndDate AS TIMESTAMP) AS end_time,
    CAST(orgName AS STRING) AS company_name,
    CAST(powerPlantName AS STRING) AS plant_name,
    CAST(operatorPower AS FLOAT64) AS installed_capacity_mwh,
    CAST(capacityAtCaseTime AS FLOAT64) AS outage_capacity_mwh,
    CAST(reason AS STRING) AS outage_reason
FROM {{ source('silver', 'outages') }}

{% if is_incremental() %} 
  WHERE CAST(CAST(caseStartDate AS TIMESTAMP) AS DATE) >= (SELECT MAX(date) FROM {{ this }}) 
{% endif %}