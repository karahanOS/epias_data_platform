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
FROM {{ source('silver', 'outages') }} AS s

{% if is_incremental() %}
  WHERE CAST(CAST(s.caseStartDate AS TIMESTAMP) AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
-- Same cross-partition-boundary duplication as stg_idm_transactions/stg_pricing:
-- the same outage `id` can appear in two adjacent Hive day-partitions with
-- identical content. `id` is the true global grain (unique_key=['id']).
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly -- id is
-- FLOAT64 per the project's cast convention, cast to INT64 first.
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(s.id AS INT64) ORDER BY CAST(s.caseStartDate AS TIMESTAMP) DESC) = 1