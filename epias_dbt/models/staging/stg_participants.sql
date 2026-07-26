{{ config(
    materialized='table'
) }}

WITH raw_participants AS (
    SELECT * FROM {{ source('silver', 'participants') }}
)

SELECT
    CAST(id AS INT64) AS organization_id,
    CAST(orgName AS STRING) AS organization_name,
    CAST(orgCode AS STRING) AS organization_code,
    CAST(eicCode AS STRING) AS eic_code,
    CAST(legalStatus AS STRING) AS legal_status
FROM raw_participants
-- BigQuery rejects PARTITION BY on a FLOAT64 expression directly ("Partitioning
-- by expressions of type FLOAT64 is not allowed") — id is FLOAT64 in Silver
-- (project-wide FLOAT64 convention), so cast to INT64 before partitioning.
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(id AS INT64) ORDER BY id) = 1