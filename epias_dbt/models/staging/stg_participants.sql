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
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1