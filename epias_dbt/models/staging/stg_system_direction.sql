{{ config(materialized='incremental', unique_key=['date', 'hour'], partition_by={"field": "date", "data_type": "date"}) }}
-- date/hour are converted to Istanbul local time (UTC+3) to match stg_pricing,
-- which also uses Istanbul local time so that JOINs on (date, hour) align correctly.
-- The raw Silver `date` column is a UTC TIMESTAMP written by parse_epias_timestamp().
SELECT
    DATE(date, 'Asia/Istanbul')                                      AS date,
    EXTRACT(HOUR FROM date AT TIME ZONE 'Asia/Istanbul')             AS hour,
    -- The EPIAS BPM API returns Turkish labels; normalize to English so downstream
    -- marts can use consistent constants (ENERGY_DEFICIT / ENERGY_SURPLUS / IN_BALANCE).
    -- English values may appear if the computed imbalance path wrote to the same table.
    CASE CAST(systemDirection AS STRING)
        WHEN 'Enerji Açığı'  THEN 'ENERGY_DEFICIT'
        WHEN 'Enerji Fazlası' THEN 'ENERGY_SURPLUS'
        WHEN 'Dengede'        THEN 'IN_BALANCE'
        ELSE CAST(systemDirection AS STRING)
    END AS system_direction
FROM {{ source('silver', 'system_direction') }}
{% if is_incremental() %} WHERE DATE(date, 'Asia/Istanbul') >= (SELECT MAX(date) FROM {{ this }}) {% endif %}