{{ config(materialized='incremental', unique_key=['date', 'hour'], incremental_strategy='merge', partition_by={"field": "date", "data_type": "date"}) }}

WITH deduped AS (
    SELECT
        CAST(date AS DATE) AS date,
        CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) AS hour,
        CAST(toplam AS FLOAT64) AS total_unlicensed_mwh,
        CAST(ruzgar AS FLOAT64) AS wind_unlicensed_mwh,
        CAST(gunes AS FLOAT64) AS solar_unlicensed_mwh,
        CAST(biyokutle AS FLOAT64) AS biomass_unlicensed_mwh,
        CAST(biyogaz AS FLOAT64) AS biogas_unlicensed_mwh,
        CAST(kanalTipi AS FLOAT64) AS canal_unlicensed_mwh,
        CAST(diger AS FLOAT64) AS other_unlicensed_mwh,
        ROW_NUMBER() OVER(PARTITION BY CAST(date AS DATE), CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64) ORDER BY _record_hash DESC) as rn
    FROM {{ source('silver', 'unlicensed') }}
)
SELECT * EXCEPT(rn) FROM deduped WHERE rn = 1
{% if is_incremental() %} AND date >= (SELECT MAX(date) FROM {{ this }}) {% endif %}