{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- mart_smf_realized: single "best known real SMF" series. Unlike PTF, SMF has
-- no interim/final split (stg_smf is EPİAŞ's single settlement layer — see
-- stg_smf.sql), so this is a straight passthrough rather than a coalesce.
-- Exists purely so smf_inference.py's forward-prediction anti-join and
-- mart_smf_forecast_outlook.sql have a mart to target, mirroring how
-- ptf_inference.py/mart_ptf_forecast_outlook.sql use mart_ptf_realized instead
-- of each re-deriving "is this hour settled yet" independently.

SELECT
    date,
    hour,
    TIMESTAMP_ADD(TIMESTAMP(date, 'Asia/Istanbul'), INTERVAL CAST(hour AS INT64) HOUR) AS datetime,
    smf_try
FROM {{ ref('stg_smf') }}
