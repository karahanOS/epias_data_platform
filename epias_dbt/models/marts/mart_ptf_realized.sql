{{ config(materialized='table', partition_by={"field": "date", "data_type": "date"}) }}

-- mart_ptf_realized: single "best known real price" series — coalesces the
-- final, appeal-confirmed PTF (stg_pricing) with the interim, pre-appeal
-- K.PTF (stg_interim_mcp) that becomes available roughly a day earlier.
-- price_status tells consumers which one a given row came from; prefer
-- final whenever both exist (it's authoritative, even though empirically
-- interim always matches it exactly — see stg_interim_mcp.sql's header).
--
-- Both ptf_inference.py (extract_forward_features' anti-join) and
-- dashboard.py's forward-forecast panel read this one mart instead of each
-- re-implementing the final-vs-interim coalesce logic separately.
WITH final AS (
    SELECT date, hour, ptf_try
    FROM {{ ref('stg_pricing') }}
),
interim AS (
    SELECT date, hour, interim_ptf_try
    FROM {{ ref('stg_interim_mcp') }}
)
SELECT
    COALESCE(f.date, i.date) AS date,
    COALESCE(f.hour, i.hour) AS hour,
    TIMESTAMP_ADD(
        TIMESTAMP(COALESCE(f.date, i.date), 'Asia/Istanbul'),
        INTERVAL COALESCE(f.hour, i.hour) HOUR
    ) AS datetime,
    COALESCE(f.ptf_try, i.interim_ptf_try) AS ptf_try,
    CASE WHEN f.ptf_try IS NOT NULL THEN 'final' ELSE 'interim' END AS price_status
FROM final f
FULL OUTER JOIN interim i USING (date, hour)
