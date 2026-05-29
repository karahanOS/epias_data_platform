{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH hourly_system AS (
    SELECT
        smf.date,
        smf.smf_try,
        p.ptf_try,
        (smf.smf_try - p.ptf_try) AS smf_ptf_spread_try,
        sd.system_direction,
        imb.net_imbalance_mwh
    FROM {{ ref('stg_smf') }} smf
    LEFT JOIN {{ ref('stg_pricing') }} p ON smf.date = p.date
    LEFT JOIN {{ ref('stg_system_direction') }} sd ON smf.date = sd.date
    LEFT JOIN {{ ref('stg_imbalance') }} imb ON smf.date = imb.date
),

company_yal AS (
    SELECT 
        date, 
        organization_id, 
        SUM(up_regulation_delivered_mwh) AS total_yal_mwh,
        AVG(net_price_try) AS avg_yal_price_try
    FROM {{ ref('stg_order_up') }}
    GROUP BY 1, 2
),

company_yat AS (
    SELECT 
        date, 
        organization_id, 
        SUM(down_regulation_delivered_mwh) AS total_yat_mwh,
        AVG(net_price_try) AS avg_yat_price_try
    FROM {{ ref('stg_order_down') }}
    GROUP BY 1, 2
),

company_actions AS (
    SELECT
        COALESCE(u.date, d.date) AS date,
        COALESCE(u.organization_id, d.organization_id) AS organization_id,
        COALESCE(u.total_yal_mwh, 0) AS total_yal_mwh,
        u.avg_yal_price_try,
        COALESCE(d.total_yat_mwh, 0) AS total_yat_mwh,
        d.avg_yat_price_try
    FROM company_yal u
    FULL OUTER JOIN company_yat d
        ON u.date = d.date AND u.organization_id = d.organization_id
),

participants AS (
    SELECT organization_id, organization_name
    FROM {{ ref('stg_participants') }}
)

SELECT
    hs.date,
    hs.system_direction,
    hs.net_imbalance_mwh,
    hs.smf_try,
    hs.ptf_try,
    hs.smf_ptf_spread_try,
    co.organization_id,
    COALESCE(part.organization_name, 'Bilinmeyen Şirket') AS company_name,
    co.total_yal_mwh,
    co.avg_yal_price_try,
    co.total_yat_mwh,
    co.avg_yat_price_try
FROM hourly_system hs
JOIN company_actions co ON hs.date = co.date
LEFT JOIN participants part ON co.organization_id = part.organization_id