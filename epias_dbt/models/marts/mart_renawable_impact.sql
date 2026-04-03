{{ config(materialized='table') }}

WITH gen_ptf AS (
    SELECT 
        g.*,
        p.mcp_usd -- ptf yerine mcp_usd kullanıyoruz
    FROM {{ ref('stg_generation_mix') }} g
    JOIN {{ ref('stg_price_spread') }} p ON g.join_key = p.join_key
)

SELECT
    date,
    hour,
    total_generation,
    renewable_ratio,
    fossil_ratio,
    mcp_usd, -- yeni kolon adı
    year,
    month
FROM gen_ptf