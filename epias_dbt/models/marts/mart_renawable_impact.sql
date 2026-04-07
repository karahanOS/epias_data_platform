{{ config(materialized='table') }}

WITH gen_mix AS (
    SELECT * FROM {{ ref('stg_generation_mix') }}
)

SELECT
    g.date,
    g.hour,
    g.total_generation,
    g.renewable_ratio,
    g.fossil_ratio,
    p.ptf, -- mcp_usd yerine ptf kullanıyoruz
    g.year,
    g.month
FROM gen_mix g
-- join_key yerine doğrudan date ve hour üzerinden bağlıyoruz
LEFT JOIN {{ ref('stg_price_spread') }} p 
    ON g.date = p.date 
    AND g.hour = p.hour