{{ config(materialized='view') }}

SELECT
    date,
    hour,
    -- EPİAŞ'tan gelen orijinal Dolar kolonlarını global terminolojiyle isimlendiriyoruz
    mcpUsd AS mcp_usd,          -- Market Clearing Price (Eski PTF)
    smpUsd AS smp_usd,          -- System Marginal Price (Eski SMF)
    
    -- Fiyat makasını (Spread) Dolar üzerinden hesaplıyoruz
    (smpUsd - mcpUsd) AS price_spread_usd,
    
    system_direction
FROM {{ source('epias_gold', 'gold_price_spread_analysis') }}
-- Not: Eğer ham verideki kolon adları 'ptf_usd' şeklindeyse, 'mcpUsd' yerine onu yazmalısın.