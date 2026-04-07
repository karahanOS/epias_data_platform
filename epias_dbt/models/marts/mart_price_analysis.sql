{{ config(materialized='view') }} -- Canlı veri için view yapıyoruz

SELECT
    date,
    hour,
    ptf,          
    smf,           
    price_spread, 
    system_direction,
    season
FROM {{ ref('stg_price_spread') }}