select
    extract(year from date)         as year,
    extract(month from date)        as month,
    season,
    system_direction,
    count(*)                        as hour_count,
    round(avg(ptf), 2)              as avg_ptf,
    round(avg(smf), 2)              as avg_smf,
    round(avg(price_spread), 2)     as avg_spread,
    round(max(price_spread), 2)     as max_spread,
    round(min(price_spread), 2)     as min_spread
from {{ ref('stg_price_spread') }}
group by 1, 2, season, system_direction
order by 1, 2