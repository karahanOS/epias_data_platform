-- depends_on: {{ ref('stg_generation_mix') }}
select
    extract(year from date)         as year,
    extract(month from date)        as month,
    round(avg(renewable_ratio), 2)  as avg_renewable_ratio,
    round(avg(fossil_ratio), 2)     as avg_fossil_ratio,
    round(avg(ptf), 2)              as avg_ptf,
    round(corr(renewable_ratio, ptf), 4) as renewable_price_correlation
from {{ ref('stg_generation_mix') }}
group by 1, 2
order by 1, 2