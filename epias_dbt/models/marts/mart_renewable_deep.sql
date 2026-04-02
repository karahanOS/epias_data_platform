-- depends_on: {{ ref('stg_renewable_deep') }}
select
    extract(year from date)         as year,
    extract(month from date)        as month,
    season,
    time_of_day,
    round(avg(wind_ratio), 2)       as avg_wind_ratio,
    round(avg(sun_ratio), 2)        as avg_sun_ratio,
    round(avg(hydro_ratio), 2)      as avg_hydro_ratio,
    round(avg(gas_ratio), 2)        as avg_gas_ratio,
    round(avg(coal_ratio), 2)       as avg_coal_ratio,
    round(avg(combined_renewable_ratio), 2) as avg_combined_renewable,
    round(avg(ptf), 2)              as avg_ptf,
    round(corr(wind_ratio, ptf), 4) as wind_price_corr,
    round(corr(sun_ratio, ptf), 4)  as sun_price_corr,
    round(corr(hydro_ratio, ptf), 4) as hydro_price_corr
from {{ ref('stg_renewable_deep') }}
group by 1, 2, 3, 4
order by 1, 2