select
    extract(year from date)                     as year,
    extract(month from date)                    as month,
    extract(hour from date)                     as hour_of_day,
    deviation_direction,
    count(*)                                    as hour_count,
    round(avg(abs(deviation)), 2)               as avg_abs_deviation_mwh,
    round(avg(abs(deviation_pct)), 2)           as avg_abs_deviation_pct,
    round(max(abs(deviation)), 2)               as max_deviation_mwh,
    round(avg(forecast_consumption), 2)         as avg_forecast,
    round(avg(actual_consumption), 2)           as avg_actual
from {{ ref('stg_load_vs_actual') }}
group by 1, 2, 3, 4
order by 1, 2, 3