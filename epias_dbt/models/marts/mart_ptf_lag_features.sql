with base as (
    select
        date,
        hour,
        ptf,
        hour_of_day,
        day_of_week,
        is_weekend,
        month_of_year,
        season,
        temperature,
        wind_speed,
        solar_radiation,
        humidity,
        wind_generation,
        solar_generation,
        hydro_generation,
        gas_generation,
        total_generation,
        actual_consumption,
        forecast_consumption,
        EXTRACT(YEAR FROM date)  as year,
        EXTRACT(MONTH FROM date) as month
    from {{ source('epias_gold', 'gold_ml_features') }}
),

with_lags as (
    select
        *,
        -- 1 saat önceki PTF
        lag(ptf, 1) over (order by date) as ptf_lag_1h,
        -- 24 saat önceki PTF (dün aynı saat)
        lag(ptf, 24) over (order by date) as ptf_lag_24h,
        -- 168 saat önceki PTF (geçen hafta aynı gün/saat)
        lag(ptf, 168) over (order by date) as ptf_lag_168h,
        -- Son 24 saatin ortalaması
        avg(ptf) over (
            order by date
            rows between 24 preceding and 1 preceding
        ) as ptf_rolling_avg_24h,
        -- Son 168 saatin ortalaması
        avg(ptf) over (
            order by date
            rows between 168 preceding and 1 preceding
        ) as ptf_rolling_avg_168h,
        -- Son 24 saatin maks/min
        max(ptf) over (
            order by date
            rows between 24 preceding and 1 preceding
        ) as ptf_rolling_max_24h,
        min(ptf) over (
            order by date
            rows between 24 preceding and 1 preceding
        ) as ptf_rolling_min_24h
    from base
)

select * from with_lags
-- İlk 168 saati at — lag değerleri null olur
where ptf_lag_168h is not null