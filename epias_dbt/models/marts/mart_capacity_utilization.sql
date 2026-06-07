{{
  config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
  )
}}

-- mart_capacity_utilization: AİÇ (uzlaştırma esaslı) / Gerçek Üretim oranları
-- kaynak tipi bazında saatlik kapasite kullanım metriği.
-- Değer > 1.0 → AİÇ > gerçek üretim (alım-satım güvencesi aşıldı)
-- Değer < 1.0 → Gerçek üretim > AİÇ (over-delivery)
-- Birleşik veri; bir tarafta kayıp olan saatler null oran üretir (LEFT JOIN).

WITH aic AS (
    SELECT
        date,
        hour,
        total_aic_mwh,
        gas_aic_mwh,
        wind_aic_mwh,
        solar_aic_mwh,
        lignite_aic_mwh,
        imported_coal_aic_mwh,
        hard_coal_aic_mwh,
        fueloil_aic_mwh,
        geothermal_aic_mwh,
        dam_aic_mwh,
        river_aic_mwh,
        naphtha_aic_mwh,
        biomass_aic_mwh
    FROM {{ ref('stg_aic') }}
),

gen AS (
    SELECT
        date,
        hour,
        total_generation_mwh,
        gas_generation_mwh,
        wind_generation_mwh,
        solar_generation_mwh,
        lignite_generation_mwh,
        imported_coal_generation_mwh,
        fueloil_generation_mwh,
        geothermal_generation_mwh,
        dam_generation_mwh,
        river_generation_mwh,
        naphtha_generation_mwh,
        biomass_generation_mwh,
        lng_generation_mwh,
        asphaltite_coal_generation_mwh,
        black_coal_generation_mwh,
        waste_heat_generation_mwh,
        import_export_mwh
    FROM {{ ref('stg_generation') }}
)

SELECT
    COALESCE(a.date, g.date)   AS date,
    COALESCE(a.hour, g.hour)   AS hour,

    -- Aggregate capacity metrics
    a.total_aic_mwh,
    g.total_generation_mwh,
    SAFE_DIVIDE(g.total_generation_mwh, a.total_aic_mwh) AS total_utilization_ratio,

    -- Per-fuel AİÇ vs actual
    a.gas_aic_mwh,        g.gas_generation_mwh,
    SAFE_DIVIDE(g.gas_generation_mwh,        a.gas_aic_mwh)        AS gas_utilization_ratio,

    a.wind_aic_mwh,       g.wind_generation_mwh,
    SAFE_DIVIDE(g.wind_generation_mwh,       a.wind_aic_mwh)       AS wind_utilization_ratio,

    a.solar_aic_mwh,      g.solar_generation_mwh,
    SAFE_DIVIDE(g.solar_generation_mwh,      a.solar_aic_mwh)      AS solar_utilization_ratio,

    a.lignite_aic_mwh,    g.lignite_generation_mwh,
    SAFE_DIVIDE(g.lignite_generation_mwh,    a.lignite_aic_mwh)    AS lignite_utilization_ratio,

    a.imported_coal_aic_mwh, g.imported_coal_generation_mwh,
    SAFE_DIVIDE(g.imported_coal_generation_mwh, a.imported_coal_aic_mwh) AS imported_coal_utilization_ratio,

    a.geothermal_aic_mwh, g.geothermal_generation_mwh,
    SAFE_DIVIDE(g.geothermal_generation_mwh, a.geothermal_aic_mwh) AS geothermal_utilization_ratio,

    a.dam_aic_mwh,        g.dam_generation_mwh,
    SAFE_DIVIDE(g.dam_generation_mwh,        a.dam_aic_mwh)        AS dam_hydro_utilization_ratio,

    a.river_aic_mwh,      g.river_generation_mwh,
    SAFE_DIVIDE(g.river_generation_mwh,      a.river_aic_mwh)      AS run_of_river_utilization_ratio,

    a.biomass_aic_mwh,    g.biomass_generation_mwh,
    SAFE_DIVIDE(g.biomass_generation_mwh,    a.biomass_aic_mwh)    AS biomass_utilization_ratio,

    -- Generation-only fuels (no AİÇ counterpart in Silver)
    g.lng_generation_mwh,
    g.asphaltite_coal_generation_mwh,
    g.black_coal_generation_mwh,
    g.waste_heat_generation_mwh,
    g.import_export_mwh

FROM aic a
FULL OUTER JOIN gen g
    ON a.date = g.date AND a.hour = g.hour

{% if is_incremental() %}
WHERE COALESCE(a.date, g.date) >= (SELECT DATE_SUB(MAX(date), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}
