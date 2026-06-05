{{ config(
    materialized='incremental',
    unique_key=['date', 'hour'],
    incremental_strategy='merge',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- DPP = Declaratory Production Plan (Beyan Edilen Günlük Üretim Planı / BGÜP)
-- Bu tablo gün öncesinde şirketlerin TEIAŞ'a bildirdiği üretim planıdır.
-- KGÜP (kesinleşmiş) için stg_sbfgp.sql kullanılmalıdır.
SELECT
    CAST(date AS DATE)                                    AS date,
    CAST(SUBSTR(CAST(time AS STRING), 1, 2) AS INT64)    AS hour,
    CAST(toplam      AS FLOAT64) AS total_planned_mwh,
    CAST(dogalgaz    AS FLOAT64) AS natural_gas_mwh,
    CAST(ruzgar      AS FLOAT64) AS wind_mwh,
    CAST(linyit      AS FLOAT64) AS lignite_mwh,
    CAST(tasKomur    AS FLOAT64) AS hard_coal_mwh,
    CAST(ithalKomur  AS FLOAT64) AS imported_coal_mwh,
    CAST(fuelOil     AS FLOAT64) AS fueloil_mwh,
    CAST(jeotermal   AS FLOAT64) AS geothermal_mwh,
    CAST(barajli     AS FLOAT64) AS dam_hydro_mwh,
    CAST(nafta       AS FLOAT64) AS naphtha_mwh,
    CAST(biokutle    AS FLOAT64) AS biomass_mwh,
    CAST(akarsu      AS FLOAT64) AS river_hydro_mwh,
    CAST(gunes       AS FLOAT64) AS solar_mwh,
    CAST(diger       AS FLOAT64) AS other_mwh
FROM {{ source('silver', 'dpp') }}

{% if is_incremental() %}
  WHERE CAST(date AS DATE) >= (SELECT MAX(date) FROM {{ this }})
{% endif %}
