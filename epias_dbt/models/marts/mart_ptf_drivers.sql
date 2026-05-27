{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH pricing AS (
    SELECT date, marketTradePrice AS ptf_try, systemMarginalPrice AS smf_try
    FROM {{ source('silver', 'pricing') }}
),

generation AS (
    SELECT 
        date,
        total AS total_generation_mwh,
        naturalGas AS gas_generation_mwh,
        importedCoal AS coal_generation_mwh,
        -- Yenilenebilir toplamı (Rüzgar, Güneş, Akarsu, Jeotermal)
        (COALESCE(wind, 0) + COALESCE(solar, 0) + COALESCE(river, 0) + COALESCE(geothermal, 0)) AS total_renewable_mwh
    FROM {{ source('silver', 'generation') }}
),

dam_clearing AS (
    -- GÖP talebini (Matched Bids) çekiyoruz
    SELECT date, matchedBidsQuantity AS total_demand_mwh
    FROM {{ source('silver', 'dam_clearing') }}
),

ptf_drivers AS (
    SELECT
        p.date,
        p.ptf_try,
        p.smf_try,
        d.total_demand_mwh,
        g.total_generation_mwh,
        g.total_renewable_mwh,
        g.gas_generation_mwh,
        
        -- Kritik Metrik 1: Yenilenebilir Enerji Penetrasyonu (%)
        -- Bu oran %40-50'leri geçtiğinde PTF tabana (sıfıra) yaklaşır.
        SAFE_DIVIDE(g.total_renewable_mwh, g.total_generation_mwh) * 100 AS renewable_penetration_pct,
        
        -- Kritik Metrik 2: Fosil Yakıt Bağımlılığı (%)
        -- Bu oran arttığında (özellikle rüzgar esmediğinde) PTF tavana gider.
        SAFE_DIVIDE((g.gas_generation_mwh + g.coal_generation_mwh), g.total_generation_mwh) * 100 AS fossil_dependency_pct

    FROM pricing p
    LEFT JOIN generation g ON p.date = g.date
    LEFT JOIN dam_clearing d ON p.date = d.date
)

SELECT * FROM ptf_drivers