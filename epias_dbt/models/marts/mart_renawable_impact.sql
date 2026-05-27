{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH generation AS (
    SELECT 
        date,
        total AS total_licensed_generation_mwh,
        (COALESCE(wind, 0) + COALESCE(solar, 0) + COALESCE(river, 0) + COALESCE(geothermal, 0) + COALESCE(biomass, 0)) AS licensed_renewable_mwh
    FROM {{ source('silver', 'generation') }}
),

unlicensed AS (
    SELECT
        TIMESTAMP_TRUNC(date, HOUR) AS date,
        SUM(total) AS total_unlicensed_mwh
    FROM {{ source('silver', 'unlicensed') }}
    GROUP BY 1
),

dam_demand AS (
    -- GÖP Eşleşen Miktarı piyasanın toplam talebi olarak kabul ediyoruz
    SELECT date, matchedBidsQuantity AS total_demand_mwh
    FROM {{ source('silver', 'dam_clearing') }}
),

pricing AS (
    SELECT date, marketTradePrice AS ptf_try FROM {{ source('silver', 'pricing') }}
),

residual_load_analysis AS (
    SELECT
        g.date,
        p.ptf_try,
        d.total_demand_mwh,
        g.licensed_renewable_mwh,
        COALESCE(u.total_unlicensed_mwh, 0) AS total_unlicensed_mwh,
        
        -- Toplam Yenilenebilir (Lisanslı + Lisanssız)
        (g.licensed_renewable_mwh + COALESCE(u.total_unlicensed_mwh, 0)) AS total_green_energy_mwh,
        
        -- KRİTİK METRİK: Residual Load (Kalan Yük)
        -- Fosil yakıtlı santrallerin karşılamak zorunda olduğu asıl net talep.
        (d.total_demand_mwh - (g.licensed_renewable_mwh + COALESCE(u.total_unlicensed_mwh, 0))) AS residual_load_mwh,
        
        -- Yenilenebilirin Talebi Karşılama Oranı (Merit Order Effect Gücü)
        SAFE_DIVIDE((g.licensed_renewable_mwh + COALESCE(u.total_unlicensed_mwh, 0)), d.total_demand_mwh) * 100 AS green_coverage_pct

    FROM generation g
    LEFT JOIN unlicensed u ON g.date = u.date
    LEFT JOIN dam_demand d ON g.date = d.date
    LEFT JOIN pricing p ON g.date = p.date
)

SELECT * FROM residual_load_analysis