{{ config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"}
) }}

-- PTF Tavan / Minimum Analizi
-- Her saati için PTF'nin nerede olduğunu ve o saatteki piyasa koşullarını birleştirir.
-- Tavan/minimum saatleri için driver analizi sağlar:
--   Tavan → residual yük yüksek + AIC düşük + sistem açık
--   Minimum → RES fazlası + sistem kapalı + YAT baskısı

WITH ptf_stats AS (
    -- Tüm dönem için PTF dağılım sınırları (p5 / p95)
    SELECT
        APPROX_QUANTILES(ptf_try, 100)[OFFSET(5)]  AS p5_ptf,
        APPROX_QUANTILES(ptf_try, 100)[OFFSET(95)] AS p95_ptf,
        AVG(ptf_try)                               AS avg_ptf,
        STDDEV(ptf_try)                            AS std_ptf
    FROM {{ ref('stg_pricing') }}
),
hourly AS (
    SELECT
        p.date,
        p.hour,
        p.ptf_try,
        s.smf_try,
        (s.smf_try - p.ptf_try)                   AS smf_ptf_spread,
        l.forecasted_load_mwh,
        r.forecasted_res_mwh,
        (l.forecasted_load_mwh - r.forecasted_res_mwh) AS residual_load_mwh,
        a.total_aic_mwh,
        -- Kapasite stres oranı: residual / AIC (1'e yaklaşırsa stres yüksek)
        SAFE_DIVIDE(
            l.forecasted_load_mwh - r.forecasted_res_mwh,
            a.total_aic_mwh
        )                                          AS capacity_utilization_ratio,
        sd.system_direction,
        imb.net_imbalance_mwh
    FROM {{ ref('stg_pricing') }}         p
    LEFT JOIN {{ ref('stg_smf') }}        s  ON p.date = s.date AND p.hour = s.hour
    LEFT JOIN {{ ref('stg_load_estimation') }} l ON p.date = l.date AND p.hour = l.hour
    LEFT JOIN {{ ref('stg_res_forecast') }}    r ON p.date = r.date AND p.hour = r.hour
    LEFT JOIN {{ ref('stg_aic') }}        a  ON p.date = a.date AND p.hour = a.hour
    LEFT JOIN {{ ref('stg_system_direction') }} sd ON p.date = sd.date AND p.hour = sd.hour
    LEFT JOIN {{ ref('stg_imbalance') }}  imb ON p.date = imb.date AND p.hour = imb.hour
)

SELECT
    h.*,
    st.p5_ptf,
    st.p95_ptf,
    st.avg_ptf,
    -- Saat kategorisi
    CASE
        WHEN h.ptf_try >= st.p95_ptf THEN 'TAVAN'    -- En pahalı %5
        WHEN h.ptf_try <= st.p5_ptf  THEN 'MINIMUM'  -- En ucuz %5
        ELSE 'NORMAL'
    END AS ptf_category,
    -- Tavan için ana tetikleyici
    CASE
        WHEN h.ptf_try >= st.p95_ptf AND h.capacity_utilization_ratio > 0.90
            THEN 'Kapasite_Stresi'
        WHEN h.ptf_try >= st.p95_ptf AND h.system_direction = 'ENERGY_DEFICIT'
            THEN 'Sistem_Acigi'
        WHEN h.ptf_try >= st.p95_ptf
            THEN 'Diger_Tavan'
        -- Minimum için ana tetikleyici
        WHEN h.ptf_try <= st.p5_ptf AND h.residual_load_mwh < 0
            THEN 'RES_Fazlasi'
        WHEN h.ptf_try <= st.p5_ptf AND h.system_direction = 'ENERGY_SURPLUS'
            THEN 'Uretim_Fazlasi'
        WHEN h.ptf_try <= st.p5_ptf
            THEN 'Diger_Minimum'
        ELSE NULL
    END AS extreme_driver
FROM hourly h
CROSS JOIN ptf_stats st
