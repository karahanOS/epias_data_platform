{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH hourly_system AS (
    -- 1. Makro Görünüm: Sistem Yönü, Fiyatlar ve Dengesizlik
    SELECT
        smf.date,
        smf.systemMarginalPrice AS smf_try,
        p.marketTradePrice AS ptf_try,
        -- Spread pozitifse (SMF > PTF): Sistemde enerji açığı var, YAL talimatları pahalıya mal olmuş
        -- Spread negatifse (SMF < PTF): Sistemde enerji fazlası var, YAT talimatları ile fiyat düşmüş
        (smf.systemMarginalPrice - p.marketTradePrice) AS smf_ptf_spread_try,
        sd.systemDirection,
        imb.imbalanceQuantity AS net_imbalance_mwh
    FROM {{ source('silver', 'smf') }} smf
    LEFT JOIN {{ source('silver', 'pricing') }} p ON smf.date = p.date
    LEFT JOIN {{ source('silver', 'system_direction') }} sd ON smf.date = sd.date
    LEFT JOIN {{ source('silver', 'imbalance') }} imb ON smf.date = imb.date
),

company_yal AS (
    -- 2. Mikro Görünüm: Yük Alma (YAL) Talimatı Alan Şirketler (Enerji Açığını Kapatanlar)
    SELECT
        date,
        organizationId,
        SUM(upRegulationDeliveredAmount) AS total_yal_mwh,
        AVG(upRegulationZeroCodedOfferPrice) AS avg_yal_price_try
    FROM {{ source('silver', 'order_up') }}
    WHERE upRegulationDeliveredAmount > 0
    GROUP BY 1, 2
),

company_yat AS (
    -- 3. Mikro Görünüm: Yük Atma (YAT) Talimatı Alan Şirketler (Enerji Fazlasını Kapatanlar)
    SELECT
        date,
        organizationId,
        SUM(downRegulationDeliveredAmount) AS total_yat_mwh,
        AVG(downRegulationZeroCodedBidPrice) AS avg_yat_price_try
    FROM {{ source('silver', 'order_down') }}
    WHERE downRegulationDeliveredAmount > 0
    GROUP BY 1, 2
),

combined_company_orders AS (
    -- 4. YAL ve YAT verilerini şirket bazında birleştiriyoruz
    SELECT
        COALESCE(u.date, d.date) AS date,
        COALESCE(u.organizationId, d.organizationId) AS organizationId,
        COALESCE(u.total_yal_mwh, 0) AS total_yal_mwh,
        u.avg_yal_price_try,
        COALESCE(d.total_yat_mwh, 0) AS total_yat_mwh,
        d.avg_yat_price_try
    FROM company_yal u
    FULL OUTER JOIN company_yat d
        ON u.date = d.date AND u.organizationId = d.organizationId
),

participants AS (
    -- 5. Boyut Tablosu: ID'leri Şirket İsimlerine Çeviriyoruz
    SELECT organizationId, organizationName
    FROM {{ source('silver', 'participants') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY organizationId ORDER BY year DESC, month DESC, day DESC) = 1
)

-- 6. Nihai Birleşim (Makro Sistem Durumu + Mikro Şirket Aksiyonları)
SELECT
    co.date,
    hs.systemDirection,
    hs.net_imbalance_mwh,
    hs.smf_try,
    hs.ptf_try,
    hs.smf_ptf_spread_try,
    
    co.organizationId,
    COALESCE(part.organizationName, 'Bilinmeyen Şirket') AS company_name,
    
    co.total_yal_mwh,
    co.avg_yal_price_try,
    co.total_yat_mwh,
    co.avg_yat_price_try,
    
    -- Finansal Göstergeler
    -- YAL Hacmi: Şirketin sisteme fazladan sattığı enerjinin tahmini değeri (Ödül)
    (co.total_yal_mwh * co.avg_yal_price_try) AS estimated_yal_volume_try,
    -- YAT Hacmi: Şirketin üretimini kısarak sistemden geri aldığı enerjinin değeri
    (co.total_yat_mwh * co.avg_yat_price_try) AS estimated_yat_volume_try

FROM combined_company_orders co
LEFT JOIN hourly_system hs ON co.date = hs.date
LEFT JOIN participants part ON co.organizationId = part.organizationId