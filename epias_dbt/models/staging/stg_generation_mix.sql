SELECT
    -- Join için bu kolonun mutlaka seçilmesi lazım
    CONCAT(CAST(date AS STRING), ' ', hour) as join_key,
    date,
    hour,
    total_generation,
    renewable_ratio,
    fossil_ratio,
    year,
    month
FROM {{ source('epias_gold', 'gold_generation_mix_price_impact') }}