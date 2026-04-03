SELECT
    CONCAT(CAST(date AS STRING), ' ', hour) as join_key,
    date,
    hour,
    total_generation,
    renewable_ratio,
    fossil_ratio,
    -- Partition kolonları yerine date üzerinden manuel extract yapıyoruz
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month
FROM {{ source('epias_gold', 'gold_generation_mix_price_impact') }}