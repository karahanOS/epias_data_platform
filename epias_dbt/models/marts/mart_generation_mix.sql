{{ config(materialized='table') }}

SELECT
    date,
    hour,
    total_generation_mwh AS total_generation,

    -- ── YENİLENEBİLİR ────────────────────────────────────────────────────────
    -- Rüzgar + Güneş + Barajlı Hidrolik + Nehir Hidrolik + Jeotermal + Biyokütle
    -- (Önceki hata: yalnızca river eklenmişti, dam_generation_mwh eksikti)
    SAFE_DIVIDE(
        COALESCE(wind_generation_mwh,      0)
      + COALESCE(solar_generation_mwh,     0)
      + COALESCE(dam_generation_mwh,       0)   -- barajlı hidrolik (~15-20% toplam)
      + COALESCE(river_generation_mwh,     0)
      + COALESCE(geothermal_generation_mwh,0)
      + COALESCE(biomass_generation_mwh,   0),
        NULLIF(total_generation_mwh, 0)
    ) AS renewable_ratio,

    -- ── FOSİL ────────────────────────────────────────────────────────────────
    -- Doğal gaz + Linyit + İthal kömür + Taş kömür + Asfaltit kömür + Fuel oil + Nafta + LNG
    -- (Önceki hata: yalnızca gas_generation_mwh hesaplanmıştı, kömür türleri eksikti)
    SAFE_DIVIDE(
        COALESCE(gas_generation_mwh,                  0)
      + COALESCE(lignite_generation_mwh,              0)   -- linyit (~20-25% toplam)
      + COALESCE(imported_coal_generation_mwh,        0)   -- ithal kömür
      + COALESCE(black_coal_generation_mwh,           0)   -- taş kömür
      + COALESCE(asphaltite_coal_generation_mwh,      0)   -- asfaltit kömür
      + COALESCE(fueloil_generation_mwh,              0)
      + COALESCE(naphtha_generation_mwh,              0)
      + COALESCE(lng_generation_mwh,                  0),
        NULLIF(total_generation_mwh, 0)
    ) AS fossil_ratio

FROM {{ ref('stg_generation') }}