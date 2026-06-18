# Analyzing the Turkish Electricity Market (EPİAŞ): A KPI, Signal, and Dashboard Design Guide

## TL;DR
- **For short-term traders**, the most actionable EPİAŞ signals are the hourly PTF (day-ahead clearing price) curve and its peak/off-peak spreads, the GİP intraday weighted-average price and its deviation from PTF, and the DGP system direction (SDF: long/short) with the imbalance prices it produces (deficit = max(PTF,SMF)×1.03; surplus = min(PTF,SMF)×0.97) — all driven fundamentally by natural-gas cost, hydro/reservoir conditions, and wind/solar forecast error.
- **For strategic/fundamental analysts**, the key drivers are the fuel mix and merit order (coal 35.6%, hydro 22%, gas 18.5%, wind 10.7% + solar 7.5% in 2024 per Ember), installed-capacity trends (121,412 MW by end-October 2025), demand growth and the summer-peaking load curve, the monthly YEKDEM unit cost and its sign (positive/negative), BOTAŞ gas tariffs, the USD/TRY rate, and a fast-moving regulatory layer (price ceilings, the AUF maximum-settlement mechanism, YEK-G certificates).
- **A practical dashboard** should be built on the EPİAŞ Transparency Platform v2.0 API (≈200+ endpoints, accessible via open-source wrappers like `eptr2` and `seffaflik`), organized into GÖP, GİP, DGP, generation, consumption, renewables/YEKDEM, and a regulatory-tracker layer, with a trader view (real-time prices, spreads, system direction) and an analyst view (fuel mix, capacity, demand forecasts, forecast-error analytics).

## Key Findings

1. **Turkey's wholesale market is a sequential GÖP → GİP → DGP structure operated by EPİAŞ (EXIST), with TEİAŞ as system operator.** The day-ahead market (GÖP) sets the reference price (PTF / Market Clearing Price), the intraday market (GİP) allows continuous trading up to ~1 hour before delivery and produces a weighted-average price, and the balancing power market (DGP) is run in real time by TEİAŞ's National Load Dispatch Center (MYTM), producing the System Marginal Price (SMF). The bulk of volume still moves through bilateral contracts.

2. **Gas sets the price at the margin, so PTF tracks BOTAŞ gas tariffs and USD/TRY.** Because gas plants are usually the marginal price-setter, wholesale electricity prices are strongly correlated with natural-gas prices, which are in turn driven by the dollar exchange rate. This makes gas price and FX the single most important fundamental inputs for price forecasting.

3. **A government price-cap regime dominates price formation.** EPDK caps the GÖP/DGP maximum price. The ceiling has been repeatedly reset — TRY 2,600 → 2,700/MWh in mid-2023, 3,400 TL/MWh from April 2025, and 4,500 TL/MWh from 4 April 2026 (per EPDK Board Decision No. 14459 dated 2 April 2026, covering GÖP and DGP — a ~32% rise from the 3,400 TL/MWh set by the 3 April 2025 decision). A parallel "maximum settlement price" mechanism (AUF/AÜM, launched 1 April 2022) diverts revenue from cheap generators (renewables) to expensive gas/coal plants. The floor is 0 TL/MWh, and in 2025 wholesale prices hit zero for 42 hours, all between 09:00–15:00 during peak solar.

4. **Wind and solar forecast error is the dominant driver of imbalance and intraday activity.** Empirical research on the Turkish balancing market shows SMF declines as variable renewable generation rises (merit-order effect) and that positive system imbalances become more likely as real-time renewable output exceeds forecasts. Forecast-vs-actual tracking for wind and solar is therefore central for both traders and analysts.

5. **The EPİAŞ Transparency Platform (seffaflik.epias.com.tr) is the single richest free data source** and publishes PTF/SMF/SDF, real-time generation by fuel and consumption, demand forecasts, KGÜP (final daily generation schedules), DGP instructions (YAL/YAT), wind forecasts, dam/reservoir data, intraday weighted-average prices and trade volumes, and YEKDEM data. Multiple open-source Python libraries wrap its API.

## Details

### A. Market structure and price reference points

| Segment | Turkish / Abbrev. | Operator | Key price | Granularity |
|---|---|---|---|---|
| Day-Ahead Market | Gün Öncesi Piyasası (GÖP) | EPİAŞ | **PTF** (Piyasa Takas Fiyatı / MCP) | Hourly, day-ahead auction |
| Intraday Market | Gün İçi Piyasası (GİP) | EPİAŞ | GİP weighted-average price (often called intraday "AOF") | Continuous, hourly/block contracts |
| Balancing Power Market | Dengeleme Güç Piyasası (DGP) | TEİAŞ (MYTM) | **SMF** (Sistem Marjinal Fiyatı / SMP) | Real-time, 15-min instructions |
| Ancillary services | Yan Hizmetler | TEİAŞ | Capacity fees (primary/secondary frequency control) | Contracted |
| Green certificates | YEK-G | EPİAŞ | Certificate price by source | Continuous trading |

GÖP timeline: orders submitted until ~12:30, clearing 13:00–13:30, objections to ~14:00, then final hourly PTF for all 24 hours of the next day. Orders are hourly, block (3–24h, all-or-nothing, can be linked), or flexible; 1 lot = 0.1 MWh.

**SMF/PTF relationship and system direction (SDF):** SMF > PTF when the system is **short** (energy deficit, "Enerji Açığı"); SMF < PTF when the system is **long** (surplus, "Enerji Fazlası"). The sign and magnitude of (SMF − PTF) is the System Direction signal (SDF) that traders watch in real time.

**Ancillary services:** Primary and secondary frequency control are procured by TEİAŞ under the Ancillary Services Regulation (Official Gazette, 26 Nov 2017, No. 30252); they are no longer mandatory and are remunerated via a capacity fee, with non-performance penalties. Secondary frequency control tests are required at facilities ≥100 MW (simulated against TEİAŞ MYTM's Automatic Generation Control). Tertiary reserve is effectively delivered through DGP instructions.

**Imbalance pricing (settlement consequences):**
- Negative (deficit) imbalance price = **max(PTF, SMF) × 1.03**
- Positive (surplus) imbalance price = **min(PTF, SMF) × 0.97**
- The ±3% (k = ℓ = 0.03) coefficients were set by an EPDK decision dated 1 May 2015 and remain current; they can be raised by board decision up to 1. A draft EPDK consultation (comments due 7 October 2025) proposes making k/ℓ direction-dependent, intended for 2026/2027 — this is pending and not yet in force. Storage facilities face a 6% coefficient.
- The **Zero Balance Correction Amount (Sıfır Bakiye Düzeltme Tutarı, SBDT)** redistributes the residual pool surplus created by the ±3% margins so the settlement nets to zero (TRY 1.23 billion in 2017 as an order-of-magnitude reference).

**KÜPST (generator deviation penalty vs. KGÜP):** Per-unit KÜPST = max(PTF, SMF) × 0.03 (0.06 for storage), applied only to deviation exceeding a tolerance band. EPDK Decision 13025 (Dec 2024) **lowered the tolerance bands**: wind from 21% → 17%, solar from 12% → 10%, other sources and storage 5%. This raised imbalance costs for wind/solar producers and increased the value of accurate renewable forecasting.

### B. What short-term traders monitor (price, spreads, arbitrage)

**Daily/hourly KPIs:**
- **PTF hourly curve** for D+1 and the day-shape: day hours (06:00–17:00), peak/puant (17:00–22:00), night (22:00–06:00). EPİAŞ weekly reports explicitly break out these three blocks (e.g., week 1 of 2025: day 2,500.21, peak 2,826.28, night 2,183.91 TL/MWh).
- **Peak/off-peak spread and intraday range** (max−min within the day), which define battery/hydro arbitrage and block-bid strategy value.
- **PTF–SMF spread** by hour (e.g., the largest PTF-favorable spread of 2,190.01 TL/MWh occurred 31 Dec 2024 at 16:00 in EPİAŞ's weekly report) — signals system tightness and imbalance risk.
- **GİP vs PTF deviation** by hour — the intraday spread is the core actionable signal for balancing positions; intraday jumps when wind forecasts worsen.
- **GİP liquidity/trade volume** and matched quantities (GİP eşleşme miktarı), and the number of active participants.
- **System direction (SDF: long/short)** and **DGP instruction volumes (YAL = load-taking/up, YAT = load-shedding/down)** in near real time.
- **Price-independent bid/offer volumes** (fiyattan bağımsız alış/satış) which EPİAŞ weekly reports cite as the swing factor that moves PTF day to day.
- **Proximity to the price cap and floor** — with a hard ceiling and a 0 TL/MWh floor, traders watch how often hours pin to the limits.

**Most predictive/actionable patterns:**
- **Gas price + USD/TRY** as the level anchor for PTF.
- **Wind/solar forecast vs. actual** — the key intraday and imbalance driver; solar now routinely drives midday PTF toward zero (42 zero-price hours in 2025, all 09:00–15:00).
- **Hydro/reservoir (dam) levels and inflows** — a structural driver of the supply curve; EPİAŞ publishes daily dam data.
- **Day-ahead vs. intraday spread densities** — academically shown to be the relevant object for storage arbitrage and bid optimization.
- **The summer cooling peak** — since 2008 Turkey's peak has shifted to summer; per Ember, more than 10% of electricity consumption between 12:00 PM and 6:00 PM came from cooling alone, with this share exceeding 18% on weekends and public holidays; all-time-high hourly consumption reached 59 GWh, 18% of it from cooling.

### C. What strategic/fundamental analysts monitor (capacity, fuel mix, demand, regulation)

**Generation & capacity (weekly/monthly/annual):**
- **Installed capacity by source** — 121,412 MW by end-October 2025 per the Türkiye Ministry of Energy and Natural Resources (26.6% hydraulic, 20.2% solar, 19.8% natural gas, 18.1% coal, 11.8% wind, 1.4% geothermal; rising to ~122.5 GW by end-December 2025) — and the additions pipeline.
- **Generation mix by fuel** — 2024 generation per Ember Türkiye Electricity Review 2025: coal 35.6%, hydro 22%, gas 18.5%, wind 10.7%, solar 7.5% (Ministry figures differ slightly: coal 34.7%, gas 18.9%, hydro 21.1%). Per Ember, Türkiye's coal-fired generation was 122 TWh — an all-time high, compared to 104 TWh in Germany and 91 TWh in Poland — making it Europe's largest coal generator in absolute terms, with imports accounting for 61% of coal power production.
- **Merit-order shifts**: wind+solar (62 TWh in 2024) now exceed domestic coal; fossil share fell to ~55%, the lowest since 1993.
- **Hydro condition** — generation swings ±10–30% year to year with rainfall/drought; drought in peak-demand August creates irrigation-vs-generation conflicts.
- **Plant outage/availability** (EAK available capacity, KGÜP final schedules, maintenance/fault messages via the Market Message System).

**Demand:**
- **National load and hourly load curve**, peak vs off-peak, weekday/weekend/holiday effects, and the widening summer-winter peak gap (12× larger in 2025 vs 2008, >9 GWh).
- **Demand forecasts** — TEİAŞ/Ministry low/base/high 10-year forecasts; per the Türkiye National Energy Plan, electricity consumption is expected to be 380.2 TWh in 2025, 455.3 TWh in 2030, and 510.5 TWh in 2035.
- **Demand growth drivers** — heavy industry, cooling, EVs. Per Ember, electricity demand increased by 5.5% (+18 TWh) in 2024 to a record 342 TWh (the Ministry reports gross consumption up 5.5% to 353.6 TWh), "mostly because of record meteorological heat pushing up cooling needs" — one of the world's highest growth rates that year.

**Renewables & YEKDEM:**
- **Wind/solar penetration and curtailment risk**, forecast-vs-actual accuracy, and hybrid (solar+storage) build-out.
- **Monthly YEKDEM unit cost and its sign** — a critical fundamental: YEKDEM became negative from Feb 2022 (when PTF exceeded feed-in tariffs), then positive again from April 2023; the YEKDEM cost is added to/subtracted from supplier costs and tracked alongside PTF (PTF+YEKDEM).
- **YEKDEM feed-in tariffs** (post-July-2021 scheme in TRY with quarterly escalation and USD caps: e.g., wind/solar TRY 0.32/kWh, hydro 0.40, geothermal 0.54, +0.08 domestic-content bonus for 5 years; eligible if commissioned between 1 July 2021 and 31 Dec 2025/2030 depending on source).
- **YEK-G green certificate prices** (launched 1 June 2021; 1 certificate per MWh, valid 12 months; ~5–60 TL/MWh range) for corporate ESG demand.

**Regulatory / structural factors unique to Turkey analysts must track:**
- **EPDK price ceiling** changes (3,400 TL/MWh from Apr 2025; 4,500 TL/MWh from 4 Apr 2026) and the **AUF/AÜM maximum settlement mechanism** status (launched Apr 2022; periodic 6-month renewals; status around 30 Sep 2025 should be verified directly against EPDK decisions — see caveats).
- **BOTAŞ gas tariff** changes (e.g., +24.2% for power plants in April 2025, +7.86% for businesses in July 2025) and the state's price guarantee for state gas plants.
- **FX (USD/TRY)** as the transmission channel from global fuel prices to domestic electricity.
- **KÜPST tolerance changes** (Dec 2024) and other DUY (Balancing & Settlement Regulation) amendments.
- **Eligible-consumer limit** (lowered to 750 kWh/year in 2025), CBAM exposure for exports (definitive regime from 2026), ENTSO-E integration, and nuclear (Akkuyu) coming online.

### D. EPİAŞ Transparency Platform: what it publishes and what practitioners derive

The platform (seffaflik.epias.com.tr; reports at rapor.epias.com.tr) is mandated by EPDK (EMRA Board Decision 6282-4 of 13/05/2016, last updated by Decision 10711 of 06/01/2022) and publishes, among ~200+ series:
- **Prices:** PTF, SMF, SDF, imbalance prices; intraday weighted-average price and trade amount.
- **Generation:** real-time generation by fuel type, **KGÜP** (Kesinleşmiş Günlük Üretim Programı / final daily generation plan) and KUDÜP, UEVM (metered realized generation), EAK available capacity, UEVÇB unit-level data.
- **DGP:** YAL/YAT instruction volumes and prices.
- **Consumption:** real-time consumption, demand forecast, distribution/planned-outage data.
- **Renewables:** wind generation forecast, solar, dam/reservoir levels.
- **YEKDEM:** participating capacity, unit cost.

Practitioners derive: forecast-error (KGÜP vs UEVM), imbalance and KÜPST cost estimates, plant-level P&L, system-direction nowcasts, fuel-mix dashboards, and PTF forecasting model inputs. Open-source tooling includes `eptr2` (Robokami/Tideseed, Apache-2.0, 213+ services, with composite plant-cost and KÜPST/imbalance calculators and an MCP server for AI agents) and `seffaflik`/`seffaflik2` (PyPI). Commercial intelligence platforms (e.g., NAR Sistem — a multi-tenant SaaS ingesting "all 300 EPİAŞ transparency API endpoints" with Bloomberg-style dashboards; Sayax for plant-side DGP/KGÜP automation) also exist.

### E. European tools that can inspire Turkish equivalents

- **EPEX SPOT** publishes refined intraday indices — **IDFull** (volume-weighted average of all continuous trades), **ID3** (last 3 trading hours), **ID1** (last trading hour, capturing last-minute imbalance needs) — plus 15/30-minute contracts and call auctions for granular price signals. A Turkish dashboard could replicate ID1/ID3-style "closeness-to-delivery" indices on GİP.
- **Nord Pool** provides intraday market statistics and data portals across the Nordic, Baltic, and Central-Western European markets.
- **ENTSO-E Transparency Platform** is the pan-European analog of seffaflik (generation mix, load, flows, prices), and the standard reference for cross-market analytics and carbon accounting.
- **Montel** and similar provide imbalance-forecast feeds and short-term market analytics; the lesson is that **early imbalance/forecast signals and market-coupling spreads** are the highest-value derived products.

### F. Analytical methods in the literature (PTF/SMF forecasting in Turkey)
- Deep-learning EPF on Turkish day-ahead data: MLP, CNN, LSTM, GRU; LSTM best at ~8.15% MAPE in one study; sub-hourly (15-min) AutoGluon ensembles using EPİAŞ price + TEİAŞ stream/sun/wind features.
- Classical ARMA/ARIMA/SARIMAX baselines remain common.
- Natural-gas price as an exogenous feature reduces MAPE (one IEEE study: 15.85% → 14.31% using two-week-lagged gas).
- Quantile/ordered-logistic regression on the balancing market quantifies the VRE merit-order effect and imbalance-direction probability.
- SARIMA + ANN for load forecasting with day-of-week/hour/holiday seasonality features.

## Recommendations

**Stage 1 — Build the data backbone (week 1–2).** Stand up ingestion from the EPİAŞ Transparency v2.0 API using `eptr2` (it already has composite functions for plant cost, KGÜP-vs-realized, and KÜPST/imbalance calculators). Prioritize these series: PTF, SMF, SDF/imbalance prices, real-time generation by fuel, real-time consumption + demand forecast, wind forecast, dam levels, intraday weighted price + volume, KGÜP, YAL/YAT, monthly YEKDEM. Store in a time-series DB (e.g., TimescaleDB) with hourly granularity.

**Stage 2 — Trader view (week 3–4).** Build a real-time Streamlit page with: (a) PTF D+1 hourly curve with day/peak/night block averages and the daily spread; (b) PTF–SMF spread and live system direction (long/short) gauge; (c) GİP-vs-PTF deviation heatmap by hour and a Turkish "ID1/ID3" closeness-to-delivery index; (d) imbalance-price calculator using max/min(PTF,SMF)×1.03/0.97; (e) wind/solar forecast-vs-actual with intraday alerts; (f) cap/floor proximity indicator.

**Stage 3 — Analyst view (week 5–6).** Add: fuel-mix and merit-order dashboards; installed-capacity and pipeline tracker; demand curve with weekday/weekend/holiday and summer-peak decomposition; YEKDEM unit-cost (with sign) and PTF+YEKDEM series; a regulatory tracker (price ceiling history, AUF status, BOTAŞ gas tariffs, KÜPST tolerances, eligible-consumer limit) with change-log alerts; USD/TRY and gas-price overlays.

**Stage 4 — Forecasting & analytics (week 7+).** Implement a PTF forecaster (start with SARIMAX baseline incorporating gas price, demand forecast, wind/solar forecast, hydro; benchmark against LSTM/GRU). Add a KGÜP-vs-UEVM forecast-error analytics module and KÜPST/imbalance cost attribution by plant. Add day-ahead-vs-intraday spread density estimation for storage/flexible-asset arbitrage.

**Benchmarks/thresholds that should change the plan:**
- If EPDK finalizes **direction-dependent k/ℓ coefficients** (2026/2027), rebuild the imbalance calculator immediately — this changes optimal bidding.
- If the **AUF mechanism is terminated**, PTF volatility and the value of price forecasting rise materially — increase forecasting investment.
- If **negative pricing** is introduced (currently floored at 0), add negative-price handling and storage-dispatch logic.
- If **15-minute settlement** is adopted (EU trend), re-granularize all series from hourly to quarter-hourly.

## Caveats
- **Regulatory volatility is the biggest risk.** Price ceilings, the AUF mechanism, KÜPST tolerances, and imbalance coefficients change frequently and sometimes retroactively; any dashboard must include a maintained regulatory change-log. The exact status of the AUF/AÜM mechanism around 30 September 2025 could not be confirmed from primary EPDK sources and should be verified directly before relying on it.
- **The reported "750 TL/MWh KÜPST floor" is unconfirmed** — the 750 figure was verifiably the 2025 eligible-consumer limit in kWh/year, not necessarily a KÜPST price floor; verify against the EPDK Decision 13025 text.
- Some generation-mix percentages differ by source (Ministry vs Ember vs IEA vs Enerdata) due to different definitions (licensed vs total, gross vs net); treat ±1–2 pp as noise and cite the source on the dashboard.
- The figures from the 2018 Turkish market report (e.g., 2017 statistics) are historical and included for structural context, not current levels.
- Open-source API wrappers are unofficial; EPİAŞ does not guarantee third-party data accuracy, so include validation checks against official daily/weekly reports.
- Price-forecasting MAPE figures are study-specific and not directly comparable across periods given the 2022+ price-cap regime distortions.