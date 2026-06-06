# F13 · Streamlit Analytics Dashboard

Entry: `dashboard.py:33` — `st.set_page_config`

```mermaid
flowchart TD
    Load["Page Load<br/>dashboard.py:32-38"]
    CSS["Apply Dark Theme CSS<br/>dashboard.py:40-88"]
    Sidebar["Render Sidebar + Page Selector<br/>dashboard.py:137-169"]
    Route{"Route by page selection<br/>dashboard.py:174"}

    Load --> CSS --> Sidebar --> Route

    Route -->|Executive Summary| P1["query(mart_gold_monthly_executive_metrics)<br/>dashboard.py:182<br/>→ 5 KPIs + 4 charts"]
    Route -->|Fiyat Analizi| P2["query(mart_price_analysis)<br/>dashboard.py:264-270<br/>→ 4 metrics + 4 charts"]
    Route -->|Üretim & Yenilenebilir| P3["query(mart_generation_mix +<br/>mart_renewable_deep)<br/>dashboard.py:360-377<br/>→ 4 metrics + 4 charts"]
    Route -->|GÖP Piyasa| P4["query(mart_gop_volume_analysis +<br/>mart_merit_order)<br/>dashboard.py:464-475<br/>→ 4 metrics + 4 charts"]
    Route -->|Arz-Talep| P5["query(mart_forecasted_residual_load +<br/>mart_ptf_drivers)<br/>dashboard.py:570-581<br/>→ 4 metrics + 3 charts"]
    Route -->|Arz Şoku| P6["query(mart_supply_shock_index)<br/>dashboard.py:664-669<br/>→ 4 metrics + 3 charts"]
    Route -->|PTF Tahmin & ML| P7["query(mart_ptf_lag_features +<br/>gold_ptf_predictions)<br/>dashboard.py:749-761<br/>→ 4 metrics + 5 charts"]
    Route -->|Lisanssız Üretim| P8["query(mart_unlicensed_impact)<br/>dashboard.py:875-880<br/>→ 4 metrics + 3 charts"]
    Route -->|GİP & Hava| P9["query(mart_gip_company_activity +<br/>stg_weather)<br/>dashboard.py:959-974<br/>→ 4 metrics + 4 charts"]
    Route -->|Üretim Planı| P10["query(mart_production_plan)<br/>dashboard.py:1083-1096<br/>→ 4 metrics + 4 charts"]
    Route -->|PTF Tavan| P11["query(mart_ptf_extremes)<br/>dashboard.py:1194-1204<br/>→ 5 metrics + 6 charts"]
    Route -->|Çapraz Arbitraj| P12["query(mart_cross_market_spread)<br/>dashboard.py:1332-1345<br/>→ 5 metrics + 7 charts"]

    P1 & P2 & P3 & P4 & P5 & P6 & P7 & P8 & P9 & P10 & P11 & P12 --> Render["st.plotly_chart / st.metric<br/>Cached @st.cache_data(ttl=3600)<br/>dashboard.py:114-131"]
```

## Gold Tables Consumed (14)
`mart_gold_monthly_executive_metrics`, `mart_price_analysis`, `mart_generation_mix`, `mart_renewable_deep`, `mart_gop_volume_analysis`, `mart_merit_order`, `mart_forecasted_residual_load`, `mart_ptf_drivers`, `mart_supply_shock_index`, `mart_ptf_lag_features`, `gold_ptf_predictions`, `mart_unlicensed_impact`, `mart_gip_company_activity`, `mart_production_plan`, `mart_ptf_extremes`, `mart_cross_market_spread`

Note: `stg_weather` (staging) is also queried directly on page 9 (line 974).

## External Dependencies
- `streamlit`, `plotly.express/graph_objects/subplots`
- `google.cloud.bigquery`, `google.oauth2.service_account`
- `pandas`, `os`, `logging`
- Credentials: `st.secrets["gcp_service_account"]`

## Caching
- `@st.cache_resource` for BQ client (line 115)
- `@st.cache_data(ttl=3600)` for query results (line 126)
