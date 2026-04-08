import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import os

# ── CONFIG (Tamamen Bağımsız) ──────────────────────────────────────────────────
st.set_page_config(page_title="EPIAŞ Elektrik Piyasası", page_icon="⚡", layout="wide")

PROJECT = "epias-data-platform"
DATASET = "epias_gold"

# ── CSS (Modern ve Temiz UI) ──────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
    :root { --bg: #0a0e1a; --surface: #111827; --accent: #00d4ff; --text: #e2e8f0; --border: rgba(0, 212, 255, 0.15); }
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background-color: var(--bg); color: var(--text); }
    [data-testid="stSidebar"] { background: var(--surface) !important; border-right: 1px solid var(--border); }
    .page-header { background: linear-gradient(135deg, #1a2235 0%, rgba(0,212,255,0.05) 100%); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

# ── BIGQUERY BAĞLANTISI ───────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    creds_path = "credentials/gcp-key.json"
    if os.path.exists(creds_path):
        return bigquery.Client.from_service_account_json(creds_path)
    return bigquery.Client(project=PROJECT)

def safe_query(sql):
    try:
        client = get_client()
        df = client.query(sql).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Veri çekme hatası: {str(e)}")
        return pd.DataFrame()

# ── SIDEBAR & NAVİGASYON ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<h2 style='color:#00d4ff; text-align:center;'>⚡ EPIAŞ ANALİTİK</h2>", unsafe_allow_html=True)
    page = st.selectbox("📊 Sayfa Seç", [
        "🏠 Executive Summary", "⚖️ Fiyat Dengesizliği", "🌱 Üretim Karışımı", 
        "🔋 Arz-Talep Analizi", "📉 Yük Tahmin Sapması", "🌬️ Yenilenebilir Derinlemesine"
    ])
    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1: EXECUTIVE SUMMARY
# ═════════════════════════════════════════════════════════════════════════════
if page == "🏠 Executive Summary":
    st.markdown("<div class='page-header'><h1>Executive Summary</h1></div>", unsafe_allow_html=True)
    df_exec = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.mart_gold_monthly_executive_metrics` ORDER BY year_month")
    
    if not df_exec.empty:
        # API Export
        st.sidebar.download_button("📥 Dataseti İndir (CSV)", df_exec.to_csv(index=False), "epias_summary.csv", "text/csv")
        
        last = df_exec.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("Ort. PTF", f"{last['avg_ptf']:,.2f} TL")
        c2.metric("Maks. PTF", f"{last['max_ptf']:,.2f} TL")
        c3.metric("Tüketim", f"{last['total_consumption']:,.0f} MWh")

        fig = px.line(df_exec, x="year_month", y="avg_ptf", title="Aylık PTF Trendi", template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

        # ML ANALİZİ (2026 Saatlik Backtesting)
        st.markdown("---")
        st.subheader("🎯 ML Tahmin Başarımı (2026)")
        df_ml = safe_query(f"""
            WITH cp AS (SELECT SAFE_CAST(SAFE_CAST(hour AS FLOAT64) AS INT64) as h, DATE(predicted_date) as d, AVG(predicted_ptf) as p FROM `{PROJECT}.{DATASET}.gold_ptf_predictions` WHERE predicted_date >= '2026-01-01' GROUP BY 1, 2),
            af AS (SELECT DATE(date) as d, CAST(hour_of_day AS INT64) as h, AVG(ptf) as a FROM `{PROJECT}.{DATASET}.mart_ptf_lag_features` WHERE date >= '2026-01-01' GROUP BY 1, 2)
            SELECT TIMESTAMP_ADD(TIMESTAMP(cp.d), INTERVAL cp.h HOUR) as dt, cp.p, af.a FROM cp JOIN af ON cp.d = af.d AND cp.h = af.h ORDER BY 1
        """)
        if not df_ml.empty:
            fig_ml = go.Figure()
            fig_ml.add_trace(go.Scatter(x=df_ml["dt"], y=df_ml["a"], name="Gerçek", line=dict(color="#00d4ff")))
            fig_ml.add_trace(go.Scatter(x=df_ml["dt"], y=df_ml["p"], name="Tahmin", line=dict(color="#ff6b35", dash="dash")))
            st.plotly_chart(fig_ml.update_layout(template="plotly_dark"), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2: FİYAT DENGESİZLİĞİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Fiyat Dengesizliği":
    st.markdown("<div class='page-header'><h1>Fiyat Dengesizliği Analizi</h1></div>", unsafe_allow_html=True)
    df_spread = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.price_spread_analysis` LIMIT 2000")
    if not df_spread.empty:
        fig_s = px.scatter(df_spread, x="ptf", y="smf", color="system_direction", title="PTF vs SMF", template="plotly_dark")
        st.plotly_chart(fig_s, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3: ÜRETİM KARIŞIMI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌱 Üretim Karışımı":
    st.markdown("<div class='page-header'><h1>Üretim Karışımı</h1></div>", unsafe_allow_html=True)
    df_gen = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.generation_mix_price_impact` LIMIT 2000")
    if not df_gen.empty:
        fig_g = px.scatter(df_gen, x="renewable_ratio", y="ptf", color="ptf", title="Yenilenebilir Oranı vs PTF", template="plotly_dark")
        st.plotly_chart(fig_g, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4: ARZ-TALEP ANALİZİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🔋 Arz-Talep Analizi":
    st.markdown("<div class='page-header'><h1>Arz-Talep Analizi</h1></div>", unsafe_allow_html=True)
    df_sd = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.supply_demand_summary`")
    if not df_sd.empty:
        fig_sd = px.bar(df_sd.groupby("time_of_day")["coverage_ratio"].mean().reset_index(), x="time_of_day", y="coverage_ratio", template="plotly_dark")
        st.plotly_chart(fig_sd, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 5: YÜK TAHMİN SAPMASI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📉 Yük Tahmin Sapması":
    st.markdown("<div class='page-header'><h1>Yük Tahmin Sapması</h1></div>", unsafe_allow_html=True)
    df_load = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.gold_load_vs_actual` LIMIT 168")
    if not df_load.empty:
        fig_l = px.line(df_load, x="hour", y=["forecast_consumption", "actual_consumption"], title="Son 1 Haftalık Tüketim Sapması", template="plotly_dark")
        st.plotly_chart(fig_l, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 6: YENİLENEBİLİR DERİNLERİNESİNE
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌬️ Yenilenebilir Derinlemesine":
    st.markdown("<div class='page-header'><h1>Yenilenebilir Enerji Derinlemesine</h1></div>", unsafe_allow_html=True)
    df_deep = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.renewable_deep_analysis` WHERE date >= '2026-01-01'")
    if not df_deep.empty:
        st.markdown("#### Mevsimsel Korelasyon Tablosu")
        corr_df = df_deep.groupby("season").apply(lambda x: pd.Series({
            "Mevsim": x.name,
            "Rüzgar": round(x["wind_ratio"].corr(x["ptf"]), 3) if "wind_ratio" in x else 0,
            "Güneş": round(x["sun_ratio"].corr(x["ptf"]), 3) if "sun_ratio" in x else 0,
            "Doğalgaz": round(x["gas_ratio"].corr(x["ptf"]), 3) if "gas_ratio" in x else 0,
        }), include_groups=False).reset_index(drop=True)
        st.dataframe(corr_df.style.background_gradient(cmap="RdYlGn"), use_container_width=True, hide_index=True)