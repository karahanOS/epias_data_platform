import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import os

# ── CONFIG ────────────────────────────────────────────────────────────────────
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/gcp-key.json"
PROJECT = "epias-data-platform"
DATASET = "epias_gold"

st.set_page_config(
    page_title="EPIAŞ Elektrik Piyasası",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CUSTOM CSS (Modern UI) ─────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
    :root {
        --bg: #0a0e1a; --surface: #111827; --accent: #00d4ff; --accent2: #ff6b35; --text: #e2e8f0; --border: rgba(0, 212, 255, 0.15);
    }
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background-color: var(--bg); color: var(--text); }
    [data-testid="stSidebar"] { background: var(--surface) !important; border-right: 1px solid var(--border); }
    [data-testid="metric-container"] { background: #1a2235; border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
    [data-testid="stMetricValue"] { font-family: 'Space Mono', monospace !important; color: var(--accent) !important; }
    .page-header { background: linear-gradient(135deg, #1a2235 0%, rgba(0,212,255,0.05) 100%); border: 1px solid var(--border); border-radius: 16px; padding: 24px 32px; margin-bottom: 24px; position: relative; overflow: hidden; }
    .page-header::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: linear-gradient(90deg, var(--accent), #7c3aed, var(--accent2)); }
    .badge { display: inline-block; background: rgba(0, 212, 255, 0.1); border: 1px solid rgba(0, 212, 255, 0.3); color: var(--accent); padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; }
</style>
""", unsafe_allow_html=True)

# ── BIGQUERY CLIENT ───────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    creds_path = "credentials/gcp-key.json"
    if os.path.exists(creds_path):
        return bigquery.Client.from_service_account_json(creds_path)
    return bigquery.Client(project=PROJECT)

@st.cache_data(ttl=3600)
def query(sql):
    client = get_client()
    return client.query(sql).to_dataframe()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 24px 0;'>
        <div style='font-size:2.5rem;'>⚡</div>
        <div style='font-family: Space Mono, monospace; font-size: 0.9rem; color: #00d4ff; margin-top: 8px;'>EPIAŞ ANALİTİK</div>
        <div style='font-size: 0.7rem; color: #64748b; margin-top: 4px;'>Profesyonel Pazar İzleme</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.selectbox(
        "📊 Sayfa Seç",
        ["🏠 Executive Summary", "⚖️ Fiyat Dengesizliği", "🌱 Üretim Karışımı", "🔋 Arz-Talep Analizi", "📉 Yük Tahmin Sapması", "🌬️ Yenilenebilir Derinlemesine"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1: Executive Summary & ML Analysis
# ═════════════════════════════════════════════════════════════════════════════
if page == "🏠 Executive Summary":
    st.markdown("<div class='page-header'><span class='badge'>CANLI</span><h1>Executive Summary</h1><p>Genel görünüm ve ML tahmin başarımı</p></div>", unsafe_allow_html=True)
    
    df = query(f"SELECT * FROM `{PROJECT}.{DATASET}.mart_gold_monthly_executive_metrics` ORDER BY year_month")
    
    if not df.empty:
        last, prev = df.iloc[-1], (df.iloc[-2] if len(df) > 1 else df.iloc[-1])
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ort. PTF (TL)", f"{last['avg_ptf']:,.2f}", f"{last['avg_ptf']-prev['avg_ptf']:+.2f}")
        c2.metric("Maks. PTF", f"{last['max_ptf']:,.2f}")
        c3.metric("Toplam Tüketim", f"{last['total_consumption']:,.0f} MWh")
        c4.metric("Ort. Fiyat Makası", f"{last.get('avg_price_spread', 0):,.2f}")

        # Trend Grafiği
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=df["year_month"], y=df["avg_ptf"], name="Ort. PTF", line=dict(color="#00d4ff", width=2.5), fill="tozeroy"), secondary_y=False)
        fig.add_trace(go.Bar(x=df["year_month"], y=df["avg_hourly_consumption"], name="Tüketim", marker_color="rgba(124, 58, 237, 0.3)"), secondary_y=True)
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # 🎯 ML BACKTESTING & SHAP (YENİ EKLENEN KISIM)
    st.markdown("---")
    st.subheader("🎯 Model Tahmin Başarımı (2026 Saatlik)")
    col_bt, col_shap = st.columns([1.2, 0.8])
    
    with col_bt:
        hourly_query = f"""
            WITH clean_p AS (SELECT SAFE_CAST(SAFE_CAST(hour AS FLOAT64) AS INT64) as h, DATE(predicted_date) as d, AVG(predicted_ptf) as p_ptf FROM `{PROJECT}.{DATASET}.gold_ptf_predictions` WHERE predicted_date >= '2026-01-01' GROUP BY 1, 2),
            actual_f AS (SELECT DATE(date) as d, CAST(hour_of_day AS INT64) as h, AVG(ptf) as a_ptf FROM `{PROJECT}.{DATASET}.mart_ptf_lag_features` WHERE date >= '2026-01-01' GROUP BY 1, 2)
            SELECT TIMESTAMP_ADD(TIMESTAMP(p.d), INTERVAL p.h HOUR) as full_dt, p.p_ptf, f.a_ptf FROM clean_p p JOIN actual_f f ON p.d = f.d AND p.h = f.h ORDER BY 1
        """
        df_bt = query(hourly_query)
        if not df_bt.empty:
            fig_bt = go.Figure()
            fig_bt.add_trace(go.Scatter(x=df_bt["full_dt"], y=df_bt["a_ptf"], name="Gerçekleşen", line=dict(color="#00d4ff")))
            fig_bt.add_trace(go.Scatter(x=df_bt["full_dt"], y=df_bt["p_ptf"], name="XGBoost Tahmin", line=dict(color="#ff6b35", dash="dash")))
            fig_bt.update_layout(template="plotly_dark", height=450)
            st.plotly_chart(fig_bt, use_container_width=True)
            st.info(f"💡 2026 Ortalama Hata (MAE): **{(df_bt['a_ptf'] - df_bt['p_ptf']).abs().mean():,.2f} TL**")

    with col_shap:
        st.subheader("🤖 SHAP Karar Yapısı")
        try:
            shap_df = pd.read_csv("models/ptf_shap_importance.csv")
            fig_shap = px.bar(shap_df.head(10), x='feature_importance_vals', y='col_name', orientation='h', color_discrete_sequence=['#00d4ff'])
            fig_shap.update_layout(template="plotly_dark", yaxis={'categoryorder':'total ascending'}, height=450)
            st.plotly_chart(fig_shap, use_container_width=True)
        except: st.info("SHAP verisi bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2: Fiyat Dengesizliği
# ═════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Fiyat Dengesizliği":
    st.markdown("<div class='page-header'><span class='badge'>PTF vs SMF</span><h1>Fiyat Dengesizliği Analizi</h1></div>", unsafe_allow_html=True)
    df = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.price_spread_analysis`")
    if not df.empty:
        selected_year = st.selectbox("Yıl Seç", sorted(df["year"].unique(), reverse=True))
        df_y = df[df["year"] == selected_year]
        c1, c2, c3 = st.columns(3)
        c1.metric("Ort. Makas", f"{df_y['price_spread'].mean():,.2f} TL")
        c2.metric("Enerji Açığı Saatleri", f"%{(df_y['system_direction'] == 'Enerji Açığı').mean()*100:.1f}")
        c3.metric("Maks. Makas", f"{df_y['price_spread'].max():,.2f} TL")
        st.plotly_chart(px.scatter(df_y.sample(min(2000, len(df_y))), x="ptf", y="smf", color="system_direction", template="plotly_dark"), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 6: Yenilenebilir Derinlemesine (Hatalar Giderilmiş)
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌬️ Yenilenebilir Derinlemesine":
    st.markdown("<div class='page-header'><span class='badge'>MERIT ORDER</span><h1>Yenilenebilir Enerji Derinlemesine</h1></div>", unsafe_allow_html=True)
    df = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.renewable_deep_analysis` WHERE date >= '2026-01-01'")
    
    if not df.empty:
        st.markdown("#### Kaynak Bazında PTF Korelasyonu — Mevsimsel")
        # HATASIZ MEVSİMSEL TABLO (pd.Series ve x.name düzeltmesi)
        corr_df = df.groupby("season").apply(lambda x: pd.Series({
            "Mevsim": x.name,
            "Rüzgar": round(x["wind_ratio"].corr(x["ptf"]), 3) if "wind_ratio" in x else 0,
            "Güneş": round(x["sun_ratio"].corr(x["ptf"]), 3) if "sun_ratio" in x else 0,
            "Hidrolik": round(x["hydro_ratio"].corr(x["ptf"]), 3) if "hydro_ratio" in x else 0,
            "Doğalgaz": round(x["gas_ratio"].corr(x["ptf"]), 3) if "gas_ratio" in x else 0,
        }), include_groups=False).reset_index(drop=True)
        
        st.dataframe(corr_df.style.background_gradient(subset=["Rüzgar", "Güneş", "Hidrolik", "Doğalgaz"], cmap="RdYlGn"), use_container_width=True, hide_index=True)

# ── Diğer sayfalar (Üretim Karışımı, Arz-Talep vb.) elindeki mevcut mantıkla çalışmaya devam eder.