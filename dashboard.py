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

# ── CUSTOM CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
    :root { --bg: #0a0e1a; --surface: #111827; --accent: #00d4ff; --text: #e2e8f0; --border: rgba(0, 212, 255, 0.15); }
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background-color: var(--bg); color: var(--text); }
    [data-testid="stSidebar"] { background: var(--surface) !important; border-right: 1px solid var(--border); }
    [data-testid="metric-container"] { background: #1a2235; border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
    .page-header { background: linear-gradient(135deg, #1a2235 0%, rgba(0,212,255,0.05) 100%); border: 1px solid var(--border); border-radius: 16px; padding: 24px 32px; margin-bottom: 24px; position: relative; }
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

# ── SIDEBAR (API EXPORT VE NAVİGASYON) ────────────────────────────────────────
with st.sidebar:
    st.markdown("<div style='text-align:center; padding: 20px 0;'><div style='font-size:2.5rem;'>⚡</div><div style='font-family: Space Mono, monospace; color: #00d4ff;'>EPIAŞ ANALİTİK</div></div>", unsafe_allow_html=True)
    
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
# PAGE 1: EXECUTIVE SUMMARY
# ═════════════════════════════════════════════════════════════════════════════
if page == "🏠 Executive Summary":
    st.markdown("<div class='page-header'><span class='badge'>KPI</span><h1>Executive Summary</h1><p>Piyasa geneli ve ML Tahmin Başarımı</p></div>", unsafe_allow_html=True)
    
    df = query(f"SELECT * FROM `{PROJECT}.{DATASET}.mart_gold_monthly_executive_metrics` ORDER BY year_month")
    
    if not df.empty:
        # API EXPORT ÖZELLİĞİ (Geri Getirildi)
        csv = df.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(label="📥 Dataseti CSV Olarak İndir", data=csv, file_name=f"epias_export_{pd.Timestamp.now().date()}.csv", mime='text/csv')

        last, prev = df.iloc[-1], (df.iloc[-2] if len(df) > 1 else df.iloc[-1])
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ort. PTF", f"{last['avg_ptf']:,.2f} TL", f"{last['avg_ptf']-prev['avg_ptf']:+.2f}")
        c2.metric("Maks. PTF", f"{last['max_ptf']:,.2f}")
        c3.metric("Toplam Tüketim", f"{last['total_consumption']:,.0f} MWh")
        c4.metric("Ort. Makas", f"{last.get('avg_price_spread', 0):,.2f}")

        # Trend Grafiği
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=df["year_month"], y=df["avg_ptf"], name="Ort. PTF", line=dict(color="#00d4ff", width=2.5)), secondary_y=False)
        fig.add_trace(go.Bar(x=df["year_month"], y=df["avg_hourly_consumption"], name="Tüketim", marker_color="rgba(124, 58, 237, 0.3)"), secondary_y=True)
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=400)
        st.plotly_chart(fig, use_container_width=True)

        # 🎯 ML Analiz Bölümü
        st.markdown("---")
        st.subheader("🎯 2026 Saatlik Backtesting & SHAP")
        c_bt, c_sh = st.columns([1.2, 0.8])
        with c_bt:
            h_query = f"""
                WITH cp AS (SELECT SAFE_CAST(SAFE_CAST(hour AS FLOAT64) AS INT64) as h, DATE(predicted_date) as d, AVG(predicted_ptf) as p FROM `{PROJECT}.{DATASET}.gold_ptf_predictions` WHERE predicted_date >= '2026-01-01' GROUP BY 1, 2),
                af AS (SELECT DATE(date) as d, CAST(hour_of_day AS INT64) as h, AVG(ptf) as a FROM `{PROJECT}.{DATASET}.mart_ptf_lag_features` WHERE date >= '2026-01-01' GROUP BY 1, 2)
                SELECT TIMESTAMP_ADD(TIMESTAMP(cp.d), INTERVAL cp.h HOUR) as dt, cp.p as p_ptf, af.a as a_ptf FROM cp JOIN af ON cp.d = af.d AND cp.h = af.h ORDER BY 1
            """
            df_bt = query(h_query)
            if not df_bt.empty:
                fig_bt = go.Figure()
                fig_bt.add_trace(go.Scatter(x=df_bt["dt"], y=df_bt["a_ptf"], name="Gerçekleşen", line=dict(color="#00d4ff")))
                fig_bt.add_trace(go.Scatter(x=df_bt["dt"], y=df_bt["p_ptf"], name="Tahmin", line=dict(color="#ff6b35", dash="dash")))
                fig_bt.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig_bt, use_container_width=True)
        with c_sh:
            try:
                shap_df = pd.read_csv("models/ptf_shap_importance.csv")
                st.plotly_chart(px.bar(shap_df.head(10), x='feature_importance_vals', y='col_name', orientation='h', title="SHAP Etki Gücü", template="plotly_dark"), use_container_width=True)
            except: st.info("SHAP verisi henüz hazır değil.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2: FİYAT DENGESİZLİĞİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Fiyat Dengesizliği":
    st.markdown("<div class='page-header'><h1>Fiyat Dengesizliği Analizi</h1><p>PTF vs SMF ve Sistem Yönü</p></div>", unsafe_allow_html=True)
    df_spread = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.price_spread_analysis`")
    if not df_spread.empty:
        sel_year = st.selectbox("Yıl Seç", sorted(df_spread["year"].unique(), reverse=True))
        df_y = df_spread[df_spread["year"] == sel_year]
        c1, c2, c3 = st.columns(3)
        c1.metric("Ort. Makas", f"{df_y['price_spread'].mean():,.2f} TL")
        c2.metric("Enerji Açığı", f"%{(df_y['system_direction'] == 'Enerji Açığı').mean()*100:.1f}")
        c3.metric("Maks. Makas", f"{df_y['price_spread'].max():,.2f} TL")
        st.plotly_chart(px.scatter(df_y.sample(min(2000, len(df_y))), x="ptf", y="smf", color="system_direction", template="plotly_dark", height=450), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3: ÜRETİM KARIŞIMI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌱 Üretim Karışımı":
    st.markdown("<div class='page-header'><h1>Üretim Karışımı & Merit Order</h1><p>Kaynak bazlı üretim ve fiyat ilişkisi</p></div>", unsafe_allow_html=True)
    df_gen = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.generation_mix_price_impact`")
    if not df_gen.empty:
        sel_year = st.selectbox("Yıl", sorted(df_gen["year"].unique(), reverse=True))
        df_y = df_gen[df_gen["year"] == sel_year]
        st.plotly_chart(px.scatter(df_y.sample(min(2000, len(df_y))), x="renewable_ratio", y="ptf", color="ptf", trendline="lowess", template="plotly_dark", height=450), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4: ARZ-TALEP ANALİZİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🔋 Arz-Talep Analizi":
    st.markdown("<div class='page-header'><h1>Arz-Talep Analizi</h1><p>Tüketim zirveleri ve karşılama oranları</p></div>", unsafe_allow_html=True)
    df_sd = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.supply_demand_summary`")
    if not df_sd.empty:
        sel_year = st.selectbox("Yıl Seçiniz", sorted(df_sd["year"].unique(), reverse=True))
        df_y = df_sd[df_sd["year"] == sel_year]
        st.plotly_chart(px.bar(df_y.groupby("time_of_day")["coverage_ratio"].mean().reset_index(), x="time_of_day", y="coverage_ratio", color="coverage_ratio", template="plotly_dark", height=450), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 5: YÜK TAHMİN SAPMASI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📉 Yük Tahmin Sapması":
    st.markdown("<div class='page-header'><h1>Yük Tahmin Sapması</h1><p>Tahmin (LEP) vs Gerçekleşen Tüketim</p></div>", unsafe_allow_html=True)
    df_load = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.gold_load_vs_actual`")
    if not df_load.empty:
        sel_year = st.selectbox("Yıl", sorted(df_load["year"].unique(), reverse=True))
        df_y = df_load[df_load["year"] == sel_year]
        st.metric("Ort. Sapma %", f"%{df_y['deviation_pct'].abs().mean():.2f}")
        st.plotly_chart(px.line(df_y.tail(168), x="hour", y=["forecast_consumption", "actual_consumption"], template="plotly_dark", height=450), use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 6: YENİLENEBİLİR DERİNLERİNESİNE
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌬️ Yenilenebilir Derinlemesine":
    st.markdown("<div class='page-header'><h1>Yenilenebilir Enerji Derinlemesine</h1><p>Kaynak bazlı Merit Order etkisi</p></div>", unsafe_allow_html=True)
    df_deep = query(f"SELECT *, EXTRACT(YEAR FROM date) as year FROM `{PROJECT}.{DATASET}.renewable_deep_analysis` WHERE date >= '2026-01-01'")
    if not df_deep.empty:
        # MEVSİMSEL KORELASYON (KeyError/AttributeError Fix)
        st.markdown("#### Kaynak Bazında PTF Korelasyonu — Mevsimsel")
        corr_df = df_deep.groupby("season").apply(lambda x: pd.Series({
            "Mevsim": x.name,
            "Rüzgar": round(x["wind_ratio"].corr(x["ptf"]), 3) if "wind_ratio" in x else 0,
            "Güneş": round(x["sun_ratio"].corr(x["ptf"]), 3) if "sun_ratio" in x else 0,
            "Hidrolik": round(x["hydro_ratio"].corr(x["ptf"]), 3) if "hydro_ratio" in x else 0,
            "Doğalgaz": round(x["gas_ratio"].corr(x["ptf"]), 3) if "gas_ratio" in x else 0,
        }), include_groups=False).reset_index(drop=True)
        st.dataframe(corr_df.style.background_gradient(subset=["Rüzgar", "Güneş", "Hidrolik", "Doğalgaz"], cmap="RdYlGn"), use_container_width=True, hide_index=True)