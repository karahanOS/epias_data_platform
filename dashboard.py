import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import os

# ── CONFIG (Genel ve Bağımsız) ────────────────────────────────────────────────
st.set_page_config(page_title="EPIAŞ Analitik Platformu", page_icon="⚡", layout="wide")

PROJECT = "epias-data-platform"
DATASET = "epias_gold"

# ── DOSYA YOLU DÜZELTMESİ (FileNotFoundError Çözümü) ─────────────────────────
def get_creds_path():
    """Scriptin bulunduğu klasöre göre credentials dosyasının mutlak yolunu bulur."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "credentials", "gcp-key.json")

# ── BIGQUERY CLIENT (Güvenli ve Sağlam Bağlantı) ──────────────────────────────
@st.cache_resource
def get_client():
    path = get_creds_path()
    if os.path.exists(path):
        # Ortam değişkenini mutlak yolla set ediyoruz
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        return bigquery.Client.from_service_account_json(path)
    # Credentials dosyası yoksa varsayılan metodları dene (st.secrets vb.)
    return bigquery.Client(project=PROJECT)

def safe_query(sql):
    """Hata yakalama mekanizmalı BigQuery sorgu fonksiyonu."""
    try:
        client = get_client()
        return client.query(sql).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Veri çekme hatası: {str(e)}")
        return pd.DataFrame()

# ── SIDEBAR & NAVİGASYON ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("<h2 style='color:#00d4ff; text-align:center;'>⚡ EPIAŞ ANALİTİK</h2>", unsafe_allow_html=True)
    
    page = st.selectbox("📊 Sayfa Seç", [
        "🏠 Executive Summary", 
        "⚖️ Fiyat Dengesizliği", 
        "🌱 Üretim Karışımı", 
        "🔋 Arz-Talep Analizi", 
        "📉 Yük Tahmin Sapması", 
        "🌬️ Yenilenebilir Derinlemesine"
    ])
    
    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1: EXECUTIVE SUMMARY & ML ANALYSIS
# ═════════════════════════════════════════════════════════════════════════════
if page == "🏠 Executive Summary":
    st.title("🚀 Executive Summary")
    df_exec = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.mart_gold_monthly_executive_metrics` ORDER BY year_month")
    
    if not df_exec.empty:
        # API EXPORT (Dataseti İndirme Butonu)
        csv = df_exec.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button("📥 Dataseti CSV İndir", csv, "epias_summary_export.csv", "text/csv")

        last = df_exec.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("Ort. PTF", f"{last['avg_ptf']:,.2f} TL")
        c2.metric("Maks. PTF", f"{last['max_ptf']:,.2f} TL")
        c3.metric("Toplam Tüketim", f"{last['total_consumption']:,.0f} MWh")

        st.plotly_chart(px.line(df_exec, x="year_month", y="avg_ptf", title="Aylık PTF Trendi", template="plotly_dark"), use_container_width=True)

        # ML BACKTESTING & SHAP BÖLÜMÜ
        st.markdown("---")
        st.subheader("🎯 Model Başarımı (2026 Saatlik Backtesting)")
        c_bt, c_sh = st.columns([1.2, 0.8])
        
        with c_bt:
            h_query = f"""
                WITH cp AS (SELECT SAFE_CAST(SAFE_CAST(hour AS FLOAT64) AS INT64) as h, DATE(predicted_date) as d, AVG(predicted_ptf) as p FROM `{PROJECT}.{DATASET}.gold_ptf_predictions` WHERE predicted_date >= '2026-01-01' GROUP BY 1, 2),
                af AS (SELECT DATE(date) as d, CAST(hour_of_day AS INT64) as h, AVG(ptf) as a FROM `{PROJECT}.{DATASET}.mart_ptf_lag_features` WHERE date >= '2026-01-01' GROUP BY 1, 2)
                SELECT TIMESTAMP_ADD(TIMESTAMP(cp.d), INTERVAL cp.h HOUR) as dt, cp.p as p_ptf, af.a as a_ptf FROM cp JOIN af ON cp.d = af.d AND cp.h = af.h ORDER BY 1
            """
            df_bt = safe_query(h_query)
            if not df_bt.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df_bt["dt"], y=df_bt["a_ptf"], name="Gerçekleşen", line=dict(color="#00d4ff")))
                fig.add_trace(go.Scatter(x=df_bt["dt"], y=df_bt["p_ptf"], name="XGBoost Tahmini", line=dict(color="#ff6b35", dash="dash")))
                st.plotly_chart(fig.update_layout(template="plotly_dark", height=400), use_container_width=True)
        
        with c_sh:
            st.subheader("🤖 SHAP Karar Yapısı")
            try:
                shap_df = pd.read_csv("models/ptf_shap_importance.csv")
                st.plotly_chart(px.bar(shap_df.head(10), x='feature_importance_vals', y='col_name', orientation='h', template="plotly_dark"), use_container_width=True)
            except: st.info("SHAP verisi (ptf_shap_importance.csv) bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2: FİYAT DENGESİZLİĞİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Fiyat Dengesizliği":
    st.title("⚖️ Fiyat Dengesizliği (PTF vs SMF)")
    df = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.price_spread_analysis` LIMIT 2000")
    if not df.empty:
        st.plotly_chart(px.scatter(df, x="ptf", y="smf", color="system_direction", title="PTF vs SMF Dağılımı", template="plotly_dark"), use_container_width=True)
    else: st.warning("Veri bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3: ÜRETİM KARIŞIMI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌱 Üretim Karışımı":
    st.title("🌱 Üretim Karışımı ve Fiyat Etkisi")
    df = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.generation_mix_price_impact` LIMIT 2000")
    if not df.empty:
        st.plotly_chart(px.scatter(df, x="renewable_ratio", y="ptf", color="ptf", title="Yenilenebilir Oranı vs PTF (Merit Order)", template="plotly_dark"), use_container_width=True)
    else: st.warning("Veri bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4: ARZ-TALEP ANALİZİ
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🔋 Arz-Talep Analizi":
    st.title("🔋 Arz-Talep Analizi")
    df = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.supply_demand_summary`")
    if not df.empty:
        tod = df.groupby("time_of_day")["coverage_ratio"].mean().reset_index()
        st.plotly_chart(px.bar(tod, x="time_of_day", y="coverage_ratio", title="Saat Dilimine Göre Karşılama Oranı (%)", template="plotly_dark"), use_container_width=True)
    else: st.warning("Veri bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 5: YÜK TAHMİN SAPMASI
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📉 Yük Tahmin Sapması":
    st.title("📉 Yük Tahmin Sapması (LEP vs Gerçekleşen)")
    df = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.gold_load_vs_actual` LIMIT 168")
    if not df.empty:
        st.plotly_chart(px.line(df, x="hour", y=["forecast_consumption", "actual_consumption"], title="Haftalık Tüketim Sapma Analizi", template="plotly_dark"), use_container_width=True)
    else: st.warning("Veri bulunamadı.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 6: YENİLENEBİLİR DERİNLERİNESİNE
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🌬️ Yenilenebilir Derinlemesine":
    st.title("🌬️ Yenilenebilir Enerji Derinlemesine Analiz")
    df = safe_query(f"SELECT * FROM `{PROJECT}.{DATASET}.renewable_deep_analysis` WHERE date >= '2026-01-01'")
    if not df.empty:
        st.markdown("#### Kaynak Bazında Mevsimsel Korelasyon Tablosu")
        # HATASIZ MEVSİMSEL TABLO (pd.Series ve x.name Düzeltmesi)
        corr_df = df.groupby("season").apply(lambda x: pd.Series({
            "Mevsim": x.name,
            "Rüzgar": round(x["wind_ratio"].corr(x["ptf"]), 3) if "wind_ratio" in x else 0,
            "Güneş": round(x["sun_ratio"].corr(x["ptf"]), 3) if "sun_ratio" in x else 0,
            "Doğalgaz": round(x["gas_ratio"].corr(x["ptf"]), 3) if "gas_ratio" in x else 0,
            "Hidrolik": round(x["hydro_ratio"].corr(x["ptf"]), 3) if "hydro_ratio" in x else 0,
        }), include_groups=False).reset_index(drop=True)
        st.dataframe(corr_df.style.background_gradient(cmap="RdYlGn"), use_container_width=True, hide_index=True)
    else: st.warning("2026 verisi bulunamadı.")