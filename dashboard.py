import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

:root {
    --bg: #0a0e1a;
    --surface: #111827;
    --surface2: #1a2235;
    --accent: #00d4ff;
    --accent2: #ff6b35;
    --accent3: #7c3aed;
    --text: #e2e8f0;
    --text-muted: #64748b;
    --green: #10b981;
    --red: #ef4444;
    --border: rgba(0, 212, 255, 0.15);
}

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: var(--bg);
    color: var(--text);
}

.stApp {
    background: var(--bg);
    background-image: 
        radial-gradient(ellipse at 20% 50%, rgba(0, 212, 255, 0.05) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 20%, rgba(124, 58, 237, 0.05) 0%, transparent 50%);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border);
}

section[data-testid="stSidebar"] * {
    color: var(--text) !important;
}

/* Metric cards */
[data-testid="metric-container"] {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px;
    transition: border-color 0.2s;
}

[data-testid="metric-container"]:hover {
    border-color: var(--accent);
}

[data-testid="stMetricValue"] {
    font-family: 'Space Mono', monospace !important;
    color: var(--accent) !important;
    font-size: 1.8rem !important;
}

[data-testid="stMetricLabel"] {
    color: var(--text-muted) !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

[data-testid="stMetricDelta"] {
    font-family: 'Space Mono', monospace !important;
}

/* Headers */
h1, h2, h3 {
    font-family: 'Space Mono', monospace !important;
}

/* Selectbox & widgets */
.stSelectbox > div > div {
    background: var(--surface2) !important;
    border-color: var(--border) !important;
    color: var(--text) !important;
}

/* Divider */
hr {
    border-color: var(--border) !important;
}

/* Page title banner */
.page-header {
    background: linear-gradient(135deg, var(--surface2) 0%, rgba(0,212,255,0.05) 100%);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 24px 32px;
    margin-bottom: 24px;
    position: relative;
    overflow: hidden;
}

.page-header::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent3), var(--accent2));
}

.page-header h1 {
    margin: 0;
    font-size: 1.6rem;
    color: var(--text);
}

.page-header p {
    margin: 8px 0 0 0;
    color: var(--text-muted);
    font-size: 0.9rem;
}

.badge {
    display: inline-block;
    background: rgba(0, 212, 255, 0.1);
    border: 1px solid rgba(0, 212, 255, 0.3);
    color: var(--accent);
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-family: 'Space Mono', monospace;
    margin-right: 8px;
}
</style>
""", unsafe_allow_html=True)
# ── API EXPORT FONKSİYONU ─────────────────────────────────────────────────────
@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# ── SIDEBAR GÜNCELLEME ────────────────────────────────────────────────────────
with st.sidebar:
    # ... (Logo ve Sayfa Seçimi kısımları aynı)
    
    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
# ── BIGQUERY CLIENT ───────────────────────────────────────────────────────────

@st.cache_resource
def get_client():
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(project=PROJECT, credentials=credentials)

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
        <div style='font-size: 0.7rem; color: #64748b; margin-top: 4px;'>Türkiye Elektrik Piyasası</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.selectbox(
        "📊 Sayfa Seç",
        ["🏠 Executive Summary", "⚖️ Fiyat Dengesizliği", "🌱 Üretim Karışımı", "🔋 Arz-Talep Analizi", "📉 Yük Tahmin Sapması","🌬️ Yenilenebilir Derinlemesine"],
    label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("<div style='font-size:0.7rem; color: #64748b;'>Veri kaynağı: EPIAŞ Şeffaflık Platformu<br>Son güncelleme: Airflow DAG</div>", unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1: Executive Summary
# ═════════════════════════════════════════════════════════════════════════════

if page == "🏠 Executive Summary":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>CANLI</span>
        <h1>Executive Summary</h1>
        <p>Türkiye elektrik piyasasına genel bakış — aylık KPI'lar ve trendler</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"SELECT * FROM `{PROJECT}.{DATASET}.mart_gold_monthly_executive_metrics` ORDER BY year_month")

    if not df.empty:
        last = df.iloc[-1]
        
        # 1. API Export Butonu (Sidebar'da)
        csv = df.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="📥 Dataseti CSV Olarak İndir",
            data=csv,
            file_name=f"epias_data_{last['year_month']}.csv",
            mime='text/csv'
        )

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        # Son ay metrikleri
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            delta = round(last["avg_ptf"] - prev["avg_ptf"], 2)
            st.metric("Ort. PTF (TL/MWh)", f"{last['avg_ptf']:,.2f}", f"{delta:+.2f}")
        with col2:
            st.metric("Maks. PTF", f"{last['max_ptf']:,.2f}")
        with col3:
            total = last["total_consumption"]
            st.metric("Toplam Tüketim (MWh)", f"{total:,.0f}")
        with col4:
            spread = last.get("avg_price_spread", 0) or 0
            st.metric("Ort. Fiyat Makası", f"{spread:,.2f}")

        st.markdown("---")

        # PTF trend
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(go.Scatter(
            x=df["year_month"], y=df["avg_ptf"],
            name="Ort. PTF",
            line=dict(color="#00d4ff", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(0, 212, 255, 0.05)",
        ))

        fig.add_trace(go.Bar(
            x=df["year_month"], y=df["avg_hourly_consumption"],
            name="Saatlik Ort. Tüketim",
            marker_color="rgba(124, 58, 237, 0.4)",
            marker_line_color="rgba(124, 58, 237, 0.8)",
            marker_line_width=1,
        ), secondary_y=True)

        fig.update_layout(
            title="Aylık PTF Trendi & Tüketim",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0", family="DM Sans"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickangle=-45),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="PTF (TL/MWh)"),
            yaxis2=dict(title="Tüketim (MWh)", gridcolor="rgba(0,0,0,0)"),
            height=420,
        )

        st.plotly_chart(fig, use_container_width=True, key="chart_1")

        # Min/Max PTF band
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df["year_month"], y=df["max_ptf"],
            name="Maks PTF", line=dict(color="#ef4444", width=1.5, dash="dot")
        ))
        fig2.add_trace(go.Scatter(
            x=df["year_month"], y=df["avg_ptf"],
            name="Ort PTF", line=dict(color="#00d4ff", width=2.5),
            fill="tonexty", fillcolor="rgba(0,212,255,0.05)"
        ))
        fig2.add_trace(go.Scatter(
            x=df["year_month"], y=df["min_ptf"],
            name="Min PTF", line=dict(color="#10b981", width=1.5, dash="dot"),
            fill="tonexty", fillcolor="rgba(16,185,129,0.05)"
        ))
        fig2.update_layout(
            title="PTF Min / Ort / Maks Bandı",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0", family="DM Sans"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickangle=-45),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            height=380,
        )
        st.plotly_chart(fig2, use_container_width=True, key="chart_2")

        # 2. Model Başarımı (Backtesting) - SAATLİK VE 2026 ODAKLI
        st.markdown("---")
        st.markdown("### 🎯 Model Başarımı: Saatlik PTF Backtesting (2026)")
        
# dashboard.py içindeki hourly_query bloğunu bununla değiştirin
        hourly_query = f"""
            WITH clean_predictions AS (
                SELECT 
                    SAFE_CAST(SAFE_CAST(hour AS FLOAT64) AS INT64) as h_int,
                    DATE(predicted_date) as p_date,
                    AVG(predicted_ptf) as predicted_ptf -- Mükerrer kayıtları engellemek için
                FROM `{PROJECT}.{DATASET}.gold_ptf_predictions`
                WHERE predicted_date >= '2026-01-01'
                GROUP BY 1, 2
            ),
            actual_features AS (
                SELECT 
                    DATE(date) as f_date,
                    CAST(hour_of_day AS INT64) as h_int,
                    AVG(ptf) as actual_ptf
                FROM `{PROJECT}.{DATASET}.mart_ptf_lag_features`
                WHERE date >= '2026-01-01'
                GROUP BY 1, 2
            )
            SELECT 
                -- En sağlam zaman damgası oluşturma yöntemi:
                TIMESTAMP_ADD(TIMESTAMP(p.p_date), INTERVAL p.h_int HOUR) as full_datetime,
                p.predicted_ptf,
                f.actual_ptf
            FROM clean_predictions p
            JOIN actual_features f 
                ON p.p_date = f.f_date 
                AND p.h_int = f.h_int
            ORDER BY full_datetime
        """
        
        try:
            hourly_df = query(hourly_query)
            
            if not hourly_df.empty:
                fig_hourly = go.Figure()
            
                # Gerçekleşen PTF (Mavi)
                fig_hourly.add_trace(go.Scatter(
                    x=hourly_df["full_datetime"], 
                    y=hourly_df["actual_ptf"],
                    name="Gerçekleşen PTF (TL)", 
                    line=dict(color="#00d4ff", width=1.5)
                ))
            
                # Model Tahmini (Turuncu Kesikli)
                fig_hourly.add_trace(go.Scatter(
                    x=hourly_df["full_datetime"], 
                    y=hourly_df["predicted_ptf"],
                    name="XGBoost Tahminimiz", 
                    line=dict(color="#ff6b35", dash="dash", width=1.5)
                ))
            
                fig_hourly.update_layout(
                    title="2026 Saatlik Fiyat Tahmin Başarımı",
                    xaxis_title="Zaman (Saatlik Çözünürlük)",
                    yaxis_title="TL / MWh",
                    hovermode="x unified",
                    height=500,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#e2e8f0"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
            
                st.plotly_chart(fig_hourly, use_container_width=True)
                
                # Hata Analizi
                mae = (hourly_df["actual_ptf"] - hourly_df["predicted_ptf"]).abs().mean()
                st.info(f"💡 2026 Yılı Ortalama Tahmin Hatası (MAE): **{mae:,.2f} TL**")
            else:
                st.warning("2026 yılına ait tahmin verisi henüz mevcut değil.")
                
        except Exception as e:
            st.error(f"Sorgu çalıştırılırken bir hata oluştu. Lütfen veri tiplerini kontrol edin.")
# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2: Fiyat Dengesizliği
# ═════════════════════════════════════════════════════════════════════════════

elif page == "⚖️ Fiyat Dengesizliği":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>PTF vs SMF</span>
        <h1>Fiyat Dengesizliği Analizi</h1>
        <p>Sistem Marjinal Fiyatı ile Piyasa Takas Fiyatı arasındaki makas ve sistem yönü</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"""
        SELECT
            date,
            hour,
            ptf,
            smf,
            price_spread,
            system_direction,
            season,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(MONTH FROM date) as month
        FROM `{PROJECT}.{DATASET}.price_spread_analysis`
        ORDER BY date, hour
    """)

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        years = sorted(df["year"].unique(), reverse=True)
        selected_year = st.selectbox("Yıl Seç", years)
        df_y = df[df["year"] == selected_year]

        # KPI
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ort. Fiyat Makası", f"{df_y['price_spread'].mean():,.2f} TL")
        with col2:
            deficit_pct = (df_y["system_direction"] == "Enerji Açığı").mean() * 100
            st.metric("Enerji Açığı Saatleri", f"%{deficit_pct:.1f}")
        with col3:
            surplus_pct = (df_y["system_direction"] == "Enerji Fazlası").mean() * 100
            st.metric("Enerji Fazlası Saatleri", f"%{surplus_pct:.1f}")
        with col4:
            st.metric("Maks. Makas", f"{df_y['price_spread'].max():,.2f} TL")

        st.markdown("---")

        col_left, col_right = st.columns([2, 1])

        with col_left:
            # PTF vs SMF scatter
            fig = px.scatter(
                df_y.sample(min(3000, len(df_y))),
                x="ptf", y="smf",
                color="system_direction",
                color_discrete_map={
                    "Enerji Açığı": "#ef4444",
                    "Enerji Fazlası": "#10b981",
                    "Dengeli": "#00d4ff"
                },
                opacity=0.6,
                title="PTF vs SMF Dağılımı",
                labels={"ptf": "PTF (TL/MWh)", "smf": "SMF (TL/MWh)"}
            )
            # 45 derece çizgisi
            max_val = max(df_y["ptf"].max(), df_y["smf"].max())
            fig.add_trace(go.Scatter(
                x=[0, max_val], y=[0, max_val],
                mode="lines",
                line=dict(color="rgba(255,255,255,0.2)", dash="dash"),
                name="Denge Çizgisi"
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True, key="chart_3")

        with col_right:
            # Sistem yönü pie
            direction_counts = df_y["system_direction"].value_counts()
            fig2 = go.Figure(go.Pie(
                labels=direction_counts.index,
                values=direction_counts.values,
                hole=0.6,
                marker_colors=["#ef4444", "#10b981", "#00d4ff"],
            ))
            fig2.update_layout(
                title="Sistem Yönü Dağılımı",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                height=420,
                annotations=[dict(
                    text=f"{selected_year}",
                    x=0.5, y=0.5,
                    font_size=20,
                    font_color="#00d4ff",
                    showarrow=False
                )]
            )
            st.plotly_chart(fig2, use_container_width=True, key="chart_4")

        # Mevsimsel spread
        seasonal = df_y.groupby("season")["price_spread"].mean().reset_index()
        fig3 = px.bar(
            seasonal, x="season", y="price_spread",
            title="Mevsimsel Ortalama Fiyat Makası",
            color="price_spread",
            color_continuous_scale=["#10b981", "#00d4ff", "#7c3aed", "#ef4444"],
            labels={"price_spread": "Ort. Makas (TL/MWh)", "season": "Mevsim"}
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            coloraxis_showscale=False,
            height=350,
        )
        st.plotly_chart(fig3, use_container_width=True, key="chart_5")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3: Üretim Karışımı
# ═════════════════════════════════════════════════════════════════════════════

elif page == "🌱 Üretim Karışımı":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>MERIT ORDER</span>
        <h1>Üretim Karışımı & Fiyat Etkisi</h1>
        <p>Yenilenebilir enerji oranı arttıkça fiyatlar nasıl tepki veriyor?</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"""
        SELECT
            date,
            hour,
            total_generation,
            renewable_generation,
            fossil_generation,
            renewable_ratio,
            fossil_ratio,
            ptf,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(MONTH FROM date) as month
        FROM `{PROJECT}.{DATASET}.generation_mix_price_impact`
        ORDER BY date, hour
    """)

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        years = sorted(df["year"].unique(), reverse=True)
        selected_year = st.selectbox("Yıl Seç", years)
        df_y = df[df["year"] == selected_year]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ort. Yenilenebilir Oranı", f"%{df_y['renewable_ratio'].mean():.1f}")
        with col2:
            st.metric("Ort. Fosil Oranı", f"%{df_y['fossil_ratio'].mean():.1f}")
        with col3:
            st.metric("Ort. Toplam Üretim", f"{df_y['total_generation'].mean():,.0f} MWh")
        with col4:
            corr = df_y["renewable_ratio"].corr(df_y["ptf"])
            st.metric("Korelasyon (Yenilenebilir~PTF)", f"{corr:.3f}")

        st.markdown("---")

        # Scatter: Yenilenebilir oranı vs PTF
        fig = px.scatter(
            df_y.sample(min(3000, len(df_y))),
            x="renewable_ratio", y="ptf",
            color="ptf",
            color_continuous_scale=["#10b981", "#00d4ff", "#ef4444"],
            opacity=0.5,
            trendline="lowess",
            title="Yenilenebilir Üretim Oranı → PTF İlişkisi (Merit Order Effect)",
            labels={"renewable_ratio": "Yenilenebilir Oranı (%)", "ptf": "PTF (TL/MWh)"}
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            coloraxis_showscale=False,
            height=430,
        )
        st.plotly_chart(fig, use_container_width=True, key="chart_6")

        # Aylık yenilenebilir vs PTF trend
        monthly = df_y.groupby("month").agg(
            avg_renewable=("renewable_ratio", "mean"),
            avg_ptf=("ptf", "mean")
        ).reset_index()

        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Bar(
            x=monthly["month"], y=monthly["avg_renewable"],
            name="Ort. Yenilenebilir %",
            marker_color="rgba(16, 185, 129, 0.6)",
            marker_line_color="#10b981",
            marker_line_width=1,
        ))
        fig2.add_trace(go.Scatter(
            x=monthly["month"], y=monthly["avg_ptf"],
            name="Ort. PTF",
            line=dict(color="#00d4ff", width=2.5),
            mode="lines+markers",
            marker=dict(size=8),
        ), secondary_y=True)

        fig2.update_layout(
            title=f"{selected_year} — Aylık Yenilenebilir Oranı vs PTF",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickmode="linear"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Yenilenebilir %"),
            yaxis2=dict(title="PTF (TL/MWh)", gridcolor="rgba(0,0,0,0)"),
            height=380,
        )
        st.plotly_chart(fig2, use_container_width=True, key="chart_7")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4: Arz-Talep Analizi
# ═════════════════════════════════════════════════════════════════════════════

elif page == "🔋 Arz-Talep Analizi":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>PEAK SAATLER</span>
        <h1>Arz-Talep Analizi</h1>
        <p>Tüketim zirveleri, karşılama oranları ve günlük yük profili</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"""
        SELECT
            date,
            hour,
            time_of_day,
            total_generation,
            total_consumption,
            coverage_ratio,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(MONTH FROM date) as month
        FROM `{PROJECT}.{DATASET}.supply_demand_summary`
        ORDER BY date, hour
    """)

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        years = sorted(df["year"].unique(), reverse=True)
        selected_year = st.selectbox("Yıl Seç", years)
        df_y = df[df["year"] == selected_year]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ort. Karşılama Oranı", f"%{df_y['coverage_ratio'].mean():.1f}")
        with col2:
            st.metric("Min. Karşılama Oranı", f"%{df_y['coverage_ratio'].min():.1f}")
        with col3:
            st.metric("Maks. Tüketim", f"{df_y['total_consumption'].max():,.0f} MWh")
        with col4:
            st.metric("Maks. Üretim", f"{df_y['total_generation'].max():,.0f} MWh")

        st.markdown("---")

        # Saat dilimine göre karşılama oranı
        tod = df_y.groupby("time_of_day").agg(
            avg_coverage=("coverage_ratio", "mean"),
            avg_gen=("total_generation", "mean"),
            avg_con=("total_consumption", "mean"),
        ).reset_index()

        col_l, col_r = st.columns(2)

        with col_l:
            fig = px.bar(
                tod, x="time_of_day", y="avg_coverage",
                title="Saat Dilimine Göre Ort. Karşılama Oranı (%)",
                color="avg_coverage",
                color_continuous_scale=["#ef4444", "#ff6b35", "#10b981"],
                labels={"avg_coverage": "Karşılama %", "time_of_day": "Saat Dilimi"}
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                coloraxis_showscale=False,
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True, key="chart_8")

        with col_r:
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=tod["time_of_day"], y=tod["avg_gen"],
                name="Üretim", marker_color="rgba(0, 212, 255, 0.7)"
            ))
            fig2.add_trace(go.Bar(
                x=tod["time_of_day"], y=tod["avg_con"],
                name="Tüketim", marker_color="rgba(255, 107, 53, 0.7)"
            ))
            fig2.update_layout(
                title="Saat Dilimine Göre Ort. Üretim vs Tüketim",
                barmode="group",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                height=380,
            )
            st.plotly_chart(fig2, use_container_width=True, key="chart_9")

        # Aylık karşılama oranı trend
        monthly_cov = df_y.groupby("month")["coverage_ratio"].mean().reset_index()
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=monthly_cov["month"],
            y=monthly_cov["coverage_ratio"],
            mode="lines+markers",
            line=dict(color="#00d4ff", width=2.5),
            marker=dict(size=8, color="#00d4ff"),
            fill="tozeroy",
            fillcolor="rgba(0, 212, 255, 0.05)",
            name="Karşılama Oranı"
        ))
        fig3.add_hline(
            y=100,
            line_dash="dash",
            line_color="rgba(255,255,255,0.3)",
            annotation_text="100% Eşiği",
            annotation_font_color="#64748b"
        )
        fig3.update_layout(
            title=f"{selected_year} — Aylık Ortalama Karşılama Oranı (%)",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickmode="linear"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            height=350,
        )
        st.plotly_chart(fig3, use_container_width=True, key="chart_10")


elif page == "📉 Yük Tahmin Sapması":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>SAPMA ANALİZİ</span>
        <h1>Yük Tahmin Sapması</h1>
        <p>Tahmin edilen tüketim ile gerçekleşen tüketim arasındaki saatlik sapmalar</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"""
        SELECT
            date,
            hour,
            forecast_consumption,
            actual_consumption,
            deviation,
            deviation_pct,
            deviation_direction,
            EXTRACT(YEAR FROM date)       as year,
            EXTRACT(MONTH FROM date)      as month,
            EXTRACT(DAY FROM date)        as day,
            EXTRACT(HOUR FROM date)       as hour_num
        FROM `{PROJECT}.{DATASET}.gold_load_vs_actual`
        ORDER BY date, hour
    """)

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        years = sorted(df["year"].unique(), reverse=True)
        selected_year = st.selectbox("Yıl Seç", years)
        df_y = df[df["year"] == selected_year]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ort. Mutlak Sapma", f"{df_y['deviation'].abs().mean():,.0f} MWh")
        with col2:
            st.metric("Ort. Sapma %", f"%{df_y['deviation_pct'].abs().mean():.2f}")
        with col3:
            fazla_pct = (df_y["deviation_direction"] == "Tüketim Fazla").mean() * 100
            st.metric("Tüketim Fazla Saatleri", f"%{fazla_pct:.1f}")
        with col4:
            st.metric("Maks. Sapma", f"{df_y['deviation'].abs().max():,.0f} MWh")

        st.markdown("---")

        df_recent = df_y.sort_values("date").tail(28 * 24).copy()
        df_recent["day_label"] = df_recent["date"].dt.strftime("%d-%m")

        pivot = df_recent.pivot_table(
            index="day_label",
            columns="hour_num",
            values="deviation",
            aggfunc="mean",
            sort=False
        )

        fig_heat = go.Figure(go.Heatmap(
            z=pivot.values,
            x=[f"{int(h):02d}:00" for h in pivot.columns],
            y=pivot.index, 
            colorscale=[
                [0.0,  "#1e40af"], 
                [0.5,  "#111827"], 
                [1.0,  "#dc2626"], 
            ],
            zmid=0,
            hovertemplate="Tarih: %{y}<br>Saat: %{x}<br>Sapma: %{z:,.0f} MWh<extra></extra>",
        ))

        fig_heat.update_layout(
            title="Saatlere Göre Tüketim Sapması (Son 28 Gün)",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            xaxis=dict(title="Saat", dtick=2),
            yaxis=dict(
                title="Gün-Ay",
                type='category', 
                autorange="reversed" 
            ),
            height=700,
        )
        st.plotly_chart(fig_heat, use_container_width=True)

        col_l, col_r = st.columns(2)

        with col_l:
            hourly = df_y.groupby("hour_num").agg(
                avg_dev=("deviation", "mean"),
                avg_abs_dev=("deviation", lambda x: x.abs().mean()),
            ).reset_index()

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=hourly["hour_num"],
                y=hourly["avg_dev"],
                name="Ort. Sapma",
                marker_color=[
                    "#ef4444" if v > 0 else "#3b82f6"
                    for v in hourly["avg_dev"]
                ],
            ))
            fig2.add_hline(
                y=0, line_color="rgba(255,255,255,0.3)", line_dash="dash"
            )
            fig2.update_layout(
                title="Saate Göre Ortalama Sapma",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                xaxis=dict(
                    gridcolor="rgba(255,255,255,0.05)",
                    tickmode="linear", title="Saat"
                ),
                yaxis=dict(
                    gridcolor="rgba(255,255,255,0.05)",
                    title="Sapma (MWh)"
                ),
                height=380,
            )
            st.plotly_chart(fig2, use_container_width=True, key="chart_12")

        with col_r:
            direction_counts = df_y["deviation_direction"].value_counts()
            fig3 = go.Figure(go.Pie(
                labels=direction_counts.index,
                values=direction_counts.values,
                hole=0.6,
                marker_colors=["#ef4444", "#3b82f6", "#00d4ff"],
            ))
            fig3.update_layout(
                title="Sapma Yönü Dağılımı",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                height=380,
                annotations=[dict(
                    text=f"{selected_year}",
                    x=0.5, y=0.5,
                    font_size=20,
                    font_color="#00d4ff",
                    showarrow=False
                )]
            )
            st.plotly_chart(fig3, use_container_width=True, key="chart_13")

        monthly = df_y.groupby("month").agg(
            avg_forecast=("forecast_consumption", "mean"),
            avg_actual=("actual_consumption", "mean"),
        ).reset_index()

        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(
            x=monthly["month"], y=monthly["avg_forecast"],
            name="Tahmin (LEP)",
            line=dict(color="#7c3aed", width=2.5, dash="dash"),
            mode="lines+markers",
            marker=dict(size=7),
        ))
        fig4.add_trace(go.Scatter(
            x=monthly["month"], y=monthly["avg_actual"],
            name="Gerçekleşen",
            line=dict(color="#00d4ff", width=2.5),
            mode="lines+markers",
            marker=dict(size=7),
            fill="tonexty",
            fillcolor="rgba(0, 212, 255, 0.05)",
        ))
        fig4.update_layout(
            title=f"{selected_year} — Aylık Tahmin vs Gerçekleşen Tüketim",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e2e8f0"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            xaxis=dict(
                gridcolor="rgba(255,255,255,0.05)",
                tickmode="linear", title="Ay"
            ),
            yaxis=dict(
                gridcolor="rgba(255,255,255,0.05)",
                title="Tüketim (MWh)"
            ),
            height=380,
        )
        st.plotly_chart(fig4, use_container_width=True, key="load_monthly_fig4")


elif page == "🌬️ Yenilenebilir Derinlemesine":

    st.markdown("""
    <div class='page-header'>
        <span class='badge'>MERIT ORDER</span>
        <h1>Yenilenebilir Enerji Derinlemesine Analiz</h1>
        <p>Rüzgar, güneş ve hidrolik üretimin fiyat üzerindeki etkisi — kaynak bazında Merit Order</p>
    </div>
    """, unsafe_allow_html=True)

    df = query(f"""
        SELECT
            date,
            hour,
            time_of_day,
            season,
            total_generation,
            wind,
            sun,
            dammed_hydro,
            river,
            natural_gas,
            wind_ratio,
            sun_ratio,
            hydro_ratio,
            gas_ratio,
            coal_ratio,
            combined_renewable_ratio,
            ptf,
            EXTRACT(YEAR FROM date)  as year,
            EXTRACT(MONTH FROM date) as month
        FROM `{PROJECT}.{DATASET}.renewable_deep_analysis`
        ORDER BY date, hour
    """)

    if df.empty:
        st.warning("Veri bulunamadı.")
    else:
        years = sorted(df["year"].unique(), reverse=True)
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            selected_year = st.selectbox("Yıl Seç", years)
        with col_filter2:
            selected_season = st.selectbox(
                "Mevsim Seç",
                ["Tümü", "Kış", "İlkbahar", "Yaz", "Sonbahar"]
            )

        df_y = df[df["year"] == selected_year]
        if selected_season != "Tümü":
            df_y = df_y[df_y["season"] == selected_season]

        # KPI kartları
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            corr_wind = df_y["wind_ratio"].corr(df_y["ptf"])
            st.metric("Rüzgar-PTF Korelasyonu", f"{corr_wind:.3f}")
        with col2:
            corr_sun = df_y["sun_ratio"].corr(df_y["ptf"])
            st.metric("Güneş-PTF Korelasyonu", f"{corr_sun:.3f}")
        with col3:
            corr_hydro = df_y["hydro_ratio"].corr(df_y["ptf"])
            st.metric("Hidrolik-PTF Korelasyonu", f"{corr_hydro:.3f}")
        with col4:
            st.metric("Ort. Yenilenebilir Oranı", f"%{df_y['combined_renewable_ratio'].mean():.1f}")

        st.markdown("---")

        # Kaynak bazında scatter plotlar
        col_l, col_r = st.columns(2)

        with col_l:
            fig1 = px.scatter(
                df_y.sample(min(2000, len(df_y))),
                x="wind_ratio", y="ptf",
                color="season",
                color_discrete_map={
                    "Kış": "#3b82f6",
                    "İlkbahar": "#10b981",
                    "Yaz": "#f59e0b",
                    "Sonbahar": "#f97316"
                },
                opacity=0.5,
                trendline="lowess",
                title="🌬️ Rüzgar Oranı → PTF",
                labels={"wind_ratio": "Rüzgar Oranı (%)", "ptf": "PTF (TL/MWh)"}
            )
            fig1.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                height=380,
            )
            st.plotly_chart(fig1, use_container_width=True, key="chart_14")

        with col_r:
            fig2 = px.scatter(
                df_y.sample(min(2000, len(df_y))),
                x="hydro_ratio", y="ptf",
                color="season",
                color_discrete_map={
                    "Kış": "#3b82f6",
                    "İlkbahar": "#10b981",
                    "Yaz": "#f59e0b",
                    "Sonbahar": "#f97316"
                },
                opacity=0.5,
                trendline="lowess",
                title="💧 Hidrolik Oranı → PTF",
                labels={"hydro_ratio": "Hidrolik Oranı (%)", "ptf": "PTF (TL/MWh)"}
            )
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                height=380,
            )
            st.plotly_chart(fig2, use_container_width=True, key="chart_15")

        # Güneş etkisi — saat dilimine göre
        sun_tod = df_y.groupby("time_of_day").agg(
            avg_sun_ratio=("sun_ratio", "mean"),
            avg_ptf=("ptf", "mean"),
        ).reset_index()

        col_l2, col_r2 = st.columns(2)

        with col_l2:
            fig3 = make_subplots(specs=[[{"secondary_y": True}]])
            fig3.add_trace(go.Bar(
                x=sun_tod["time_of_day"],
                y=sun_tod["avg_sun_ratio"],
                name="Ort. Güneş %",
                marker_color="rgba(245, 158, 11, 0.7)",
                marker_line_color="#f59e0b",
                marker_line_width=1,
            ))
            fig3.add_trace(go.Scatter(
                x=sun_tod["time_of_day"],
                y=sun_tod["avg_ptf"],
                name="Ort. PTF",
                line=dict(color="#00d4ff", width=2.5),
                mode="lines+markers",
                marker=dict(size=8),
            ), secondary_y=True)
            fig3.update_layout(
                title="☀️ Güneş Üretimi ve PTF — Saat Dilimine Göre",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Güneş %"),
                yaxis2=dict(title="PTF (TL/MWh)", gridcolor="rgba(0,0,0,0)"),
                height=380,
            )
            st.plotly_chart(fig3, use_container_width=True, key="chart_16")

        with col_r2:
            # Aylık üretim karışımı stacked bar
            monthly_mix = df_y.groupby("month").agg(
                wind=("wind_ratio", "mean"),
                sun=("sun_ratio", "mean"),
                hydro=("hydro_ratio", "mean"),
                gas=("gas_ratio", "mean"),
                coal=("coal_ratio", "mean"),
            ).reset_index()

            fig4 = go.Figure()
            fig4.add_trace(go.Bar(x=monthly_mix["month"], y=monthly_mix["wind"],
                name="Rüzgar", marker_color="rgba(59, 130, 246, 0.8)"))
            fig4.add_trace(go.Bar(x=monthly_mix["month"], y=monthly_mix["sun"],
                name="Güneş", marker_color="rgba(245, 158, 11, 0.8)"))
            fig4.add_trace(go.Bar(x=monthly_mix["month"], y=monthly_mix["hydro"],
                name="Hidrolik", marker_color="rgba(16, 185, 129, 0.8)"))
            fig4.add_trace(go.Bar(x=monthly_mix["month"], y=monthly_mix["gas"],
                name="Doğalgaz", marker_color="rgba(239, 68, 68, 0.8)"))
            fig4.add_trace(go.Bar(x=monthly_mix["month"], y=monthly_mix["coal"],
                name="Kömür", marker_color="rgba(107, 114, 128, 0.8)"))

            fig4.update_layout(
                title=f"{selected_year} — Aylık Üretim Karışımı (%)",
                barmode="stack",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e2e8f0"),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)",
                           tickmode="linear", title="Ay"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="%"),
                height=380,
            )
            st.plotly_chart(fig4, use_container_width=True, key="chart_17")

        # Mevsimsel korelasyon tablosu - GÜNCELLENMİŞ
        st.markdown("#### Kaynak Bazında PTF Korelasyonu — Mevsimsel")
        
        # apply içindeki x["season"] yerine x.name kullanıyoruz
        seasonal_corr = df_y.groupby("season").apply(lambda x: pd.Series({
            "Mevsim": x.name,
            "Rüzgar": round(x["wind_ratio"].corr(x["ptf"]), 3) if not x["wind_ratio"].isnull().all() else 0,
            "Güneş": round(x["sun_ratio"].corr(x["ptf"]), 3) if not x["sun_ratio"].isnull().all() else 0,
            "Hidrolik": round(x["hydro_ratio"].corr(x["ptf"]), 3) if not x["hydro_ratio"].isnull().all() else 0,
            "Doğalgaz": round(x["gas_ratio"].corr(x["ptf"]), 3) if not x["gas_ratio"].isnull().all() else 0,
        }), include_groups=False).reset_index(drop=True)

        st.dataframe(
            seasonal_corr.style.background_gradient(
                subset=["Rüzgar", "Güneş", "Hidrolik", "Doğalgaz"],
                cmap="RdYlGn"
            ),
            use_container_width=True,
            hide_index=True,
        )

        corr_df = pd.DataFrame(seasonal_corr.tolist())
        st.dataframe(
            corr_df.style.background_gradient(
                subset=["Rüzgar", "Güneş", "Hidrolik", "Doğalgaz"],
                cmap="RdYlGn"
            ),
            use_container_width=True,
            hide_index=True,
        )