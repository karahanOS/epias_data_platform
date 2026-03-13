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

# ── BIGQUERY CLIENT ───────────────────────────────────────────────────────────

@st.cache_resource
def get_client():
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
        <div style='font-size: 0.7rem; color: #64748b; margin-top: 4px;'>Türkiye Elektrik Piyasası</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.selectbox(
        "📊 Sayfa Seç",
        ["🏠 Executive Summary", "⚖️ Fiyat Dengesizliği", "🌱 Üretim Karışımı", "🔋 Arz-Talep Analizi"],
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

    df = query(f"""
        SELECT * FROM `{PROJECT}.{DATASET}.monthly_executive_metrics`
        ORDER BY year, month
    """)

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

        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig2, use_container_width=True)

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
            st.plotly_chart(fig, use_container_width=True)

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
            st.plotly_chart(fig2, use_container_width=True)

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
        st.plotly_chart(fig3, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig2, use_container_width=True)

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
            st.plotly_chart(fig, use_container_width=True)

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
            st.plotly_chart(fig2, use_container_width=True)

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
        st.plotly_chart(fig3, use_container_width=True)