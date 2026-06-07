"""
dashboard.py — EPIAŞ Elektrik Piyasası Analitik Paneli
=======================================================
Tüm dbt Gold mart tablolarını kapsayan interaktif Streamlit dashboard.

Sayfalar:
  1. 🏠 Executive Summary        — mart_gold_monthly_executive_metrics
  2. ⚖️  Fiyat Analizi            — mart_price_analysis
  3. 🌱 Üretim & Yenilenebilir   — mart_generation_mix + mart_renawable_impact + mart_renewable_deep
  4. 📊 GÖP Piyasa Hacimleri     — mart_gop_volume_analysis + mart_merit_order
  5. 🔋 Arz-Talep & Residual Yük — mart_supply_demand + mart_forecasted_residual_load + mart_ptf_drivers
  6. 🚨 Arz Şoku & Risk          — mart_supply_shock_index
  7. 🤖 PTF Tahmin & ML          — mart_ptf_lag_features + gold_ptf_predictions
  8. 🌿 Lisanssız Üretim (YEKDEM)— mart_unlicensed_impact
"""

import os
import sys
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from google.cloud import bigquery
from google.api_core.exceptions import NotFound as BQNotFound

logger = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
# Import shared GCP config so dashboard stays in sync with the pipeline.
# Falls back to env-var defaults when running outside the container (local dev).
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
    from config import GCP_PROJECT_ID as PROJECT, BQ_GOLD_DATASET as DATASET
except ImportError:
    PROJECT = os.getenv("GCP_PROJECT_ID", "epias-data-platform")
    DATASET = os.getenv("BQ_GOLD_DATASET", "epias_gold")

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EPIAŞ Analitik",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── THEME CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;700&display=swap');
:root {
    --bg:#0a0e1a; --surface:#111827; --surface2:#1a2235;
    --accent:#00d4ff; --accent2:#ff6b35; --accent3:#7c3aed;
    --text:#e2e8f0; --muted:#64748b;
    --green:#10b981; --red:#ef4444; --border:rgba(0,212,255,0.15);
}
html,body,[class*="css"]{ font-family:'DM Sans',sans-serif; background:var(--bg); color:var(--text); }
.stApp{
    background:var(--bg);
    background-image:
        radial-gradient(ellipse at 20% 50%,rgba(0,212,255,0.05) 0%,transparent 50%),
        radial-gradient(ellipse at 80% 20%,rgba(124,58,237,0.05) 0%,transparent 50%);
}
section[data-testid="stSidebar"]{ background:var(--surface)!important; border-right:1px solid var(--border); }
section[data-testid="stSidebar"] *{ color:var(--text)!important; }
[data-testid="metric-container"]{
    background:var(--surface2); border:1px solid var(--border);
    border-radius:12px; padding:16px; transition:border-color .2s;
}
[data-testid="metric-container"]:hover{ border-color:var(--accent); }
[data-testid="stMetricValue"]{ font-family:'Space Mono',monospace!important; color:var(--accent)!important; font-size:1.8rem!important; }
[data-testid="stMetricLabel"]{ color:var(--muted)!important; font-size:.75rem!important; text-transform:uppercase; letter-spacing:.1em; }
[data-testid="stMetricDelta"]{ font-family:'Space Mono',monospace!important; }
h1,h2,h3{ font-family:'Space Mono',monospace!important; }
.stSelectbox>div>div{ background:var(--surface2)!important; border-color:var(--border)!important; color:var(--text)!important; }
hr{ border-color:var(--border)!important; }
.page-header{
    background:linear-gradient(135deg,var(--surface2) 0%,rgba(0,212,255,.05) 100%);
    border:1px solid var(--border); border-radius:16px;
    padding:24px 32px; margin-bottom:24px; position:relative; overflow:hidden;
}
.page-header::before{
    content:''; position:absolute; top:0;left:0;right:0; height:2px;
    background:linear-gradient(90deg,var(--accent),var(--accent3),var(--accent2));
}
.page-header h1{ margin:0; font-size:1.6rem; color:var(--text); }
.page-header p{ margin:8px 0 0 0; color:var(--muted); font-size:.9rem; }
.badge{
    display:inline-block; background:rgba(0,212,255,.1);
    border:1px solid rgba(0,212,255,.3); color:var(--accent);
    padding:2px 10px; border-radius:20px;
    font-size:.75rem; font-family:'Space Mono',monospace; margin-right:8px;
}
</style>
""", unsafe_allow_html=True)

# ── PLOTLY LAYOUT DEFAULTS ────────────────────────────────────────────────────
# xaxis / yaxis are intentionally omitted here.  Including them caused
# "TypeError: multiple values for keyword argument 'xaxis'" whenever a caller
# passed xaxis=dict(...) to dark() or update_layout(**DARK_LAYOUT, xaxis=...).
DARK_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#e2e8f0", family="DM Sans"),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

_DARK_AXIS = dict(gridcolor="rgba(255,255,255,0.05)")

MONTHS_TR = {1:"Oca",2:"Şub",3:"Mar",4:"Nis",5:"May",6:"Haz",
             7:"Tem",8:"Ağu",9:"Eyl",10:"Eki",11:"Kas",12:"Ara"}

# BigQuery EXTRACT(DAYOFWEEK) → 1=Sun … 7=Sat
DOW_TR = {1:"Paz", 2:"Pzt", 3:"Sal", 4:"Çar", 5:"Per", 6:"Cum", 7:"Cmt"}

def dark(fig, height=420, **extra):
    """Apply dark theme.  Caller xaxis/yaxis dicts are merged with dark defaults."""
    xaxis = {**_DARK_AXIS, **extra.pop("xaxis", {})}
    yaxis = {**_DARK_AXIS, **extra.pop("yaxis", {})}
    fig.update_layout(**DARK_LAYOUT, height=height, xaxis=xaxis, yaxis=yaxis, **extra)
    return fig

# ── BIGQUERY ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    try:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(project=PROJECT, credentials=creds)
    except Exception:
        return bigquery.Client(project=PROJECT)

@st.cache_data(ttl=3600, show_spinner="BigQuery sorgulanıyor...")
def query(sql: str) -> pd.DataFrame:
    try:
        df = get_client().query(sql).to_dataframe()
        # BigQuery Storage API returns pandas nullable extension types (Float64, Int64).
        # On all-null columns, .sum()/.mean() return pd.NA — not np.nan.
        # f"{pd.NA:,.1f}" raises TypeError; converting to numpy float64 fixes this
        # (pd.NA → np.nan, and np.nan formats safely as "nan").
        for col in df.select_dtypes(include="number").columns:
            if pd.api.types.is_extension_array_dtype(df[col].dtype):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except BQNotFound:
        # Table doesn't exist yet — expected for tables populated by scheduled jobs
        # (e.g. gold_ptf_predictions before the first inference run).
        # Return empty silently; callers show context-appropriate messages.
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Sorgu hatası: {e}")
        return pd.DataFrame()

def tbl(mart: str) -> str:
    return f"`{PROJECT}.{DATASET}.{mart}`"

# ── DATA FRESHNESS ────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_last_updated() -> str:
    """Return the most recent date available in mart_price_analysis."""
    df = query(f"SELECT MAX(date) AS last_date FROM {tbl('mart_price_analysis')}")
    if df.empty or pd.isna(df["last_date"].iloc[0]):
        return "Bilinmiyor"
    return pd.to_datetime(df["last_date"].iloc[0]).strftime("%Y-%m-%d")

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:16px 0 24px 0;'>
        <div style='font-size:2.5rem;'>⚡</div>
        <div style='font-family:Space Mono,monospace;font-size:.9rem;color:#00d4ff;margin-top:8px;'>EPIAŞ ANALİTİK</div>
        <div style='font-size:.7rem;color:#64748b;margin-top:4px;'>Türkiye Elektrik Piyasası</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.selectbox("📊 Sayfa", [
        "🏠 Executive Summary",
        "⚖️ Fiyat Analizi",
        "🌱 Üretim & Yenilenebilir",
        "📊 GÖP Piyasa Hacimleri",
        "🔋 Arz-Talep & Residual Yük",
        "🚨 Arz Şoku & Risk",
        "🤖 PTF Tahmin & ML",
        "🌿 Lisanssız Üretim (YEKDEM)",
        "⚡ GİP & Hava Durumu",
        "🏭 Üretim Planı (BGÜP vs KGÜP)",
        "🔥 PTF Tavan & Minimum Analizi",
        "📈 Çapraz Piyasa Arbitraj",
    ], label_visibility="collapsed")

    st.markdown("---")

    # ── GLOBAL YEAR & MONTH FILTERS ─────────────────────────────────────────
    # Both stored in session_state and read below as `sel_year` / `sel_month`.
    # Changing either widget triggers a full rerender so all page SQL queries
    # instantly reflect the new selection.
    _now_year = pd.Timestamp.now().year
    _year_opts = [_now_year, _now_year - 1, _now_year - 2, _now_year - 3]
    st.selectbox("📅 Analiz Yılı", _year_opts, index=0, key="sel_year")

    _month_labels = ["Tümü"] + list(MONTHS_TR.values())  # ["Tümü","Oca",…,"Ara"]
    st.selectbox("📆 Ay", _month_labels, index=0, key="sel_month")

    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        # query.clear() invalidates only BQ cache — @st.cache_resource (BQ client)
        # is intentionally preserved so the connection is not re-created on refresh.
        query.clear()
        st.rerun()

    # Data freshness indicator
    _last_updated = get_last_updated()
    st.markdown(
        f"<div style='font-size:.7rem;color:#64748b;margin-top:8px;'>"
        f"🕐 Son veri: <b style='color:#10b981'>{_last_updated}</b><br>"
        f"Kaynak: EPIAŞ Şeffaflık Platformu<br>Pipeline: Airflow → Spark → dbt → BQ"
        f"</div>", unsafe_allow_html=True
    )

# ── GLOBAL YEAR + MONTH (read from sidebar widgets via session_state) ────────
_MONTHS_TR_INV: dict[str, int] = {v: k for k, v in MONTHS_TR.items()}
sel_year: int       = st.session_state.get("sel_year", pd.Timestamp.now().year)
sel_month: int|None = _MONTHS_TR_INV.get(st.session_state.get("sel_month", "Tümü"))

# SQL fragments appended after the year WHERE clause.
# Empty string when "Tümü" (all months) is selected.
#   _month_filter    → for columns named `date`
#   _month_filter_td → for columns named `trade_date` (Page 9 GİP query)
_month_filter    = f" AND EXTRACT(MONTH FROM date) = {sel_month}"       if sel_month else ""
_month_filter_td = f" AND EXTRACT(MONTH FROM trade_date) = {sel_month}" if sel_month else ""

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Executive Summary":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>KPI</span>
        <h1>Executive Summary</h1>
        <p>Türkiye elektrik piyasasına aylık bakış — PTF, tüketim ve sistem yönü</p>
    </div>""", unsafe_allow_html=True)

    # Fetch all years — needed for YoY chart and all-time trend
    df = query(f"SELECT * FROM {tbl('mart_gold_monthly_executive_metrics')} ORDER BY year, month")
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    # CSV download
    st.sidebar.download_button("📥 CSV İndir", df.to_csv(index=False).encode(),
                               "epias_executive.csv", "text/csv")

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Ort. PTF (TL/MWh)", f"{last['avg_ptf']:,.2f}",
              f"{last['avg_ptf']-prev['avg_ptf']:+.2f}")
    c2.metric("Maks PTF", f"{last['max_ptf']:,.2f}")
    c3.metric("Min PTF",  f"{last['min_ptf']:,.2f}")
    c4.metric("Ort Fiyat Makası", f"{last['avg_price_spread']:,.2f}")
    c5.metric("Enerji Açığı (saat)", f"{int(last['energy_deficit_hours']):,}",
              f"{int(last['energy_deficit_hours']-prev['energy_deficit_hours']):+d}")

    st.markdown("---")

    # PTF band (min/avg/max)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["year_month"], y=df["max_ptf"],
        name="Maks PTF", line=dict(color="#ef4444", width=1.5, dash="dot")))
    fig.add_trace(go.Scatter(x=df["year_month"], y=df["avg_ptf"],
        name="Ort PTF", line=dict(color="#00d4ff", width=2.5),
        fill="tonexty", fillcolor="rgba(0,212,255,0.06)"))
    fig.add_trace(go.Scatter(x=df["year_month"], y=df["min_ptf"],
        name="Min PTF", line=dict(color="#10b981", width=1.5, dash="dot"),
        fill="tonexty", fillcolor="rgba(16,185,129,0.04)"))
    dark(fig, title="Aylık PTF Bandı (Min / Ort / Maks)",
         xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickangle=-45))
    st.plotly_chart(fig, use_container_width=True, key="ex_band")

    col_l, col_r = st.columns(2)

    with col_l:
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Bar(x=df["year_month"], y=df["energy_deficit_hours"],
            name="Enerji Açığı (saat)", marker_color="rgba(239,68,68,0.6)"))
        fig2.add_trace(go.Bar(x=df["year_month"], y=df["energy_surplus_hours"],
            name="Enerji Fazlası (saat)", marker_color="rgba(16,185,129,0.6)"))
        fig2.update_layout(**DARK_LAYOUT, barmode="group", height=380,
            title="Aylık Sistem Yönü Dağılımı",
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickangle=-45))
        st.plotly_chart(fig2, use_container_width=True, key="ex_dir")

    with col_r:
        fig3 = go.Figure(go.Scatter(
            x=df["year_month"], y=df["avg_price_spread"],
            mode="lines+markers", name="Fiyat Makası",
            line=dict(color="#ff6b35", width=2.5),
            fill="tozeroy", fillcolor="rgba(255,107,53,0.07)"))
        fig3.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
        dark(fig3, height=380, title="Aylık Ortalama Fiyat Makası (PTF - SMF)",
             xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickangle=-45))
        st.plotly_chart(fig3, use_container_width=True, key="ex_spread")

    # Season heatmap
    if "season" in df.columns and "year" in df.columns:
        pivot = df.pivot_table(index="season", columns="year",
                               values="avg_ptf", aggfunc="mean")
        fig4 = px.imshow(pivot, color_continuous_scale="Blues",
                         text_auto=".0f", title="Mevsim × Yıl Ort. PTF Isı Haritası")
        dark(fig4, height=300, coloraxis_colorbar=dict(title="TL/MWh"))
        st.plotly_chart(fig4, use_container_width=True, key="ex_heat")

    # ── YoY COMPARISON CHART ──────────────────────────────────────────────────
    df_curr = df[df["year"] == sel_year]     if "year" in df.columns else pd.DataFrame()
    df_prev = df[df["year"] == sel_year - 1] if "year" in df.columns else pd.DataFrame()
    if sel_month and "month" in df.columns:
        df_curr = df_curr[df_curr["month"] == sel_month]
        df_prev = df_prev[df_prev["month"] == sel_month]

    if not df_curr.empty and not df_prev.empty:
        st.markdown(f"### 📅 Yıllık Karşılaştırma — {sel_year} vs {sel_year - 1}")
        col_yoy_l, col_yoy_r = st.columns(2)

        with col_yoy_l:
            fig_yoy = go.Figure()
            fig_yoy.add_trace(go.Scatter(
                x=df_curr["month"], y=df_curr["avg_ptf"],
                name=str(sel_year), mode="lines+markers",
                line=dict(color="#00d4ff", width=2.5)))
            fig_yoy.add_trace(go.Scatter(
                x=df_prev["month"], y=df_prev["avg_ptf"],
                name=str(sel_year - 1), mode="lines+markers",
                line=dict(color="#7c3aed", width=2.5, dash="dash")))
            dark(fig_yoy, height=360,
                 title=f"Aylık Ort. PTF: {sel_year} vs {sel_year - 1}",
                 xaxis=dict(title="Ay", tickmode="linear",
                            gridcolor="rgba(255,255,255,0.05)"),
                 yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"))
            st.plotly_chart(fig_yoy, use_container_width=True, key="ex_yoy_ptf")

        with col_yoy_r:
            fig_yoy2 = go.Figure()
            fig_yoy2.add_trace(go.Bar(
                x=df_curr["month"], y=df_curr["energy_deficit_hours"],
                name=str(sel_year), marker_color="rgba(0,212,255,0.7)"))
            fig_yoy2.add_trace(go.Bar(
                x=df_prev["month"], y=df_prev["energy_deficit_hours"],
                name=str(sel_year - 1), marker_color="rgba(124,58,237,0.5)"))
            dark(fig_yoy2, height=360, barmode="group",
                 title=f"Enerji Açığı (saat): {sel_year} vs {sel_year - 1}",
                 xaxis=dict(title="Ay", tickmode="linear",
                            gridcolor="rgba(255,255,255,0.05)"),
                 yaxis=dict(title="Saat", gridcolor="rgba(255,255,255,0.05)"))
            st.plotly_chart(fig_yoy2, use_container_width=True, key="ex_yoy_deficit")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — FİYAT ANALİZİ
# ══════════════════════════════════════════════════════════════════════════════
elif page == "⚖️ Fiyat Analizi":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>PTF vs SMF</span>
        <h1>Fiyat Analizi</h1>
        <p>Saatlik PTF, SMF ve fiyat makası — sistem yönü ve mevsimsel örüntüler</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, hour, ptf_try, smf_try, price_spread, season,
               EXTRACT(YEAR FROM date)      AS year,
               EXTRACT(MONTH FROM date)     AS month,
               EXTRACT(DAYOFWEEK FROM date) AS day_of_week
        FROM {tbl('mart_price_analysis')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    st.sidebar.download_button("📥 CSV İndir", df.to_csv(index=False).encode(),
                               "epias_price.csv", "text/csv")

    seasons_avail = ["Tümü"] + list(df["season"].dropna().unique())
    sel_season = st.selectbox("Mevsim", seasons_avail, key="p2_season")

    dfy = df.copy()
    if sel_season != "Tümü":
        dfy = dfy[dfy["season"] == sel_season]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort. PTF", f"{dfy['ptf_try'].mean():,.2f} TL")
    c2.metric("Ort. SMF", f"{dfy['smf_try'].mean():,.2f} TL")
    c3.metric("Ort. Makas", f"{dfy['price_spread'].mean():,.2f} TL")
    c4.metric("Maks Makas", f"{dfy['price_spread'].max():,.2f} TL")

    st.markdown("---")

    # PTF vs SMF scatter
    col_l, col_r = st.columns([2, 1])
    with col_l:
        fig = px.scatter(dfy.sample(min(4000, len(dfy))),
            x="ptf_try", y="smf_try", color="season",
            opacity=0.5, trendline="ols",
            title="PTF vs SMF Saçılım",
            labels={"ptf_try": "PTF (TL/MWh)", "smf_try": "SMF (TL/MWh)"})
        mv = max(dfy["ptf_try"].max(), dfy["smf_try"].max())
        fig.add_shape(type="line", x0=0, y0=0, x1=mv, y1=mv,
                      line=dict(color="rgba(255,255,255,0.2)", dash="dash"))
        dark(fig)
        st.plotly_chart(fig, use_container_width=True, key="pr_scatter")

    with col_r:
        season_spread = dfy.groupby("season")["price_spread"].mean().reset_index()
        fig2 = px.bar(season_spread, x="season", y="price_spread",
            color="price_spread",
            color_continuous_scale=["#10b981","#00d4ff","#7c3aed","#ef4444"],
            title="Mevsimsel Ort. Makas",
            labels={"price_spread": "Makas (TL/MWh)", "season": ""})
        dark(fig2, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True, key="pr_season")

    # Hourly average PTF/SMF profile
    hourly = dfy.groupby("hour").agg(
        avg_ptf=("ptf_try", "mean"), avg_smf=("smf_try", "mean"),
        avg_spread=("price_spread", "mean")).reset_index()

    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    fig3.add_trace(go.Scatter(x=hourly["hour"], y=hourly["avg_ptf"],
        name="Ort PTF", line=dict(color="#00d4ff", width=2.5)))
    fig3.add_trace(go.Scatter(x=hourly["hour"], y=hourly["avg_smf"],
        name="Ort SMF", line=dict(color="#7c3aed", width=2.5)))
    fig3.add_trace(go.Bar(x=hourly["hour"], y=hourly["avg_spread"],
        name="Ort Makas", marker_color="rgba(255,107,53,0.5)"),
        secondary_y=True)
    fig3.update_layout(**DARK_LAYOUT, height=380, barmode="overlay",
        title="Saatlik Ortalama PTF / SMF / Makas Profili",
        xaxis=dict(title="Saat", gridcolor="rgba(255,255,255,0.05)", tickmode="linear"),
        yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"),
        yaxis2=dict(title="Makas", gridcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig3, use_container_width=True, key="pr_hourly")

    # Spread distribution
    fig4 = px.histogram(dfy, x="price_spread", nbins=80,
        color="season", barmode="overlay",
        title="Fiyat Makası Dağılımı (Histogram)",
        labels={"price_spread": "Makas (TL/MWh)"}, opacity=0.7)
    dark(fig4, height=360)
    st.plotly_chart(fig4, use_container_width=True, key="pr_hist")

    # ── HOUR × DAY-OF-WEEK PTF HEATMAP ───────────────────────────────────────
    if "day_of_week" in dfy.columns:
        st.markdown("### 🗓️ Saat × Haftanın Günü PTF Isı Haritası")
        heat_df = dfy.groupby(["day_of_week", "hour"])["ptf_try"].mean().reset_index()
        heat_df["gun"] = heat_df["day_of_week"].map(DOW_TR)
        pivot_heat = heat_df.pivot(index="gun", columns="hour", values="ptf_try")
        # Reorder rows Mon→Sun (Pzt…Paz)
        _dow_order = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]
        pivot_heat = pivot_heat.reindex([d for d in _dow_order if d in pivot_heat.index])
        fig_heat = px.imshow(
            pivot_heat,
            color_continuous_scale=["#0a0e1a", "#00d4ff", "#ff6b35", "#ef4444"],
            text_auto=".0f",
            title=f"{sel_year} — Saatlik Ort. PTF (TL/MWh)",
            labels={"x": "Saat", "y": "Gün", "color": "PTF (TL/MWh)"},
            aspect="auto",
        )
        fig_heat.update_layout(**DARK_LAYOUT, height=340,
            coloraxis_colorbar=dict(title="TL/MWh"))
        st.plotly_chart(fig_heat, use_container_width=True, key="pr_dow_heat")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — ÜRETİM & YENİLENEBİLİR
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🌱 Üretim & Yenilenebilir":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>MERIT ORDER</span>
        <h1>Üretim Karışımı & Yenilenebilir Etki</h1>
        <p>Yenilenebilir/fosil oranı, rüzgar tahmini sapması ve yeşil enerji residual yük</p>
    </div>""", unsafe_allow_html=True)

    with st.spinner("Üretim verileri yükleniyor..."):
        df_mix = query(f"""
            SELECT date, hour, total_generation, renewable_ratio, fossil_ratio,
                   EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
            FROM {tbl('mart_generation_mix')}
            WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
            ORDER BY date, hour
        """)
        df_ren = query(f"""
            SELECT date, hour, ptf_try, licensed_renewable_mwh, total_unlicensed_mwh,
                   total_green_energy_mwh, residual_load_mwh, total_demand_mwh,
                   SAFE_DIVIDE(total_green_energy_mwh, total_demand_mwh) AS renewable_ratio,
                   EXTRACT(YEAR FROM date) AS year
            FROM {tbl('mart_renawable_impact')}
            WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
            ORDER BY date, hour
        """)
        df_deep = query(f"""
            SELECT date, hour, wind_generation_mwh, solar_generation_mwh,
                   forecasted_res_mwh, wind_forecast_error,
                   EXTRACT(YEAR FROM date) AS year
            FROM {tbl('mart_renewable_deep')}
            WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
            ORDER BY date, hour
        """)

    if df_mix.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy      = df_mix
    dfy_ren  = df_ren  if not df_ren.empty  else pd.DataFrame()
    dfy_deep = df_deep if not df_deep.empty else pd.DataFrame()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort. Yenilenebilir %", f"%{dfy['renewable_ratio'].mean()*100:.1f}")
    c2.metric("Ort. Fosil %", f"%{dfy['fossil_ratio'].mean()*100:.1f}")
    c3.metric("Ort. Toplam Üretim", f"{dfy['total_generation'].mean():,.0f} MWh")
    if not dfy_ren.empty:
        corr = dfy_ren["renewable_ratio"].corr(dfy_ren["ptf_try"]) if "renewable_ratio" in dfy_ren.columns else None
        if corr is not None:
            c4.metric("Yenilenebilir~PTF Korel.", f"{corr:.3f}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Renewable ratio vs PTF
        if not dfy_ren.empty and "ptf_try" in dfy_ren.columns:
            fig = px.scatter(dfy_ren.sample(min(3000, len(dfy_ren))),
                x="renewable_ratio", y="ptf_try",
                color="ptf_try", color_continuous_scale=["#10b981","#00d4ff","#ef4444"],
                opacity=0.5, trendline="lowess",
                title="🌿 Yenilenebilir Oranı → PTF (Merit Order Effect)",
                labels={"renewable_ratio": "Yenilenebilir Oranı", "ptf_try": "PTF (TL/MWh)"})
            dark(fig, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True, key="ren_scatter")

    with col_r:
        # Monthly stack renewable/fossil
        monthly = dfy.groupby("month").agg(
            ren=("renewable_ratio", "mean"), fos=("fossil_ratio", "mean")).reset_index()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=monthly["month"], y=monthly["ren"]*100,
            name="Yenilenebilir %", marker_color="rgba(16,185,129,0.8)"))
        fig2.add_trace(go.Bar(x=monthly["month"], y=monthly["fos"]*100,
            name="Fosil %", marker_color="rgba(239,68,68,0.7)"))
        dark(fig2, height=380, title=f"{sel_year} Aylık Üretim Karışımı",
             barmode="stack",
             xaxis=dict(title="Ay", gridcolor="rgba(255,255,255,0.05)", tickmode="linear"),
             yaxis=dict(title="%", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig2, use_container_width=True, key="ren_stack")

    # Wind forecast error
    if not dfy_deep.empty and "wind_forecast_error" in dfy_deep.columns:
        col_l2, col_r2 = st.columns(2)
        with col_l2:
            fig3 = px.histogram(dfy_deep, x="wind_forecast_error", nbins=60,
                color_discrete_sequence=["#00d4ff"],
                title="🌬️ Rüzgar Üretim Tahmin Sapması Dağılımı",
                labels={"wind_forecast_error": "Sapma (MWh)"}, opacity=0.8)
            dark(fig3, height=360)
            st.plotly_chart(fig3, use_container_width=True, key="ren_wferr")

        with col_r2:
            # Residual load trend
            if not dfy_ren.empty and "residual_load_mwh" in dfy_ren.columns:
                daily_res = dfy_ren.groupby("date")["residual_load_mwh"].mean().reset_index()
                fig4 = go.Figure(go.Scatter(
                    x=daily_res["date"], y=daily_res["residual_load_mwh"],
                    mode="lines", line=dict(color="#7c3aed", width=1.5),
                    fill="tozeroy", fillcolor="rgba(124,58,237,0.07)",
                    name="Residual Yük"))
                dark(fig4, height=360, title="🔋 Günlük Ortalama Residual Yük (Talep - Yeşil Enerji)")
                st.plotly_chart(fig4, use_container_width=True, key="ren_residual")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — GÖP PİYASA HACİMLERİ
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 GÖP Piyasa Hacimleri":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>DAM</span>
        <h1>GÖP Piyasa Hacimleri & Merit Order</h1>
        <p>Gün öncesi piyasa eşleşme hacimleri, işlem değerleri ve arz eğrisi analizi</p>
    </div>""", unsafe_allow_html=True)

    df_vol = query(f"""
        SELECT date, hour, total_buy_mwh, total_sell_mwh, ptf_try, market_volume_try,
               EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_gop_volume_analysis')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)
    df_mo = query(f"""
        SELECT date, bid_offer_price_try, cumulative_supply_mwh,
               cumulative_demand_mwh, ptf_try, supply_status
        FROM {tbl('mart_merit_order')}
        WHERE date = (SELECT MAX(date) FROM {tbl('mart_merit_order')})
        ORDER BY bid_offer_price_try
    """)

    if df_vol.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy = df_vol

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort. Alış Hacmi", f"{dfy['total_buy_mwh'].mean():,.0f} MWh")
    c2.metric("Ort. Satış Hacmi", f"{dfy['total_sell_mwh'].mean():,.0f} MWh")
    c3.metric("Toplam İşlem Değ.", f"₺{dfy['market_volume_try'].sum()/1e9:.2f} Mrd")
    c4.metric("Ort. PTF", f"{dfy['ptf_try'].mean():,.2f} TL/MWh")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Daily market volume trend
        daily_vol = dfy.groupby("date")["market_volume_try"].sum().reset_index()
        fig = go.Figure(go.Bar(x=daily_vol["date"], y=daily_vol["market_volume_try"]/1e6,
            marker_color="rgba(0,212,255,0.6)", name="İşlem Değeri"))
        dark(fig, height=380, title="Günlük GÖP İşlem Değeri (Milyon TL)",
             yaxis=dict(title="Milyon TL", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig, use_container_width=True, key="gop_vol")

    with col_r:
        # Hourly buy/sell profile
        hourly = dfy.groupby("hour").agg(
            buy=("total_buy_mwh","mean"), sell=("total_sell_mwh","mean")).reset_index()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=hourly["hour"], y=hourly["buy"],
            name="Ort Alış", marker_color="rgba(0,212,255,0.7)"))
        fig2.add_trace(go.Bar(x=hourly["hour"], y=hourly["sell"],
            name="Ort Satış", marker_color="rgba(255,107,53,0.7)"))
        dark(fig2, height=380, title="Saatlik Ort. Alış/Satış Hacmi (MWh)", barmode="group",
             xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
             yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig2, use_container_width=True, key="gop_hourly")

    # Merit order curve (most recent day)
    if not df_mo.empty:
        st.subheader("📈 Merit Order Eğrisi — En Son Gün")
        col_m_l, col_m_r = st.columns([2,1])
        with col_m_l:
            ptf_val = df_mo["ptf_try"].iloc[0] if "ptf_try" in df_mo.columns else None
            fig3 = go.Figure()
            in_merit = df_mo[df_mo["supply_status"] == "In Merit (Eşleşti)"]
            out_merit = df_mo[df_mo["supply_status"] == "Out of Merit (Eşleşmedi)"]
            fig3.add_trace(go.Scatter(
                x=in_merit["cumulative_supply_mwh"], y=in_merit["bid_offer_price_try"],
                mode="lines", name="Eşleşti", line=dict(color="#10b981", width=2)))
            fig3.add_trace(go.Scatter(
                x=out_merit["cumulative_supply_mwh"], y=out_merit["bid_offer_price_try"],
                mode="lines", name="Eşleşmedi", line=dict(color="#ef4444", width=2)))
            _demand = df_mo.dropna(subset=["cumulative_demand_mwh"])
            fig3.add_trace(go.Scatter(
                x=_demand["cumulative_demand_mwh"],
                y=_demand["bid_offer_price_try"],
                mode="lines", name="Talep Eğrisi",
                line=dict(color="#7c3aed", width=2, dash="dash")))
            if ptf_val is not None:
                fig3.add_hline(y=ptf_val, line_dash="dot",
                               line_color="#ff6b35",
                               annotation_text=f"PTF: {ptf_val:,.0f} TL",
                               annotation_font_color="#ff6b35")
            dark(fig3, height=420, title="Arz-Talep Kesişim Noktası",
                 xaxis=dict(title="Kümülatif Hacim (MWh)", gridcolor="rgba(255,255,255,0.05)"),
                 yaxis=dict(title="Teklif Fiyatı (TL/MWh)", gridcolor="rgba(255,255,255,0.05)"))
            st.plotly_chart(fig3, use_container_width=True, key="gop_merit")

        with col_m_r:
            if "supply_status" in df_mo.columns:
                status_cnt = df_mo["supply_status"].value_counts().reset_index()
                status_cnt.columns = ["Durum", "Adet"]
                fig4 = go.Figure(go.Pie(
                    labels=status_cnt["Durum"], values=status_cnt["Adet"],
                    hole=0.6, marker_colors=["#10b981", "#ef4444"]))
                dark(fig4, height=380, title="Merit Dağılımı")
                st.plotly_chart(fig4, use_container_width=True, key="gop_pie")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — ARZ-TALEP & RESİDUAL YÜK
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔋 Arz-Talep & Residual Yük":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>FORECAST</span>
        <h1>Arz-Talep & Öngörülen Residual Yük</h1>
        <p>LEP yük tahmini, yenilenebilir tahmin ve residual yükün PTF üzerindeki baskısı</p>
    </div>""", unsafe_allow_html=True)

    df_frl = query(f"""
        SELECT date, ptf_try, forecasted_load_mwh, forecasted_res_mwh,
               price_independent_bid_mwh, forecasted_residual_load_mwh,
               EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_forecasted_residual_load')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date
    """)
    df_drv = query(f"""
        SELECT date, hour, ptf_try, smf_try, forecasted_load_mwh,
               forecasted_res_mwh, forecasted_residual_load_mwh,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('mart_ptf_drivers')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)

    if df_frl.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy   = df_frl
    dfy_d = df_drv if not df_drv.empty else pd.DataFrame()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort PTF", f"{dfy['ptf_try'].mean():,.2f} TL")
    c2.metric("Ort Yük Tahmini", f"{dfy['forecasted_load_mwh'].mean():,.0f} MWh")
    c3.metric("Ort RES Tahmini", f"{dfy['forecasted_res_mwh'].mean():,.0f} MWh")
    c4.metric("Ort Residual Yük", f"{dfy['forecasted_residual_load_mwh'].mean():,.0f} MWh")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Residual load vs PTF
        fig = px.scatter(dfy.dropna(subset=["forecasted_residual_load_mwh","ptf_try"]),
            x="forecasted_residual_load_mwh", y="ptf_try",
            color="ptf_try", color_continuous_scale=["#10b981","#00d4ff","#ef4444"],
            opacity=0.6, trendline="lowess",
            title="Residual Yük → PTF İlişkisi",
            labels={"forecasted_residual_load_mwh": "Residual Yük (MWh)", "ptf_try": "PTF (TL/MWh)"})
        dark(fig, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True, key="frl_scatter")

    with col_r:
        # Load vs RES monthly
        monthly = dfy.groupby("month").agg(
            load=("forecasted_load_mwh","mean"),
            res=("forecasted_res_mwh","mean"),
            residual=("forecasted_residual_load_mwh","mean")).reset_index()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=monthly["month"], y=monthly["load"],
            name="LEP Yük Tahmini", marker_color="rgba(0,212,255,0.7)"))
        fig2.add_trace(go.Bar(x=monthly["month"], y=monthly["res"],
            name="RES Tahmini", marker_color="rgba(16,185,129,0.7)"))
        fig2.add_trace(go.Scatter(x=monthly["month"], y=monthly["residual"],
            name="Residual Yük", mode="lines+markers",
            line=dict(color="#ef4444", width=2.5)))
        dark(fig2, height=380, title="Aylık Yük / RES / Residual Bileşimi", barmode="overlay",
             xaxis=dict(title="Ay", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
             yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig2, use_container_width=True, key="frl_monthly")

    # Hourly PTF driver profile
    if not dfy_d.empty:
        hourly_d = dfy_d.groupby("hour").agg(
            ptf=("ptf_try","mean"), load=("forecasted_load_mwh","mean"),
            res=("forecasted_res_mwh","mean")).reset_index()
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        fig3.add_trace(go.Bar(x=hourly_d["hour"], y=hourly_d["load"],
            name="Yük", marker_color="rgba(0,212,255,0.4)"))
        fig3.add_trace(go.Bar(x=hourly_d["hour"], y=hourly_d["res"],
            name="RES", marker_color="rgba(16,185,129,0.4)"))
        fig3.add_trace(go.Scatter(x=hourly_d["hour"], y=hourly_d["ptf"],
            name="Ort PTF", mode="lines+markers",
            line=dict(color="#ff6b35", width=2.5)), secondary_y=True)
        fig3.update_layout(**DARK_LAYOUT, height=380, barmode="overlay",
            title="Saatlik Yük, RES ve PTF Profili",
            xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="PTF (TL/MWh)", gridcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig3, use_container_width=True, key="frl_hourly")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — ARZ ŞOKU & RİSK
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🚨 Arz Şoku & Risk":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>RİSK</span>
        <h1>Arz Şoku & Risk Analizi</h1>
        <p>Santral arıza oranı, emre amade kapasite ve arz stres endeksi</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, total_outage_mwh, total_available_capacity_mwh, supply_shock_index,
               EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_supply_shock_index')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy = df

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort Arıza Kapasite", f"{dfy['total_outage_mwh'].mean():,.0f} MWh")
    c2.metric("Ort Emre Amade", f"{dfy['total_available_capacity_mwh'].mean():,.0f} MWh")
    c3.metric("Ort Arz Şok Endeksi", f"{dfy['supply_shock_index'].mean():.4f}")
    risk_days = (dfy["supply_shock_index"] > 0.1).sum()
    c4.metric("Yüksek Risk Günü (>0.1)", f"{risk_days}", delta="⚠️" if risk_days > 10 else "✅",
              delta_color="off")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dfy["date"], y=dfy["total_available_capacity_mwh"],
            name="Emre Amade Kapasite", fill="tozeroy",
            fillcolor="rgba(16,185,129,0.15)", line=dict(color="#10b981", width=1.5)))
        fig.add_trace(go.Scatter(x=dfy["date"], y=dfy["total_outage_mwh"],
            name="Arıza/Bakım", fill="tozeroy",
            fillcolor="rgba(239,68,68,0.2)", line=dict(color="#ef4444", width=1.5)))
        dark(fig, title="Günlük Kapasite: Emre Amade vs Arıza/Bakım",
             yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig, use_container_width=True, key="risk_cap")

    with col_r:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=dfy["date"], y=dfy["supply_shock_index"],
            mode="lines", fill="tozeroy",
            fillcolor="rgba(255,107,53,0.12)",
            line=dict(color="#ff6b35", width=2)))
        shock_max = dfy["supply_shock_index"].max(skipna=True)
        shock_max = shock_max if pd.notna(shock_max) else 0.5
        fig2.add_hrect(y0=0.1, y1=shock_max + 0.01,
                       fillcolor="rgba(239,68,68,0.07)",
                       line_width=0, annotation_text="Yüksek Risk Bölgesi",
                       annotation_font_color="#ef4444", annotation_position="top left")
        fig2.add_hline(y=0.1, line_dash="dot", line_color="#ef4444",
                       annotation_text="Eşik: 0.10", annotation_font_color="#ef4444")
        dark(fig2, title="Günlük Arz Şoku Endeksi",
             yaxis=dict(title="Endeks", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig2, use_container_width=True, key="risk_idx")

    # Monthly heatmap of shock index
    pivot = dfy.pivot_table(index="month", values="supply_shock_index", aggfunc="mean")
    if not pivot.empty:
        # Assign via pd.Index so the name "month" is preserved.
        pivot.index = pd.Index(
            [MONTHS_TR.get(m, str(m)) for m in pivot.index],
            name=pivot.index.name,
        )
        fig3 = px.bar(pivot.reset_index(), x="month", y="supply_shock_index",
            color="supply_shock_index",
            color_continuous_scale=["#10b981","#f59e0b","#ef4444"],
            title="Aylık Ort. Arz Şoku Endeksi",
            labels={"supply_shock_index": "Endeks", "month": ""})
        dark(fig3, height=320, coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True, key="risk_monthly")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — PTF TAHMİN & ML
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🤖 PTF Tahmin & ML":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>ML</span>
        <h1>PTF Tahmin Modeli & Lag Analizi</h1>
        <p>XGBoost backtesting, fiyat gecikmesi korelasyonları ve feature importance</p>
    </div>""", unsafe_allow_html=True)

    df_lag = query(f"""
        SELECT date, hour, ptf_try,
               ptf_lag_1h, ptf_lag_24h, ptf_lag_168h, ptf_rolling_avg_24h,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('mart_ptf_lag_features')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)

    # Fetch ALL predictions (no year filter) — includes future dates for forward panel
    # and historical dates for backtesting.  query() returns empty silently on BQNotFound.
    df_pred = query(f"""
        SELECT predicted_date, hour, predicted_ptf FROM {tbl('gold_ptf_predictions')}
        ORDER BY predicted_date, hour
    """)

    if df_lag.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy = df_lag.dropna(subset=["ptf_lag_24h", "ptf_lag_168h", "ptf_rolling_avg_24h"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort PTF", f"{dfy['ptf_try'].mean():,.2f} TL")
    c2.metric("T-24h Korel.", f"{dfy['ptf_try'].corr(dfy['ptf_lag_24h']):.3f}")
    c3.metric("T-168h Korel.", f"{dfy['ptf_try'].corr(dfy['ptf_lag_168h']):.3f}")
    c4.metric("Rolling 24h Korel.", f"{dfy['ptf_try'].corr(dfy['ptf_rolling_avg_24h']):.3f}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Lag correlation scatter T-24
        fig = px.scatter(dfy.sample(min(3000, len(dfy))),
            x="ptf_lag_24h", y="ptf_try", opacity=0.4, trendline="ols",
            color_discrete_sequence=["#00d4ff"],
            title="PTF(t) vs PTF(t-24h) — Günlük Kalıcılık",
            labels={"ptf_lag_24h": "PTF t-24h (TL/MWh)", "ptf_try": "PTF t (TL/MWh)"})
        dark(fig)
        st.plotly_chart(fig, use_container_width=True, key="ml_lag24")

    with col_r:
        # Lag correlation scatter T-168
        fig2 = px.scatter(dfy.sample(min(3000, len(dfy))),
            x="ptf_lag_168h", y="ptf_try", opacity=0.4, trendline="ols",
            color_discrete_sequence=["#7c3aed"],
            title="PTF(t) vs PTF(t-168h) — Haftalık Kalıcılık",
            labels={"ptf_lag_168h": "PTF t-168h (TL/MWh)", "ptf_try": "PTF t (TL/MWh)"})
        dark(fig2)
        st.plotly_chart(fig2, use_container_width=True, key="ml_lag168")

    # ── FORWARD FORECAST PANEL ────────────────────────────────────────────────
    # Shows future predictions (predicted_date >= today).  Appears only when
    # the gold_ptf_predictions table exists and has upcoming rows.
    if not df_pred.empty:
        df_pred["predicted_date"] = pd.to_datetime(df_pred["predicted_date"])
        _today = pd.Timestamp.now().normalize()
        df_forward = df_pred[df_pred["predicted_date"] >= _today].copy()

        if not df_forward.empty:
            st.markdown("### 🔮 İleriye Yönelik 24h Tahmin")
            df_forward["hour"] = pd.to_numeric(df_forward["hour"], errors="coerce").astype(int)
            df_forward["ts"] = df_forward["predicted_date"] + pd.to_timedelta(df_forward["hour"], unit="h")
            df_forward = df_forward.sort_values("ts")

            fig_fwd = go.Figure()
            fig_fwd.add_trace(go.Scatter(
                x=df_forward["ts"], y=df_forward["predicted_ptf"],
                mode="lines+markers",
                name="XGBoost Tahmin",
                line=dict(color="#ff6b35", width=2.5),
                fill="tozeroy", fillcolor="rgba(255,107,53,0.07)",
                hovertemplate="%{x|%d %b %H:%M}<br>%{y:,.0f} TL/MWh<extra></extra>",
            ))
            dark(fig_fwd, height=360,
                 title=f"Önümüzdeki Saatlik PTF Tahminleri (XGBoost)",
                 yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"),
                 hovermode="x unified")
            st.plotly_chart(fig_fwd, use_container_width=True, key="ml_forward")

            # Summary metrics for forward window
            fw1, fw2, fw3 = st.columns(3)
            fw1.metric("Tahmin Ort. PTF", f"{df_forward['predicted_ptf'].mean():,.2f} TL/MWh")
            fw2.metric("Tahmin Maks PTF", f"{df_forward['predicted_ptf'].max():,.2f} TL/MWh")
            fw3.metric("Tahmin Min PTF",  f"{df_forward['predicted_ptf'].min():,.2f} TL/MWh")

    # ── BACKTESTING ───────────────────────────────────────────────────────────
    if df_pred.empty:
        st.info(
            "**Tahmin tablosu henüz mevcut değil.**  \n"
            "`gold_ptf_predictions` tablosu ilk `ptf_inference.py` çalışması "
            "tamamlandıktan sonra otomatik olarak oluşturulur.  \n"
            "Airflow DAG: `ptf_hourly_inference → run_ptf_inference`"
        )
    else:
        st.markdown("### 🎯 Model Backtesting — Gerçekleşen vs Tahmin")

        df_pred["predicted_date"] = pd.to_datetime(df_pred["predicted_date"])
        df_pred["hour"] = pd.to_numeric(df_pred["hour"], errors="coerce").astype(int)

        # ── Deduplicate predictions ───────────────────────────────────────────────
        # Airflow task retries cause duplicate streaming inserts.  Keep the first
        # occurrence of each (predicted_date, hour) pair.
        df_pred = df_pred.drop_duplicates(subset=["predicted_date", "hour"])

        # Backtesting: fetch actual PTF for the EXACT date range present in
        # gold_ptf_predictions — independent of the sidebar year/month filter.
        #
        # Source: stg_pricing (NOT mart_forecasted_residual_load).
        # Reason: mart_forecasted_residual_load is driven by stg_load_estimation
        # (LEP), which stores Turkish-local hours (1–24) from the EPIAS API's
        # `time` field.  stg_pricing derives its hour via
        # EXTRACT(HOUR FROM UTC_timestamp), producing UTC hours (0–23).
        # The inference stores predicted_date/hour from the UTC datetime index
        # of mart_forecasted_residual_load.datetime — which is built as
        # TIMESTAMP_ADD(CAST(date AS TIMESTAMP), INTERVAL hour HOUR), where
        # `hour` in that mart comes from LEP (Turkish hours), not UTC.
        # Consequently the JOIN l.hour = p.hour between LEP and pricing always
        # misses (Turkish 22 ≠ UTC 19), leaving ptf_try NULL for all rows.
        #
        # stg_pricing.hour = EXTRACT(HOUR FROM UTC timestamp) matches
        # predicted_ts.hour (also UTC) exactly — the merge is guaranteed to find
        # rows as long as stg_pricing has settled data for the prediction dates.
        _bt_min = df_pred["predicted_date"].min().strftime("%Y-%m-%d")
        # +1 day buffer: UTC hours 22–23 on date D are fetched by the ds=D+1
        # Bronze pipeline run (Turkish hours 01–02 of D+1 = UTC 22–23 of D).
        _bt_max = (df_pred["predicted_date"].max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        df_actual = query(f"""
            SELECT date, hour, ptf_try
            FROM {tbl('stg_pricing')}
            WHERE date BETWEEN '{_bt_min}' AND '{_bt_max}'
            ORDER BY date, hour
        """)
        df_actual["date"] = pd.to_datetime(df_actual["date"])

        merged = df_actual.merge(
            df_pred, left_on=["date", "hour"], right_on=["predicted_date", "hour"],
            how="inner")

        if not merged.empty:
            mae = (merged["ptf_try"] - merged["predicted_ptf"]).abs().mean()
            rmse = ((merged["ptf_try"] - merged["predicted_ptf"])**2).mean()**0.5
            mape = ((merged["ptf_try"] - merged["predicted_ptf"]).abs() /
                    merged["ptf_try"].replace(0, float("nan"))).mean() * 100

            cm1, cm2, cm3 = st.columns(3)
            cm1.metric("MAE", f"{mae:,.2f} TL/MWh")
            cm2.metric("RMSE", f"{rmse:,.2f} TL/MWh")
            cm3.metric("MAPE", f"%{mape:.2f}")

            sample = merged.sort_values("date").tail(7*24)
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=sample["date"], y=sample["ptf_try"],
                name="Gerçekleşen", line=dict(color="#00d4ff", width=1.5)))
            fig3.add_trace(go.Scatter(x=sample["date"], y=sample["predicted_ptf"],
                name="XGBoost Tahmin", line=dict(color="#ff6b35", width=1.5, dash="dash")))
            dark(fig3, height=420, title="Son 7 Günlük Saatlik Backtesting",
                 yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"),
                 hovermode="x unified")
            st.plotly_chart(fig3, use_container_width=True, key="ml_bt")
        else:
            st.info("Tahmin ve gerçekleşen veriler eşleştirilemedi.")

    # SHAP importance
    try:
        _shap_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "ptf_shap_importance.csv")
        shap_df = pd.read_csv(_shap_path)
        fig4 = px.bar(shap_df.head(12),
            x="feature_importance_vals", y="col_name", orientation="h",
            color="feature_importance_vals", color_continuous_scale="Blues",
            title="🔬 XGBoost Feature Importance (SHAP)",
            labels={"feature_importance_vals": "SHAP Skoru", "col_name": ""})
        fig4.update_layout(**DARK_LAYOUT, height=400,
            yaxis={"categoryorder": "total ascending"},
            coloraxis_showscale=False)
        st.plotly_chart(fig4, use_container_width=True, key="ml_shap")
    except FileNotFoundError:
        st.info("SHAP verisi henüz mevcut değil. ptf_trainer.py çalıştırıldıktan sonra görünür.")

    # Rolling avg trend
    daily_roll = dfy.groupby("date").agg(
        ptf=("ptf_try","mean"), roll=("ptf_rolling_avg_24h","mean")).reset_index()
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(x=daily_roll["date"], y=daily_roll["ptf"],
        name="Günlük Ort PTF", line=dict(color="#00d4ff", width=1.5), opacity=0.7))
    fig5.add_trace(go.Scatter(x=daily_roll["date"], y=daily_roll["roll"],
        name="24h Rolling Ort", line=dict(color="#ff6b35", width=2.5)))
    dark(fig5, height=360, title="Günlük PTF vs 24h Hareketli Ortalama",
         yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"))
    st.plotly_chart(fig5, use_container_width=True, key="ml_roll")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — LİSANSSIZ ÜRETİM (YEKDEM)
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🌿 Lisanssız Üretim (YEKDEM)":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>YEKDEM</span>
        <h1>Lisanssız Üretim & YEKDEM Piyasa Etkisi</h1>
        <p>Çatı GES ve küçük ölçekli yenilenebilir — piyasa değeri ve üretim trendi</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, total_unlicensed_mwh, ptf_try, estimated_market_value_try,
               EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_unlicensed_impact')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    st.sidebar.download_button("📥 CSV İndir", df.to_csv(index=False).encode(),
                               "epias_yekdem.csv", "text/csv")

    dfy = df

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort Lisanssız Üretim", f"{dfy['total_unlicensed_mwh'].mean():,.0f} MWh")
    c2.metric("Toplam Üretim", f"{dfy['total_unlicensed_mwh'].sum()/1e6:.2f} TWh")
    c3.metric("Tahmini Piyasa Değ.", f"₺{dfy['estimated_market_value_try'].sum()/1e9:.2f} Mrd")
    corr_ptf = dfy["total_unlicensed_mwh"].corr(dfy["ptf_try"])
    c4.metric("Üretim~PTF Korel.", f"{corr_ptf:.3f}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=dfy["date"], y=dfy["total_unlicensed_mwh"],
            name="Lisanssız Üretim (MWh)", marker_color="rgba(16,185,129,0.6)"))
        fig.add_trace(go.Scatter(x=dfy["date"], y=dfy["ptf_try"],
            name="PTF (TL/MWh)", mode="lines",
            line=dict(color="#ff6b35", width=1.5)), secondary_y=True)
        fig.update_layout(**DARK_LAYOUT, height=400,
            title="Günlük Lisanssız Üretim vs PTF",
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="PTF (TL/MWh)", gridcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, use_container_width=True, key="yek_trend")

    with col_r:
        fig2 = px.scatter(dfy.dropna(subset=["total_unlicensed_mwh","ptf_try"]),
            x="total_unlicensed_mwh", y="ptf_try",
            opacity=0.6, trendline="lowess",
            color_discrete_sequence=["#10b981"],
            title="Lisanssız Üretim → PTF Baskısı",
            labels={"total_unlicensed_mwh": "Lisanssız Üretim (MWh)",
                    "ptf_try": "PTF (TL/MWh)"})
        dark(fig2)
        st.plotly_chart(fig2, use_container_width=True, key="yek_scatter")

    # Monthly trend
    monthly = dfy.groupby("month").agg(
        mwh=("total_unlicensed_mwh","sum"),
        value=("estimated_market_value_try","sum")).reset_index()
    monthly["ay"] = monthly["month"].map(MONTHS_TR)

    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    fig3.add_trace(go.Bar(x=monthly["ay"], y=monthly["mwh"]/1e3,
        name="Üretim (GWh)", marker_color="rgba(16,185,129,0.7)"))
    fig3.add_trace(go.Scatter(x=monthly["ay"], y=monthly["value"]/1e6,
        name="Piyasa Değeri (Milyon TL)", mode="lines+markers",
        line=dict(color="#f59e0b", width=2.5)), secondary_y=True)
    fig3.update_layout(**DARK_LAYOUT, height=360,
        title=f"{sel_year} — Aylık Lisanssız Üretim & Tahmini Piyasa Değeri",
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title="GWh", gridcolor="rgba(255,255,255,0.05)"),
        yaxis2=dict(title="Milyon TL", gridcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig3, use_container_width=True, key="yek_monthly")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 9 — GİP & HAVA DURUMU
# ══════════════════════════════════════════════════════════════════════════════
elif page == "⚡ GİP & Hava Durumu":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>IDM</span>
        <h1>Gün İçi Piyasası & Hava Durumu</h1>
        <p>GİP saatlik hacim/fiyat profili ve hava koşullarının piyasa üzerindeki etkisi</p>
    </div>""", unsafe_allow_html=True)

    df_gip = query(f"""
        SELECT trade_date AS date, hour, contract_name,
               total_transaction_count, avg_transaction_price_try,
               total_volume_mwh, total_transaction_value_try,
               EXTRACT(YEAR FROM trade_date) AS year,
               EXTRACT(MONTH FROM trade_date) AS month
        FROM {tbl('mart_gip_company_activity')}
        WHERE EXTRACT(YEAR FROM trade_date) = {sel_year}{_month_filter_td}
        ORDER BY trade_date, hour
    """)
    df_wx = query(f"""
        SELECT date, hour, city_name, temperature_celsius,
               wind_speed_kmh, shortwave_radiation, relative_humidity,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('stg_weather')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour, city_name
    """)

    if df_gip.empty:
        st.warning("GİP verisi bulunamadı.")
        st.stop()

    cities = ["Tümü"] + sorted(df_wx["city_name"].dropna().unique().tolist()) if not df_wx.empty else ["Tümü"]
    sel_city = st.selectbox("Şehir (Hava)", cities, key="gip_city")

    dfy    = df_gip
    dfy_wx = df_wx if not df_wx.empty else pd.DataFrame()
    if sel_city != "Tümü" and not dfy_wx.empty:
        dfy_wx = dfy_wx[dfy_wx["city_name"] == sel_city]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort GİP Fiyatı", f"{dfy['avg_transaction_price_try'].mean():,.2f} TL/MWh")
    c2.metric("Toplam Hacim", f"{dfy['total_volume_mwh'].sum()/1e3:,.1f} GWh")
    c3.metric("Toplam İşlem Değ.", f"₺{dfy['total_transaction_value_try'].sum()/1e9:.2f} Mrd")
    c4.metric("Toplam İşlem Adedi", f"{dfy['total_transaction_count'].sum():,}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        hourly = dfy.groupby("hour").agg(
            vol=("total_volume_mwh","mean"),
            price=("avg_transaction_price_try","mean")).reset_index()
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["vol"],
            name="Ort Hacim (MWh)", marker_color="rgba(0,212,255,0.6)"))
        fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["price"],
            name="Ort Fiyat (TL)", mode="lines+markers",
            line=dict(color="#ff6b35", width=2.5)), secondary_y=True)
        fig.update_layout(**DARK_LAYOUT, height=380, barmode="overlay",
            title="Saatlik GİP Hacim & Fiyat Profili",
            xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="TL/MWh", gridcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, use_container_width=True, key="gip_hourly")

    with col_r:
        monthly = dfy.groupby("month")["total_volume_mwh"].sum().reset_index()
        monthly["ay"] = monthly["month"].map(MONTHS_TR)
        fig2 = px.bar(monthly, x="ay", y="total_volume_mwh",
            color="total_volume_mwh",
            color_continuous_scale=["#10b981","#00d4ff","#7c3aed"],
            title="Aylık GİP İşlem Hacmi (MWh)",
            labels={"total_volume_mwh": "MWh", "ay": ""})
        dark(fig2, height=380, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True, key="gip_monthly")

    # Weather section
    if not dfy_wx.empty:
        st.markdown("### 🌡️ Hava Koşulları")
        daily_wx = dfy_wx.groupby("date").agg(
            temp=("temperature_celsius","mean"),
            wind=("wind_speed_kmh","mean"),
            rad=("shortwave_radiation","mean")).reset_index()

        col_w1, col_w2 = st.columns(2)

        with col_w1:
            fig3 = make_subplots(specs=[[{"secondary_y": True}]])
            fig3.add_trace(go.Scatter(x=daily_wx["date"], y=daily_wx["temp"],
                name="Sıcaklık (°C)", mode="lines",
                line=dict(color="#ef4444", width=1.5),
                fill="tozeroy", fillcolor="rgba(239,68,68,0.06)"))
            fig3.add_trace(go.Scatter(x=daily_wx["date"], y=daily_wx["wind"],
                name="Rüzgar (km/h)", mode="lines",
                line=dict(color="#00d4ff", width=1.5)), secondary_y=True)
            fig3.update_layout(**DARK_LAYOUT, height=360,
                title="Günlük Sıcaklık & Rüzgar Hızı",
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(title="°C", gridcolor="rgba(255,255,255,0.05)"),
                yaxis2=dict(title="km/h", gridcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig3, use_container_width=True, key="gip_wx_temp")

        with col_w2:
            # Merge GİP daily price with weather
            daily_gip = dfy.groupby("date")["avg_transaction_price_try"].mean().reset_index()
            merged_wx = daily_wx.merge(daily_gip, on="date", how="inner")
            if not merged_wx.empty:
                fig4 = px.scatter(merged_wx,
                    x="temp", y="avg_transaction_price_try",
                    color="wind", color_continuous_scale=["#10b981","#00d4ff","#7c3aed"],
                    opacity=0.7, trendline="lowess",
                    title="Sıcaklık → GİP Fiyatı İlişkisi",
                    labels={"temp": "Ort. Sıcaklık (°C)",
                            "avg_transaction_price_try": "Ort. GİP Fiyatı (TL/MWh)",
                            "wind": "Rüzgar (km/h)"})
                dark(fig4, height=360, coloraxis_colorbar=dict(title="Rüzgar"))
                st.plotly_chart(fig4, use_container_width=True, key="gip_wx_corr")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 10 — ÜRETİM PLANI (BGÜP vs KGÜP)
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏭 Üretim Planı (BGÜP vs KGÜP)":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>PLANLAMA</span>
        <h1>Üretim Planı: BGÜP vs KGÜP</h1>
        <p>Beyan edilen plan vs kesinleşmiş plan — GİP revizyonlarının büyüklüğü ve yönü</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, hour,
               bgup_total_mwh, kgup_total_mwh,
               bgup_wind_mwh, kgup_wind_mwh,
               bgup_solar_mwh, kgup_solar_mwh,
               bgup_hydro_mwh, kgup_hydro_mwh,
               intraday_revision_mwh, wind_revision_mwh,
               solar_revision_mwh, hydro_revision_mwh,
               revision_direction, revision_pct,
               EXTRACT(YEAR FROM date) AS year,
               EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_production_plan')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)
    if df.empty:
        st.info(
            "🔄 **Üretim Planı verisi henüz mevcut değil.**\n\n"
            "Bu sayfa `mart_production_plan` tablosunu kullanır. Tablo, Silver katmanı "
            "backfill'i tamamlandıktan sonra `dbt run` ile oluşturulacak. "
            "Backfill durumunu Airflow UI'den takip edebilirsiniz."
        )
        st.stop()

    dfy = df.dropna(subset=["bgup_total_mwh", "kgup_total_mwh"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort BGÜP", f"{dfy['bgup_total_mwh'].mean():,.0f} MWh")
    c2.metric("Ort KGÜP", f"{dfy['kgup_total_mwh'].mean():,.0f} MWh")
    c3.metric("Ort Revizyon", f"{dfy['intraday_revision_mwh'].mean():+.0f} MWh")
    rev_pct = (dfy["revision_direction"] != "Denge").mean() * 100
    c4.metric("Aktif Revizyon %", f"%{rev_pct:.1f}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Daily BGÜP vs KGÜP trend
        daily = dfy.groupby("date").agg(
            bgup=("bgup_total_mwh","mean"),
            kgup=("kgup_total_mwh","mean"),
            rev=("intraday_revision_mwh","mean")).reset_index()
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["bgup"],
            name="BGÜP (Beyan)", line=dict(color="#00d4ff", width=1.5)))
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["kgup"],
            name="KGÜP (Kesinleşmiş)", line=dict(color="#10b981", width=1.5)))
        fig.add_trace(go.Bar(x=daily["date"], y=daily["rev"],
            name="Revizyon (KGÜP-BGÜP)",
            marker_color=daily["rev"].apply(
                lambda v: "rgba(16,185,129,0.5)" if v >= 0 else "rgba(239,68,68,0.5)")),
            secondary_y=True)
        fig.update_layout(**DARK_LAYOUT, height=400,
            title="Günlük BGÜP / KGÜP & Revizyon",
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"),
            yaxis2=dict(title="Revizyon (MWh)", gridcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, use_container_width=True, key="pp_trend")

    with col_r:
        # Revision direction distribution
        dir_counts = dfy["revision_direction"].value_counts().reset_index()
        dir_counts.columns = ["Yön", "Saat"]
        color_map = {"GIP_Alim": "#ef4444", "GIP_Satis": "#10b981", "Denge": "#64748b"}
        fig2 = px.pie(dir_counts, names="Yön", values="Saat",
            hole=0.55,
            color="Yön", color_discrete_map=color_map,
            title="Revizyon Yönü Dağılımı")
        dark(fig2, height=400)
        st.plotly_chart(fig2, use_container_width=True, key="pp_dir_pie")

    # Source breakdown: Wind, Solar, Hydro revision
    st.markdown("### 🌬️ Kaynak Bazlı Revizyon (Rüzgar / Güneş / Baraj)")
    hourly_src = dfy.groupby("hour").agg(
        wind_rev=("wind_revision_mwh","mean"),
        solar_rev=("solar_revision_mwh","mean"),
        hydro_rev=("hydro_revision_mwh","mean")).reset_index()

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(x=hourly_src["hour"], y=hourly_src["wind_rev"],
        name="Rüzgar Revizyonu", marker_color="rgba(0,212,255,0.7)"))
    fig3.add_trace(go.Bar(x=hourly_src["hour"], y=hourly_src["solar_rev"],
        name="Güneş Revizyonu", marker_color="rgba(245,158,11,0.7)"))
    fig3.add_trace(go.Bar(x=hourly_src["hour"], y=hourly_src["hydro_rev"],
        name="Baraj Revizyonu", marker_color="rgba(124,58,237,0.7)"))
    dark(fig3, height=380, title="Saatlik Ort. Kaynak Revizyonu (KGÜP - BGÜP)",
         barmode="group",
         xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
         yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"))
    fig3.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
    st.plotly_chart(fig3, use_container_width=True, key="pp_src_rev")

    # Revision pct distribution
    fig4 = px.histogram(dfy[dfy["revision_pct"].notna() & (dfy["revision_pct"] < 50)],
        x="revision_pct", nbins=60, color="revision_direction",
        color_discrete_map=color_map,
        barmode="overlay", opacity=0.75,
        title="Revizyon Büyüklüğü Dağılımı (BGÜP'e göre %)",
        labels={"revision_pct": "Revizyon %", "revision_direction": "Yön"})
    dark(fig4, height=340)
    st.plotly_chart(fig4, use_container_width=True, key="pp_rev_hist")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 11 — PTF TAVAN & MİNİMUM ANALİZİ
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔥 PTF Tavan & Minimum Analizi":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>EXTREME</span>
        <h1>PTF Tavan & Minimum Analizi</h1>
        <p>En pahalı/ucuz %5 saatlerin tetikleyicileri — kapasite stresi, RES fazlası, sistem açığı</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, hour, ptf_try, smf_try, smf_ptf_spread,
               residual_load_mwh, total_aic_mwh, capacity_utilization_ratio,
               system_direction, net_imbalance_mwh,
               p5_ptf, p95_ptf, avg_ptf,
               ptf_category, extreme_driver,
               EXTRACT(YEAR FROM date) AS year,
               EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_ptf_extremes')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    dfy = df

    p95 = dfy["p95_ptf"].iloc[0] if not dfy.empty else 0
    p5  = dfy["p5_ptf"].iloc[0] if not dfy.empty else 0
    tavan = dfy[dfy["ptf_category"] == "TAVAN"]
    minimum = dfy[dfy["ptf_category"] == "MINIMUM"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Tavan Eşiği (p95)", f"{p95:,.2f} TL")
    c2.metric("Min Eşiği (p5)", f"{p5:,.2f} TL")
    c3.metric("Tavan Saati", f"{len(tavan):,}")
    c4.metric("Minimum Saati", f"{len(minimum):,}")
    c5.metric("Tavan/Min Oranı", f"{len(tavan)/max(len(minimum),1):.1f}x")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # PTF distribution with threshold bands
        fig = px.histogram(dfy, x="ptf_try", nbins=100,
            color="ptf_category",
            color_discrete_map={"TAVAN":"#ef4444","MINIMUM":"#10b981","NORMAL":"rgba(100,116,139,0.5)"},
            barmode="overlay", opacity=0.75,
            title="PTF Dağılımı — Tavan / Normal / Minimum",
            labels={"ptf_try": "PTF (TL/MWh)", "ptf_category": "Kategori"})
        fig.add_vline(x=p95, line_dash="dot", line_color="#ef4444",
                      annotation_text=f"p95: {p95:,.0f}", annotation_font_color="#ef4444")
        fig.add_vline(x=p5, line_dash="dot", line_color="#10b981",
                      annotation_text=f"p5: {p5:,.0f}", annotation_font_color="#10b981")
        dark(fig, height=400)
        st.plotly_chart(fig, use_container_width=True, key="ext_hist")

    with col_r:
        # Extreme driver breakdown
        driver_df = dfy[dfy["extreme_driver"].notna()].groupby(
            ["ptf_category","extreme_driver"]).size().reset_index(name="count")
        fig2 = px.bar(driver_df, x="extreme_driver", y="count",
            color="ptf_category",
            color_discrete_map={"TAVAN":"#ef4444","MINIMUM":"#10b981"},
            barmode="group",
            title="Tavan & Minimum Tetikleyicileri",
            labels={"extreme_driver":"Tetikleyici","count":"Saat Sayısı","ptf_category":"Kategori"})
        dark(fig2, height=400)
        st.plotly_chart(fig2, use_container_width=True, key="ext_drivers")

    # Capacity utilization for ceiling hours
    if not tavan.empty and "capacity_utilization_ratio" in tavan.columns:
        st.markdown("### 🔴 Tavan Saati Analizi")
        col_t1, col_t2 = st.columns(2)

        with col_t1:
            fig3 = px.scatter(tavan.dropna(subset=["capacity_utilization_ratio","ptf_try"]),
                x="capacity_utilization_ratio", y="ptf_try",
                color="extreme_driver",
                color_discrete_sequence=px.colors.qualitative.Set2,
                opacity=0.7, trendline="ols",
                title="Kapasite Kullanım Oranı → Tavan PTF",
                labels={"capacity_utilization_ratio":"Kapasite Kull. Oranı",
                        "ptf_try":"PTF (TL/MWh)", "extreme_driver":"Tetikleyici"})
            dark(fig3)
            st.plotly_chart(fig3, use_container_width=True, key="ext_cap_util")

        with col_t2:
            hourly_tavan = tavan.groupby("hour").size().reset_index(name="count")
            fig4 = px.bar(hourly_tavan, x="hour", y="count",
                color="count", color_continuous_scale=["#f59e0b","#ef4444"],
                title="Tavan Saatlerinin Saat Dağılımı",
                labels={"hour":"Saat","count":"Tavan Saati Adedi"})
            dark(fig4, height=380, coloraxis_showscale=False,
                 xaxis=dict(tickmode="linear"))
            st.plotly_chart(fig4, use_container_width=True, key="ext_hour_dist")

    # Minimum hours: residual load analysis
    if not minimum.empty and "residual_load_mwh" in minimum.columns:
        st.markdown("### 🟢 Minimum Saati Analizi")
        col_m1, col_m2 = st.columns(2)

        with col_m1:
            fig5 = px.scatter(minimum.dropna(subset=["residual_load_mwh","ptf_try"]),
                x="residual_load_mwh", y="ptf_try",
                color="extreme_driver",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                opacity=0.7,
                title="Residual Yük → Minimum PTF",
                labels={"residual_load_mwh":"Residual Yük (MWh)",
                        "ptf_try":"PTF (TL/MWh)", "extreme_driver":"Tetikleyici"})
            dark(fig5)
            st.plotly_chart(fig5, use_container_width=True, key="ext_res_min")

        with col_m2:
            monthly_min = dfy.groupby("month")["ptf_category"].apply(
                lambda s: (s == "MINIMUM").sum()).reset_index(name="min_count")
            monthly_tav = dfy.groupby("month")["ptf_category"].apply(
                lambda s: (s == "TAVAN").sum()).reset_index(name="tav_count")
            monthly_ext = monthly_min.merge(monthly_tav, on="month")
            monthly_ext["ay"] = monthly_ext["month"].map(MONTHS_TR)
            fig6 = go.Figure()
            fig6.add_trace(go.Bar(x=monthly_ext["ay"], y=monthly_ext["tav_count"],
                name="Tavan Saatleri", marker_color="rgba(239,68,68,0.7)"))
            fig6.add_trace(go.Bar(x=monthly_ext["ay"], y=monthly_ext["min_count"],
                name="Minimum Saatleri", marker_color="rgba(16,185,129,0.7)"))
            dark(fig6, height=380, title="Aylık Tavan & Minimum Saat Dağılımı",
                 barmode="group",
                 xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                 yaxis=dict(title="Saat Sayısı", gridcolor="rgba(255,255,255,0.05)"))
            st.plotly_chart(fig6, use_container_width=True, key="ext_monthly")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 12 — ÇAPRAZ PİYASA ARBİTRAJ (GÖP → GİP → DGP)
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Çapraz Piyasa Arbitraj":
    st.markdown("""
    <div class='page-header'>
        <span class='badge'>ARBİTRAJ</span>
        <h1>Çapraz Piyasa Analizi: GÖP → GİP → DGP</h1>
        <p>Üç piyasa spread'leri, kademeli fiyat yapıları ve arbitraj fırsatı skorları
        (Maciejowska et al. · Wozabal & Ferreira)</p>
    </div>""", unsafe_allow_html=True)

    df = query(f"""
        SELECT date, hour,
               gop_ptf_try, gip_vwap_try, dgp_smf_try,
               gip_gop_spread_try, smf_gop_spread_try, smf_gip_spread_try,
               gip_gop_spread_pct, price_cascade,
               arbitrage_opportunity_score,
               system_direction, net_imbalance_mwh,
               yal_delivered_mwh, yat_delivered_mwh, net_dgp_mwh,
               gip_total_volume_mwh, gip_transaction_count,
               EXTRACT(YEAR  FROM date) AS year,
               EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_cross_market_spread')}
        WHERE EXTRACT(YEAR FROM date) = {sel_year}{_month_filter}
        ORDER BY date, hour
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    cascades = ["Tümü"] + sorted(df["price_cascade"].dropna().unique().tolist())
    sel_cascade = st.selectbox("Kademeli Yapı", cascades, key="arb_cascade")

    dfy = df.copy()
    if sel_cascade != "Tümü":
        dfy = dfy[dfy["price_cascade"] == sel_cascade]

    # ── KPI'lar ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Ort. GÖP (PTF)", f"{dfy['gop_ptf_try'].mean():,.2f} TL")
    c2.metric("Ort. GİP (VWAP)", f"{dfy['gip_vwap_try'].mean():,.2f} TL",
              f"{dfy['gip_gop_spread_try'].mean():+.2f}")
    c3.metric("Ort. DGP (SMF)", f"{dfy['dgp_smf_try'].mean():,.2f} TL",
              f"{dfy['smf_gop_spread_try'].mean():+.2f}")
    hi_arb = (dfy["arbitrage_opportunity_score"] > 0.05).sum()
    c4.metric("Yüksek Arbitraj Saati (>%5)", f"{hi_arb:,}")
    avg_score = dfy["arbitrage_opportunity_score"].mean()
    c5.metric("Ort. Arbitraj Skoru", f"{avg_score:.4f}")

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Üç piyasa fiyat serisi — son 30 gün
        recent = dfy.sort_values(["date","hour"]).tail(30 * 24)
        recent["ts"] = pd.to_datetime(recent["date"].astype(str)) + pd.to_timedelta(recent["hour"], unit="h")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=recent["ts"], y=recent["gop_ptf_try"],
            name="GÖP (PTF)", line=dict(color="#00d4ff", width=1.5)))
        fig.add_trace(go.Scatter(x=recent["ts"], y=recent["gip_vwap_try"],
            name="GİP (VWAP)", line=dict(color="#f59e0b", width=1.5)))
        fig.add_trace(go.Scatter(x=recent["ts"], y=recent["dgp_smf_try"],
            name="DGP (SMF)", line=dict(color="#ef4444", width=1.5, dash="dot")))
        dark(fig, height=400, title="Son 30 Gün — GÖP / GİP / DGP Fiyat Serisi",
             yaxis=dict(title="TL/MWh", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig, use_container_width=True, key="arb_price_series")

    with col_r:
        # Kademeli yapı dağılımı
        cascade_counts = dfy["price_cascade"].value_counts().reset_index()
        cascade_counts.columns = ["Yapı", "Saat"]
        cascade_colors = {
            "ESCALATING": "#ef4444", "DESCENDING": "#10b981",
            "GIP_PEAK": "#f59e0b", "GIP_TROUGH": "#7c3aed", "FLAT": "#64748b"
        }
        fig2 = px.pie(cascade_counts, names="Yapı", values="Saat",
            hole=0.55,
            color="Yapı", color_discrete_map=cascade_colors,
            title="Kademeli Fiyat Yapısı Dağılımı")
        dark(fig2, height=400)
        st.plotly_chart(fig2, use_container_width=True, key="arb_cascade_pie")

    st.markdown("### 📊 Spread Analizi")
    col_s1, col_s2 = st.columns(2)

    with col_s1:
        # GİP-GÖP spread saatlik profili
        hourly_spread = dfy.groupby("hour").agg(
            gip_gop=("gip_gop_spread_try", "mean"),
            smf_gop=("smf_gop_spread_try", "mean"),
            smf_gip=("smf_gip_spread_try", "mean"),
        ).reset_index()
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(x=hourly_spread["hour"], y=hourly_spread["gip_gop"],
            name="GİP - GÖP", marker_color="rgba(245,158,11,0.7)"))
        fig3.add_trace(go.Scatter(x=hourly_spread["hour"], y=hourly_spread["smf_gop"],
            name="SMF - GÖP", mode="lines+markers",
            line=dict(color="#ef4444", width=2)))
        fig3.add_trace(go.Scatter(x=hourly_spread["hour"], y=hourly_spread["smf_gip"],
            name="SMF - GİP", mode="lines+markers",
            line=dict(color="#7c3aed", width=2, dash="dash")))
        fig3.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)")
        dark(fig3, height=380, title="Saatlik Ort. Spread Profili (TL/MWh)",
             xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
             yaxis=dict(title="Spread (TL/MWh)", gridcolor="rgba(255,255,255,0.05)"),
             barmode="overlay")
        st.plotly_chart(fig3, use_container_width=True, key="arb_spread_hourly")

    with col_s2:
        # GİP-GÖP spread dağılımı — sistem yönüne göre renk
        fig4 = px.histogram(
            dfy.dropna(subset=["gip_gop_spread_try","system_direction"]),
            x="gip_gop_spread_try", nbins=80,
            color="system_direction",
            color_discrete_map={
                "ENERGY_DEFICIT": "#ef4444",
                "ENERGY_SURPLUS": "#10b981",
                "IN_BALANCE":     "#64748b",
            },
            barmode="overlay", opacity=0.7,
            title="GİP - GÖP Spread Dağılımı (Sistem Yönüne Göre)",
            labels={"gip_gop_spread_try": "GİP - GÖP (TL/MWh)",
                    "system_direction": "Sistem Yönü"})
        fig4.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.4)")
        dark(fig4, height=380)
        st.plotly_chart(fig4, use_container_width=True, key="arb_spread_hist")

    st.markdown("### 🎯 Arbitraj Fırsatı & DGP Baskısı")
    col_a1, col_a2 = st.columns(2)

    with col_a1:
        # Arbitraj skoru — günlük ortalama trend
        daily_arb = dfy.groupby("date")["arbitrage_opportunity_score"].mean().reset_index()
        fig5 = go.Figure(go.Scatter(
            x=daily_arb["date"], y=daily_arb["arbitrage_opportunity_score"],
            mode="lines", line=dict(color="#f59e0b", width=1.8),
            fill="tozeroy", fillcolor="rgba(245,158,11,0.08)"))
        fig5.add_hline(y=0.05, line_dash="dot", line_color="rgba(239,68,68,0.6)",
                       annotation_text="Yüksek Fırsat Eşiği (%5)",
                       annotation_font_color="#ef4444")
        dark(fig5, height=360, title="Günlük Ort. Arbitraj Fırsatı Skoru",
             yaxis=dict(title="Skor", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig5, use_container_width=True, key="arb_score_trend")

    with col_a2:
        # YAL vs YAT saatlik profil
        hourly_dgp = dfy.groupby("hour").agg(
            yal=("yal_delivered_mwh", "mean"),
            yat=("yat_delivered_mwh", "mean"),
        ).reset_index()
        fig6 = go.Figure()
        fig6.add_trace(go.Bar(x=hourly_dgp["hour"], y=hourly_dgp["yal"],
            name="YAL (Yük Alma)", marker_color="rgba(239,68,68,0.7)"))
        fig6.add_trace(go.Bar(x=hourly_dgp["hour"], y=hourly_dgp["yat"],
            name="YAT (Yük Atma)", marker_color="rgba(16,185,129,0.7)"))
        dark(fig6, height=360, title="Saatlik Ort. DGP Regülasyon Hacimleri",
             barmode="group",
             xaxis=dict(title="Saat", tickmode="linear", gridcolor="rgba(255,255,255,0.05)"),
             yaxis=dict(title="MWh", gridcolor="rgba(255,255,255,0.05)"))
        st.plotly_chart(fig6, use_container_width=True, key="arb_dgp_hourly")

    # ── Aylık Kademeli Yapı Heatmap ──────────────────────────────────────────
    st.markdown("### 🗓️ Aylık Kademeli Yapı Dağılımı")
    cascade_monthly = (
        dfy.groupby(["month", "price_cascade"])
        .size().reset_index(name="count")
    )
    cascade_monthly["ay"] = cascade_monthly["month"].map(MONTHS_TR)
    fig7 = px.bar(cascade_monthly, x="ay", y="count",
        color="price_cascade",
        color_discrete_map=cascade_colors,
        barmode="stack",
        title=f"{sel_year} — Aylık Kademeli Fiyat Yapısı Dağılımı",
        labels={"count": "Saat", "ay": "", "price_cascade": "Yapı"})
    dark(fig7, height=360)
    st.plotly_chart(fig7, use_container_width=True, key="arb_cascade_monthly")
