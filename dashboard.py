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
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
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
        return get_client().query(sql).to_dataframe()
    except Exception as e:
        st.error(f"Sorgu hatası: {e}")
        return pd.DataFrame()

def tbl(mart: str) -> str:
    return f"`{PROJECT}.{DATASET}.{mart}`"

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
    ], label_visibility="collapsed")

    st.markdown("---")
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown(
        "<div style='font-size:.7rem;color:#64748b;'>"
        "Kaynak: EPIAŞ Şeffaflık Platformu<br>Pipeline: Airflow → Spark → dbt → BQ"
        "</div>", unsafe_allow_html=True
    )

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
               EXTRACT(YEAR FROM date)  AS year,
               EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_price_analysis')}
        ORDER BY date, hour
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    st.sidebar.download_button("📥 CSV İndir", df.to_csv(index=False).encode(),
                               "epias_price.csv", "text/csv")

    years = sorted(df["year"].unique(), reverse=True)
    col_f1, col_f2 = st.columns(2)
    sel_year = col_f1.selectbox("Yıl", years)
    seasons_avail = ["Tümü"] + list(df["season"].dropna().unique())
    sel_season = col_f2.selectbox("Mevsim", seasons_avail)

    dfy = df[df["year"] == sel_year]
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

    df_mix = query(f"""
        SELECT date, hour, total_generation, renewable_ratio, fossil_ratio,
               EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
        FROM {tbl('mart_generation_mix')} ORDER BY date, hour
    """)
    df_ren = query(f"""
        SELECT date, hour, ptf_try, licensed_renewable_mwh, total_unlicensed_mwh,
               total_green_energy_mwh, residual_load_mwh, total_demand_mwh,
               SAFE_DIVIDE(total_green_energy_mwh, total_demand_mwh) AS renewable_ratio,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('mart_renawable_impact')} ORDER BY date, hour
    """)
    df_deep = query(f"""
        SELECT date, hour, wind_generation_mwh, solar_generation_mwh,
               forecasted_res_mwh, wind_forecast_error,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('mart_renewable_deep')} ORDER BY date, hour
    """)

    if df_mix.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    years = sorted(df_mix["year"].unique(), reverse=True)
    sel_year = st.selectbox("Yıl", years, key="ren_year")
    dfy = df_mix[df_mix["year"] == sel_year]
    dfy_ren = df_ren[df_ren["year"] == sel_year] if not df_ren.empty else pd.DataFrame()
    dfy_deep = df_deep[df_deep["year"] == sel_year] if not df_deep.empty else pd.DataFrame()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ort. Yenilenebilir %", f"%{dfy['renewable_ratio'].mean()*100:.1f}")
    c2.metric("Ort. Fosil %", f"%{dfy['fossil_ratio'].mean()*100:.1f}")
    c3.metric("Ort. Toplam Üretim", f"{dfy['total_generation'].mean():,.0f} MWh")
    if not dfy_ren.empty:
        corr = dfy_ren["renewable_ratio"].corr(dfy_ren["ptf_try"]) if "renewable_ratio" in dfy_ren else None
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
        FROM {tbl('mart_gop_volume_analysis')} ORDER BY date, hour
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

    years = sorted(df_vol["year"].unique(), reverse=True)
    sel_year = st.selectbox("Yıl", years, key="gop_year")
    dfy = df_vol[df_vol["year"] == sel_year]

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
            fig3.add_trace(go.Scatter(
                x=df_mo["cumulative_demand_mwh"].dropna(),
                y=df_mo["bid_offer_price_try"],
                mode="lines", name="Talep Eğrisi",
                line=dict(color="#7c3aed", width=2, dash="dash")))
            if ptf_val:
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
        FROM {tbl('mart_forecasted_residual_load')} ORDER BY date
    """)
    df_drv = query(f"""
        SELECT date, hour, ptf_try, smf_try, forecasted_load_mwh,
               forecasted_res_mwh, forecasted_residual_load_mwh,
               EXTRACT(YEAR FROM date) AS year
        FROM {tbl('mart_ptf_drivers')} ORDER BY date, hour
    """)

    if df_frl.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    years = sorted(df_frl["year"].unique(), reverse=True)
    sel_year = st.selectbox("Yıl", years, key="frl_year")
    dfy = df_frl[df_frl["year"] == sel_year]
    dfy_d = df_drv[df_drv["year"] == sel_year] if not df_drv.empty else pd.DataFrame()

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
        ORDER BY date
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    years = sorted(df["year"].unique(), reverse=True)
    col_f1, col_f2 = st.columns(2)
    sel_year = col_f1.selectbox("Yıl", years, key="risk_year")
    dfy = df[df["year"] == sel_year]

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
        fig2.add_hrect(y0=0.1, y1=dfy["supply_shock_index"].max()+0.01,
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
        months_tr = {1:"Oca",2:"Şub",3:"Mar",4:"Nis",5:"May",6:"Haz",
                     7:"Tem",8:"Ağu",9:"Eyl",10:"Eki",11:"Kas",12:"Ara"}
        # Assign via pd.Index so the name "month" is preserved.
        # A plain list assignment drops the name, making reset_index() produce
        # a column called "index" (or 0) instead of "month" → px.bar crash.
        pivot.index = pd.Index(
            [months_tr.get(m, str(m)) for m in pivot.index],
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
        ORDER BY date, hour
    """)

    # Try to fetch predictions table
    df_pred = query(f"""
        SELECT predicted_date, hour, predicted_ptf FROM `{PROJECT}.{DATASET}.gold_ptf_predictions`
        ORDER BY predicted_date, hour
    """)

    if df_lag.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    years = sorted(df_lag["year"].unique(), reverse=True)
    sel_year = st.selectbox("Yıl", years, key="ml_year")
    dfy = df_lag[df_lag["year"] == sel_year].dropna(
        subset=["ptf_lag_24h", "ptf_lag_168h", "ptf_rolling_avg_24h"])

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

    # Backtesting: actual vs predicted
    if not df_pred.empty:
        st.markdown("### 🎯 Model Backtesting — Gerçekleşen vs Tahmin")

        df_pred["predicted_date"] = pd.to_datetime(df_pred["predicted_date"])
        df_pred["hour"] = pd.to_numeric(df_pred["hour"], errors="coerce").astype("Int64")

        merged = df_lag.merge(
            df_pred, left_on=["date","hour"], right_on=["predicted_date","hour"], how="inner")

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
        shap_df = pd.read_csv("models/ptf_shap_importance.csv")
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
        ORDER BY date
    """)
    if df.empty:
        st.warning("Veri bulunamadı.")
        st.stop()

    st.sidebar.download_button("📥 CSV İndir", df.to_csv(index=False).encode(),
                               "epias_yekdem.csv", "text/csv")

    years = sorted(df["year"].unique(), reverse=True)
    sel_year = st.selectbox("Yıl", years, key="yek_year")
    dfy = df[df["year"] == sel_year]

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
    months_tr = {1:"Oca",2:"Şub",3:"Mar",4:"Nis",5:"May",6:"Haz",
                 7:"Tem",8:"Ağu",9:"Eyl",10:"Eki",11:"Kas",12:"Ara"}
    monthly["ay"] = monthly["month"].map(months_tr)

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
