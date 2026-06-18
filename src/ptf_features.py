"""
ptf_features.py — Shared PTF feature engineering

Called by both ptf_trainer.py (full history) and ptf_inference.py (single row).
Returns a DataFrame with all model features; callers are responsible for
dropna() since lag/rolling columns carry NaN during the warm-up window and
the appropriate subset differs between training and inference.
"""

import numpy as np
import pandas as pd

try:
    import holidays as _holidays_lib
    _TR_HOLIDAYS = _holidays_lib.Turkey(years=range(2020, 2031))
except ImportError:  # graceful degradation if package absent
    _TR_HOLIDAYS = {}


def build_ptf_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shared PTF feature engineering for training and inference.

    Expects a DataFrame with a DatetimeIndex (Turkey local time, UTC+3) and
    at minimum the column ``ptf_try``.  All other columns are optional — the
    function falls back gracefully when source columns are absent.

    Additional columns used when present (all come from mart_ptf_lag_features):
        ptf_lag_1h, ptf_lag_24h, ptf_lag_168h
        ptf_rolling_avg_24h, ptf_rolling_max_24h, ptf_rolling_min_24h
        ptf_rolling_avg_168h
        forecasted_residual_load_mwh, forecasted_load_mwh, forecasted_res_mwh
        actual_consumption_mwh, consumption_error_mwh
        temperature_celsius, wind_speed_kmh, shortwave_radiation, relative_humidity
        wind_generation_mwh, solar_generation_mwh, hydro_generation_mwh
        gas_generation_mwh, total_generation_mwh, actual_renewable_ratio
        gop_volume_imbalance_mwh, capacity_utilization_ratio
        net_imbalance_mwh, gip_gop_price_spread, arb_score_lag24
        supply_shock_index, total_outage_mwh, supply_shock_trend_7d
        yal_lag24, yat_lag24, net_dgp_lag24
    """
    df = df.copy()

    # ── Calendar features ─────────────────────────────────────────────────────
    df["hour"]        = df.index.hour
    df["day_of_week"] = df.index.dayofweek        # Mon=0 … Sun=6
    df["month"]       = df.index.month
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)

    # Turkish national holidays (demand collapses ~15–25% on public holidays)
    df["is_holiday"] = df.index.date
    df["is_holiday"] = df["is_holiday"].apply(
        lambda d: 1 if d in _TR_HOLIDAYS else 0
    ).astype(int)

    # Cyclical hour encoding — XGBoost treats raw integers as ordinal;
    # sin/cos preserves the wrap-around continuity (23 ≈ 0)
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # ── PTF lag / rolling features ────────────────────────────────────────────
    # If pre-computed lag columns already exist (from mart_ptf_lag_features) use
    # them as-is.  Otherwise compute from the series — useful during inference
    # when the caller builds a small window from recent history.
    if "ptf_lag_24h" not in df.columns:
        df["ptf_lag_24h"]  = df["ptf_try"].shift(24)
    if "ptf_lag_168h" not in df.columns:
        df["ptf_lag_168h"] = df["ptf_try"].shift(168)
    if "ptf_lag_1h" not in df.columns:
        df["ptf_lag_1h"]   = df["ptf_try"].shift(1)

    if "ptf_rolling_avg_24h" not in df.columns:
        df["ptf_rolling_avg_24h"]  = df["ptf_try"].shift(1).rolling(24).mean()
    if "ptf_rolling_max_24h" not in df.columns:
        df["ptf_rolling_max_24h"]  = df["ptf_try"].shift(1).rolling(24).max()
    if "ptf_rolling_min_24h" not in df.columns:
        df["ptf_rolling_min_24h"]  = df["ptf_try"].shift(1).rolling(24).min()
    if "ptf_rolling_avg_168h" not in df.columns:
        df["ptf_rolling_avg_168h"] = df["ptf_try"].shift(1).rolling(168).mean()

    # ── Renewable ratio (merit-order pressure) ────────────────────────────────
    # If detailed generation columns exist, compute a more granular ratio.
    # Fall back to the pre-computed column when individual types are absent.
    if all(c in df.columns for c in
           ["wind_generation_mwh", "solar_generation_mwh",
            "hydro_generation_mwh", "total_generation_mwh"]):
        df["renewable_ratio"] = (
            (df["wind_generation_mwh"]
             + df["solar_generation_mwh"]
             + df["hydro_generation_mwh"])
            / df["total_generation_mwh"].replace(0, np.nan)
        )
    elif "actual_renewable_ratio" in df.columns:
        df["renewable_ratio"] = df["actual_renewable_ratio"]
    else:
        df["renewable_ratio"] = np.nan

    # ── Consumption error (demand surprise) ───────────────────────────────────
    if "consumption_error_mwh" in df.columns:
        # Already computed in mart_ml_features; rename for model consistency
        df["consumption_error"] = df["consumption_error_mwh"]
    elif ("actual_consumption_mwh" in df.columns and
          "forecasted_load_mwh" in df.columns):
        df["consumption_error"] = (
            df["actual_consumption_mwh"] - df["forecasted_load_mwh"]
        )
    else:
        df["consumption_error"] = 0.0

    # ── Supply shock features ─────────────────────────────────────────────────
    # Outage data is sparse (only dates with events); forward-fill propagates
    # the last known outage level.
    for col in ["supply_shock_index", "total_outage_mwh",
                "total_available_capacity_mwh", "total_aic_mwh"]:
        if col in df.columns:
            df[col] = df[col].ffill().fillna(0.0)

    if "supply_shock_index" in df.columns:
        df["supply_shock_trend_7d"] = (
            df["supply_shock_index"].rolling(168).mean()
        )
    elif "supply_shock_trend_7d" not in df.columns:
        df["supply_shock_trend_7d"] = 0.0

    return df


# ── Canonical feature list ────────────────────────────────────────────────────
# Defines the input columns used at model.predict() time.
# Order is fixed so joblib-serialised models stay compatible across retrains.
# Columns prefixed with (*) are optional — set to 0/NaN when absent so the
# model degrades gracefully rather than crashing during inference.

FEATURE_COLS = [
    # Calendar
    "hour_sin",
    "hour_cos",
    "day_of_week",
    "month",
    "is_weekend",
    "is_holiday",

    # PTF lags / rolling
    "ptf_lag_1h",
    "ptf_lag_24h",
    "ptf_lag_168h",
    "ptf_rolling_avg_24h",
    "ptf_rolling_max_24h",
    "ptf_rolling_min_24h",
    "ptf_rolling_avg_168h",

    # Demand / load
    "forecasted_load_mwh",        # LEP load forecast
    "forecasted_res_mwh",         # RES forecast
    "forecasted_residual_load_mwh",
    "actual_consumption_mwh",     # (*) RT actual demand
    "consumption_error",          # (*) actual − forecast

    # Generation mix
    "wind_generation_mwh",        # (*)
    "solar_generation_mwh",       # (*)
    "hydro_generation_mwh",       # (*)
    "gas_generation_mwh",         # (*)
    "total_generation_mwh",       # (*)
    "actual_renewable_ratio",
    "renewable_ratio",

    # Weather
    "temperature_celsius",        # (*)
    "wind_speed_kmh",             # (*)
    "shortwave_radiation",        # (*)
    "relative_humidity",          # (*)

    # Market structure
    "gop_volume_imbalance_mwh",
    "capacity_utilization_ratio",
    "net_imbalance_mwh",
    # smf_try ve ptf_smf_spread ÇIKARILDI — gün öncesi tahminlerde data leakage.
    # Aynı saatin SMF'i yalnızca gerçek zamanlı dengeleme sonrası bilinir.
    # Bunların yerine lag versiyonları kullanılıyor (aşağıda).

    # SMF lag özellikleri (leakage-free)
    # Weron & Misiorek (2008): "Forecasting spot electricity prices: A comparison..."
    # → T-24h ve T-168h SMF, yarının dengeleme baskısını öngörür
    "smf_try_lag_24h",
    "smf_try_lag_168h",

    # Cross-market signals (Maciejowska et al.)
    "gip_gop_price_spread",
    "arb_score_lag24",
    "yal_lag24",
    "yat_lag24",
    "net_dgp_lag24",

    # Supply shock
    "supply_shock_index",
    "total_outage_mwh",
    "supply_shock_trend_7d",
]
