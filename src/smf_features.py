"""
smf_features.py — Shared SMF feature engineering (2-stage: direction + price)

Called by both smf_trainer.py (full history) and smf_inference.py (single/batch
rows). Mirrors ptf_features.py's structure and conventions closely — see that
file for the calendar/renewable/supply-shock feature rationale, which is
identical here.

Direction (Stage 1) vs price (Stage 2): PRICE_FEATURE_COLS = DIRECTION_FEATURE_COLS
plus 3 predicted-probability columns (pred_proba_deficit/surplus/balance) that
the trainer/inference layer appends at runtime from Stage 1's output — see
smf_trainer.py's out-of-fold probability discipline (avoids Stage 1 leaking its
own training labels into Stage 2).
"""

import numpy as np
import pandas as pd

try:
    import holidays as _holidays_lib
    _TR_HOLIDAYS = _holidays_lib.Turkey(years=range(2020, 2031))
except ImportError:  # graceful degradation if package absent
    _TR_HOLIDAYS = {}

_DIRECTION_VALUES = ["ENERGY_DEFICIT", "ENERGY_SURPLUS", "IN_BALANCE"]


def build_smf_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shared SMF feature engineering for training and inference.

    Expects a DataFrame with a DatetimeIndex (Turkey local time, UTC+3) and at
    minimum the column ``smf_try``. All other columns are optional — the
    function falls back gracefully when source columns are absent.

    Additional columns used when present (all come from mart_smf_lag_features):
        smf_try_lag_5h, smf_try_lag_24h, smf_try_lag_168h
        smf_rolling_avg_24h, smf_rolling_max_24h, smf_rolling_min_24h, smf_rolling_avg_168h
        system_direction, system_direction_lag_5h
        ptf_try
        forecasted_load_mwh, forecasted_res_mwh, forecasted_residual_load_mwh
        capacity_utilization_ratio
        yal_lag24, yat_lag24, net_dgp_lag24, arb_score_lag24
        supply_shock_index, total_outage_mwh, supply_shock_trend_7d
    """
    df = df.copy()

    # ── Calendar features ─────────────────────────────────────────────────────
    df["hour"]        = df.index.hour
    df["day_of_week"] = df.index.dayofweek
    df["month"]       = df.index.month
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)

    df["is_holiday"] = df.index.date
    df["is_holiday"] = df["is_holiday"].apply(
        lambda d: 1 if d in _TR_HOLIDAYS else 0
    ).astype(int)

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # ── SMF lag / rolling features ────────────────────────────────────────────
    # If pre-computed lag columns already exist (from mart_smf_lag_features) use
    # them as-is. Otherwise compute from the series — useful during inference
    # when the caller builds a small window from recent history.
    if "smf_try_lag_5h" not in df.columns:
        df["smf_try_lag_5h"]   = df["smf_try"].shift(5)
    if "smf_try_lag_24h" not in df.columns:
        df["smf_try_lag_24h"]  = df["smf_try"].shift(24)
    if "smf_try_lag_168h" not in df.columns:
        df["smf_try_lag_168h"] = df["smf_try"].shift(168)

    if "smf_rolling_avg_24h" not in df.columns:
        df["smf_rolling_avg_24h"]  = df["smf_try"].shift(1).rolling(24).mean()
    if "smf_rolling_max_24h" not in df.columns:
        df["smf_rolling_max_24h"]  = df["smf_try"].shift(1).rolling(24).max()
    if "smf_rolling_min_24h" not in df.columns:
        df["smf_rolling_min_24h"]  = df["smf_try"].shift(1).rolling(24).min()
    if "smf_rolling_avg_168h" not in df.columns:
        df["smf_rolling_avg_168h"] = df["smf_try"].shift(1).rolling(168).mean()

    # ── System direction (persistence input feature — NOT the target) ────────
    # One-hot encode system_direction_lag_5h into fixed-name flag columns so
    # XGBoost gets numeric input regardless of which classes happen to appear
    # in a given training/inference window.
    if "system_direction_lag_5h" not in df.columns:
        df["system_direction_lag_5h"] = (
            df["system_direction"].shift(5) if "system_direction" in df.columns else None
        )
    for val in _DIRECTION_VALUES:
        col = f"direction_lag5h_{val.lower()}"
        df[col] = (df["system_direction_lag_5h"] == val).astype(int)

    # ── Supply shock (mirrors ptf_features.py exactly — same known quirk: only
    # populated when the caller's df already has these columns, e.g. via
    # mart_smf_forward_features; absent from mart_smf_lag_features/training) ──
    for col in ["supply_shock_index", "total_outage_mwh",
                "total_available_capacity_mwh", "total_aic_mwh"]:
        if col in df.columns:
            df[col] = df[col].ffill().fillna(0.0)

    # Only recompute from supply_shock_index when supply_shock_trend_7d isn't
    # already present — mart_smf_forward_features precomputes it correctly
    # from a real historical window (mart_supply_shock_index); recomputing
    # via .rolling(168) here on a small forward-inference batch (far short of
    # 168 rows) would silently overwrite it with near-all-NaN garbage.
    if "supply_shock_trend_7d" not in df.columns:
        if "supply_shock_index" in df.columns:
            df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(168).mean()
        else:
            df["supply_shock_trend_7d"] = 0.0

    return df


# ── Canonical feature lists ────────────────────────────────────────────────────
# Order is fixed so joblib-serialised models stay compatible across retrains.
DIRECTION_FEATURE_COLS = [
    # Calendar
    "hour_sin",
    "hour_cos",
    "day_of_week",
    "month",
    "is_weekend",
    "is_holiday",

    # SMF lags / rolling — smf_try_lag_5h is the genuinely-fresh signal
    # unlocked by the 2026-08-15 same-day ingestion fix (see
    # mart_smf_lag_features.sql); lag_24h/168h are the always-safe fallback.
    "smf_try_lag_5h",
    "smf_try_lag_24h",
    "smf_try_lag_168h",
    "smf_rolling_avg_24h",
    "smf_rolling_max_24h",
    "smf_rolling_min_24h",
    "smf_rolling_avg_168h",

    # System direction persistence (5h-lag, one-hot)
    "direction_lag5h_energy_deficit",
    "direction_lag5h_energy_surplus",
    "direction_lag5h_in_balance",

    # PTF for the target hour — safe, known a day ahead of SMF settlement.
    "ptf_try",

    # Demand / load / RES forecasts (day-ahead-known)
    "forecasted_load_mwh",
    "forecasted_res_mwh",
    "forecasted_residual_load_mwh",
    "capacity_utilization_ratio",

    # DGP cross-market signals — confirmed G+1 official lag (EPİAŞ Kurul Kararı
    # 10711, row 3), so 24h is the freshest safe version, same as PTF's model.
    "arb_score_lag24",
    "yal_lag24",
    "yat_lag24",
    "net_dgp_lag24",

    # Supply shock
    "supply_shock_index",
    "total_outage_mwh",
    "supply_shock_trend_7d",
]

# Stage 2 (price regressor) inputs = Stage 1 inputs + Stage 1's predicted class
# probabilities (appended by the trainer/inference layer at runtime — these
# columns don't come from the Gold mart).
PRICE_FEATURE_COLS = DIRECTION_FEATURE_COLS + [
    "pred_proba_energy_deficit",
    "pred_proba_energy_surplus",
    "pred_proba_in_balance",
]

# Fixed class order for the direction classifier — must match the label
# encoding used consistently across training and inference.
DIRECTION_CLASSES = _DIRECTION_VALUES
