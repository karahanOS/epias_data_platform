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

# ── Ordinal rank order (2026-08-25, ADR-09 Action Item 3) ─────────────────────
# The true deficit-to-surplus ordering — NOT the same as DIRECTION_CLASSES'
# own order below (["ENERGY_DEFICIT", "ENERGY_SURPLUS", "IN_BALANCE"], fixed
# for backward joblib/BQ column-name compatibility). ORDINAL_RANK is what the
# cumulative-binary ordinal classifier (see OrdinalDirectionClassifier) is
# actually built on.
ORDINAL_RANK = {"ENERGY_DEFICIT": 0, "IN_BALANCE": 1, "ENERGY_SURPLUS": 2}
_ORDINAL_ORDER = ["ENERGY_DEFICIT", "IN_BALANCE", "ENERGY_SURPLUS"]

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

    # Regime persistence length (2026-08-25) — how many consecutive hours,
    # ending at T-5 (same safety boundary as system_direction_lag_5h itself),
    # the system had already been in that direction. Gaps-and-islands via
    # cumcount(), same technique as the SQL version in
    # mart_smf_lag_features.sql/mart_smf_forward_features.sql — kept here only
    # as a fallback for ad-hoc windows that don't already carry the
    # precomputed mart column.
    if "direction_persistence_lag5h" not in df.columns:
        if "system_direction" in df.columns:
            _dir = df["system_direction"]
            _streak_id = (_dir != _dir.shift()).cumsum()
            _persistence = _dir.groupby(_streak_id).cumcount() + 1
            df["direction_persistence_lag5h"] = _persistence.shift(5)
        else:
            df["direction_persistence_lag5h"] = None

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

    # direction_persistence_lag5h (2026-08-25) — how many consecutive hours the
    # direction_lag5h_* state above had already held. Added after a sustained
    # ~16h+ deficit streak got badly under-predicted, then seeded-ablation-
    # tested: helps the PRICE regressor (MAE/sMAPE/bias/MASE all improved) but
    # actively HURTS the direction classifier specifically on long sustained
    # streaks (17h+ bucket: 80.5%->73.2% accuracy — worse on the exact case it
    # was meant to fix). Deliberately NOT in this list — see PRICE_FEATURE_COLS
    # below, which adds it directly for Stage 2 only. See
    # memory/smf_model_quality.md for the full ablation.

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

    # gip_gop_spread_lag24 (2026-08-24): wired into mart_smf_forward_features.sql
    # (mirrors arb_score_lag24/yal_lag24/yat_lag24's cross_lag join, already
    # forward-safe) but deliberately NOT added here — a fresh retrain with it
    # included came back worse on every held-out metric (accuracy 0.723->0.716,
    # price MASE 0.771->0.803, bias -16->-187 TL/MWh) than the same run without
    # it. Optuna has no fixed seed here so single-run search noise is a real
    # confound and this isn't necessarily a genuine negative signal — but until
    # a proper multi-run ablation says otherwise, leave it out of training. The
    # column stays available in the Gold mart for that ablation.

    # Supply shock
    "supply_shock_index",
    "total_outage_mwh",
    "supply_shock_trend_7d",
]

# Stage 2 (price regressor) inputs = Stage 1 inputs + Stage 1's predicted class
# probabilities (appended by the trainer/inference layer at runtime — these
# columns don't come from the Gold mart).
#
# 2026-08-25: direction_persistence_lag5h was deployed here price-only after
# the direction-classifier ablation showed it hurt Stage 1 on sustained
# streaks. That "price-only" config was never actually validated in isolation
# though — the ablation script that produced the promising price numbers
# (777 MAE / 0.757 MASE) put persistence into BOTH stages' feature lists, not
# price alone, so the improvement was confounded with Stage 1 also having the
# feature and producing different OOF probabilities. The real price-only
# deploy came back worse than no-persistence-at-all on every metric (MAE 799,
# MASE 0.778, bias -114 vs. 786/0.766/-80) — reverted. See
# memory/smf_model_quality.md for the full story before trying this again.
PRICE_FEATURE_COLS = DIRECTION_FEATURE_COLS + [
    "pred_proba_energy_deficit",
    "pred_proba_energy_surplus",
    "pred_proba_in_balance",
]

# Fixed class order for the direction classifier — must match the label
# encoding used consistently across training and inference.
DIRECTION_CLASSES = _DIRECTION_VALUES


class BlendedDirectionClassifier:
    """Weighted-average blend of two fitted classifiers' predict_proba output
    (2026-08-25, Stage 1 direction) — the blend weight (`alpha`, CatBoost's
    share) is chosen in smf_trainer.py via out-of-fold log-loss, not guessed,
    per the 2026-08-24 model comparison showing CatBoost and Random Forest
    make genuinely different mistakes (CatBoost better accuracy/F1, RF better
    macro AUC). Lives here rather than in smf_trainer.py so smf_inference.py
    (which only imports smf_features, not smf_trainer) can unpickle a model
    artifact containing one of these without a new import — the joblib
    artifact's `direction_model` value can be either a bare classifier or one
    of these; smf_inference.py's `_predict_both_stages()` only ever calls
    `.predict_proba()`, so it doesn't need to know which."""

    def __init__(self, model_a, model_b, alpha: float):
        self.model_a = model_a
        self.model_b = model_b
        self.alpha = alpha

    def predict_proba(self, X):
        return (self.alpha * self.model_a.predict_proba(X)
                + (1 - self.alpha) * self.model_b.predict_proba(X))


def ordinal_cumulative_to_class_proba(p_ge_balance, p_ge_surplus, direction_classes):
    """Converts two cumulative probabilities — P(rank >= IN_BALANCE) and
    P(rank >= ENERGY_SURPLUS) — into the 3 class probabilities, reordered into
    `direction_classes`'s own column order. Shared by OrdinalDirectionClassifier
    and smf_trainer.py's OOF generator so there's one definition of the
    ordinal decode, not two that could drift apart.

    Monotonicity isn't guaranteed since the two binary models are fit
    independently — p_ge_surplus is clipped to never exceed p_ge_balance
    before differencing, otherwise IN_BALANCE's derived probability could go
    negative."""
    p_ge_surplus = np.minimum(p_ge_surplus, p_ge_balance)
    p_deficit = 1 - p_ge_balance
    p_balance = p_ge_balance - p_ge_surplus
    p_surplus = p_ge_surplus

    ordinal_proba = np.column_stack([p_deficit, p_balance, p_surplus])
    col_order = [_ORDINAL_ORDER.index(c) for c in direction_classes]
    return ordinal_proba[:, col_order]


class OrdinalDirectionClassifier:
    """Two cumulative binary classifiers (proportional-odds / ordinal
    regression), instead of one unordered 3-way softmax — 2026-08-25, ADR-09
    Action Item 3 (plans/09-smf-forecaster-validation-methodology.md). The
    direction classes have a real order (ENERGY_DEFICIT < IN_BALANCE <
    ENERGY_SURPLUS) that plain MultiClass loss doesn't know about: predicting
    deficit when the truth is surplus costs exactly the same as predicting
    deficit when the truth is balance under that loss, even though the former
    is the miss DUY Madde 28's asymmetric settlement formula punishes hardest
    (the same Jensen's-inequality concern already on record in ADR-08).

    model_ge_balance predicts P(rank >= IN_BALANCE, i.e. NOT deficit).
    model_ge_surplus predicts P(rank >= ENERGY_SURPLUS, i.e. IS surplus).
    predict_proba() decodes these back into the 3 class probabilities, in
    DIRECTION_CLASSES order, so every downstream consumer (Stage 2,
    smf_inference.py, the BQ proba_* columns) sees the exact same interface a
    plain multiclass classifier would produce — the ordinal structure is
    entirely internal to how this wrapper is built."""

    def __init__(self, model_ge_balance, model_ge_surplus, direction_classes):
        self.model_ge_balance = model_ge_balance
        self.model_ge_surplus = model_ge_surplus
        self.direction_classes = direction_classes

    def predict_proba(self, X):
        p_ge_balance = self.model_ge_balance.predict_proba(X)[:, 1]
        p_ge_surplus = self.model_ge_surplus.predict_proba(X)[:, 1]
        return ordinal_cumulative_to_class_proba(p_ge_balance, p_ge_surplus, self.direction_classes)
