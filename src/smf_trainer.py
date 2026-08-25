"""
smf_trainer.py — 2-Stage XGBoost SMF Training Job (direction classifier + price regressor)
============================================================================================
Cadence : Weekly via Airflow — task_id: train_smf_model
Purpose : Full retraining on all historical data. Saves model artifact to GCS.
Runtime : 10-30 minutes (Optuna adds time on top of base training, x2 for two stages).

Mirrors ptf_trainer.py's structure closely (Optuna + TimeSeriesSplit CV, recency
weighting, MASE-vs-naive benchmark). The addition here is the 2-stage design:
Stage 1 (CatBoostClassifier) predicts system_direction; Stage 2 (XGBRegressor)
predicts smf_try using Stage 1's class probabilities as extra input features.

Stage 1 was XGBoost until 2026-08-16, when a direct comparison against
CatBoost/LightGBM/Random Forest (src/smf_direction_model_comparison.py) showed
CatBoost with meaningfully better macro F1 (0.47 vs 0.44) and macro AUC (0.80
vs 0.75) on real backtest data — see that script's results CSV.

Stage-1-into-Stage-2 leakage: Stage 2's training features use Stage 1's
OUT-OF-FOLD probabilities (via TimeSeriesSplit), not the fully-fit classifier's
in-sample predict_proba() — otherwise Stage 2 would implicitly see Stage 1
"cheating" on the training labels it already memorized, inflating backtest
accuracy in a way that wouldn't hold at real inference time (where Stage 1
genuinely has never seen the target hour). The final deployed Stage 1 model is
still fit on the FULL training set — only the features fed into Stage 2's
training use the OOF variant.
"""

import logging
import tempfile
import warnings
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from catboost import CatBoostClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import mean_absolute_error, log_loss, accuracy_score
from sklearn.model_selection import TimeSeriesSplit

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _OPTUNA_AVAILABLE = True
except ImportError:
    _OPTUNA_AVAILABLE = False

from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, GCS_BUCKET, get_bq_client, get_gcs_client
from smf_features import (
    build_smf_features, DIRECTION_FEATURE_COLS, PRICE_FEATURE_COLS, DIRECTION_CLASSES,
    BlendedDirectionClassifier, OrdinalDirectionClassifier, ordinal_cumulative_to_class_proba,
    ORDINAL_RANK,
)

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SMFTrainer")

MODEL_GCS_PATH                = "models/smf_xgb_model.joblib"
DIRECTION_IMPORTANCE_GCS_PATH = "models/smf_direction_importance.csv"
PRICE_IMPORTANCE_GCS_PATH     = "models/smf_price_importance.csv"
LOCAL_TMP                     = tempfile.gettempdir()

OPTUNA_N_TRIALS = 20

# Stage 1 architecture switch (2026-08-25, ADR-09 —
# plans/09-smf-forecaster-validation-methodology.md). "plain" is the deployed,
# known-good default (single CatBoost multiclass classifier). "blend"
# (CatBoost+RF, see BlendedDirectionClassifier) was implemented and validated
# on a single window — net negative there (accuracy +0.3pt, price meaningfully
# worse) — not yet re-checked through smf_walk_forward.py. "ordinal" (two
# cumulative binary classifiers, see OrdinalDirectionClassifier) is the newest,
# targets the asymmetric-miss problem directly rather than adding another
# input signal. Default "plain" so the weekly scheduled DAG
# (epias_smf_training_weekly) keeps producing the known-good model until/
# unless someone deliberately flips this AND re-validates through the
# walk-forward harness first — without this switch, train_direction_classifier()
# would silently start deploying an unvalidated architecture change on the
# next scheduled retrain. See memory/smf_model_quality.md for the full history.
DIRECTION_ARCHITECTURE = "plain"  # "plain" | "blend" | "ordinal"

# 2026-08-25: optuna.create_study() previously had no fixed sampler seed, so
# back-to-back runs on IDENTICAL data/features produced meaningfully different
# hyperparameters purely from TPE search-trajectory randomness (observed: price
# regressor bias swung -16 -> -187 -> -75 TL/MWh across 3 same-day runs, two of
# them on the exact same feature set — see memory/smf_model_quality.md). Fixing
# this seed is what makes single-run before/after feature comparisons on this
# trainer trustworthy at all.
OPTUNA_SEED = 42

# Same half-life as ptf_trainer.py — see that file's docstring for the
# 2026-08-02 backtest rationale (90 days balances regime adaptation against
# still learning yearly seasonality from full history).
RECENCY_HALFLIFE_DAYS = 90

_PROBA_COLS = [f"pred_proba_{c.lower()}" for c in DIRECTION_CLASSES]


# ── DATA EXTRACTION ───────────────────────────────────────────────────────────

def extract_training_data() -> pd.DataFrame:
    """Pull full history from mart_smf_lag_features (Gold layer) for training."""
    logger.info("Pulling full training data from mart_smf_lag_features...")
    client = get_bq_client()

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_smf_lag_features`
        WHERE date IS NOT NULL
          AND smf_try IS NOT NULL
        ORDER BY date, hour
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Pulled {len(df):,} rows ({df.memory_usage(deep=True).sum() / 1e6:.1f} MB)")

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = pd.to_datetime(df["date"].astype(str)) + pd.to_timedelta(df["hour"], unit="h")

    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    logger.info(f"Date range: {df.index.min()} -> {df.index.max()}")
    return df


# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build full feature set. Requires full history for lag correctness."""
    logger.info("Engineering features...")
    df = build_smf_features(df)

    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if not nan_cols.empty:
        logger.info(f"NaN counts before dropna:\n{nan_cols.to_string()}")

    # Drop rows where target or core lag features are NaN (warm-up window), and
    # where the direction label itself is missing (can't train Stage 1 without it).
    required = [c for c in ["smf_try", "smf_try_lag_24h", "smf_try_lag_168h", "system_direction"]
                if c in df.columns]
    before = len(df)
    df.dropna(subset=required, inplace=True)
    logger.info(f"Dropped {before - len(df):,} NaN rows (lag warm-up / missing direction). "
                f"Training rows: {len(df):,}")
    return df


# ── SAMPLE WEIGHTING ──────────────────────────────────────────────────────────

def compute_recency_weights(index: pd.DatetimeIndex,
                             halflife_days: float = RECENCY_HALFLIFE_DAYS) -> pd.Series:
    """Identical to ptf_trainer.py's — see that docstring."""
    age_days = (index.max() - index).total_seconds() / 86400
    return pd.Series(0.5 ** (age_days / halflife_days), index=index)


def _to_float(df_slice: pd.DataFrame) -> pd.DataFrame:
    return df_slice.apply(pd.to_numeric, errors="coerce").fillna(0).astype("float64")


# ── STAGE 1: DIRECTION CLASSIFIER ─────────────────────────────────────────────

_DEFAULT_CLF_PARAMS = {
    "iterations":          600,
    "learning_rate":       0.05,
    "depth":               5,
    "l2_leaf_reg":         3.0,
    "bagging_temperature": 0.5,
    "random_strength":     0.5,
}


def _catboost_classifier(params: dict, n_classes: int) -> CatBoostClassifier:
    return CatBoostClassifier(
        **params,
        loss_function="MultiClass",
        classes_count=n_classes,
        auto_class_weights="Balanced",
        random_state=42,
        verbose=False,
    )


def _optimise_classifier_hyperparams(X: pd.DataFrame, y: np.ndarray,
                                      sample_weight: pd.Series,
                                      n_trials: int = OPTUNA_N_TRIALS) -> dict:
    """Optuna time-series CV for the direction classifier — same TimeSeriesSplit
    discipline as ptf_trainer.py's regressor search, objective = mean log-loss."""
    tscv = TimeSeriesSplit(n_splits=3)
    X_f = _to_float(X)
    n_classes = len(DIRECTION_CLASSES)

    def objective(trial):
        params = {
            "iterations":          trial.suggest_int("iterations", 200, 1000),
            "depth":               trial.suggest_int("depth", 3, 8),
            "learning_rate":       trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "l2_leaf_reg":         trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
            "random_strength":     trial.suggest_float("random_strength", 0.0, 2.0),
        }
        scores = []
        for tr_idx, val_idx in tscv.split(X_f):
            m = _catboost_classifier(params, n_classes)
            m.fit(X_f.iloc[tr_idx], y[tr_idx], sample_weight=sample_weight.iloc[tr_idx])
            proba = m.predict_proba(X_f.iloc[val_idx])
            scores.append(log_loss(y[val_idx], proba, labels=list(range(n_classes))))
        return float(np.mean(scores))

    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=OPTUNA_SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    logger.info(f"Optuna best log-loss (CV, direction): {study.best_value:.4f} | "
                f"params: {study.best_params}")
    return study.best_params


def _generate_oof_direction_probabilities(X: pd.DataFrame, y: np.ndarray,
                                           sample_weight: pd.Series,
                                           params: dict) -> pd.DataFrame:
    """Out-of-fold class probabilities via TimeSeriesSplit — see module
    docstring for why Stage 2 must train on these, not the final model's
    in-sample predict_proba(). Rows before the first fold's validation start
    have no OOF coverage and are returned as NaN (dropped before Stage 2
    training)."""
    tscv = TimeSeriesSplit(n_splits=3)
    X_f = _to_float(X)
    n_classes = len(DIRECTION_CLASSES)

    oof = pd.DataFrame(np.nan, index=X.index, columns=_PROBA_COLS)

    for tr_idx, val_idx in tscv.split(X_f):
        m = _catboost_classifier(params, n_classes)
        m.fit(X_f.iloc[tr_idx], y[tr_idx], sample_weight=sample_weight.iloc[tr_idx])
        proba = m.predict_proba(X_f.iloc[val_idx])
        oof.iloc[val_idx] = proba

    covered = oof.dropna().shape[0]
    logger.info(f"OOF direction probabilities: {covered:,}/{len(oof):,} rows covered "
                f"(earliest rows lack preceding folds — expected).")
    return oof


# ── STAGE 1B: RANDOM FOREST (blended with CatBoost — 2026-08-25) ──────────────
# The 2026-08-24 model comparison (src/smf_direction_model_comparison.py) found
# CatBoost and Random Forest make genuinely different mistakes on this problem
# — CatBoost wins accuracy/F1, RF wins macro AUC by ~4pt — rather than RF being
# a strictly worse CatBoost. These are the same params that comparison used, no
# separate Optuna search for RF (keeps this a blend of two independently-good
# models, not a second full hyperparameter-tuning pass).
_DEFAULT_RF_PARAMS = dict(
    n_estimators=300, max_depth=10, min_samples_leaf=5,
    class_weight="balanced", n_jobs=-1, random_state=42,
)


def _random_forest_classifier() -> RandomForestClassifier:
    return RandomForestClassifier(**_DEFAULT_RF_PARAMS)


def _generate_oof_rf_probabilities(X: pd.DataFrame, y: np.ndarray,
                                    sample_weight: pd.Series) -> pd.DataFrame:
    """Same OOF discipline as _generate_oof_direction_probabilities(), for RF.
    Uses the identical TimeSeriesSplit(n_splits=3) over the same X, so fold
    boundaries — and therefore NaN coverage — match CatBoost's OOF frame
    exactly, which _select_blend_alpha() relies on.

    Defensive padding: TimeSeriesSplit's earliest fold trains on a much
    smaller prefix than later ones, and IN_BALANCE is rare enough historically
    (~1-2% of rows) that a fold's training slice can end up with fewer than
    all 3 classes. Unlike CatBoost (told classes_count=n_classes explicitly),
    plain RandomForestClassifier infers classes_ from what it saw, so
    predict_proba can silently return fewer than 3 columns — pad any missing
    class's column with 0 probability rather than let that misalign against
    the fixed 3-column _PROBA_COLS layout."""
    tscv = TimeSeriesSplit(n_splits=3)
    X_f = _to_float(X)
    n_classes = len(DIRECTION_CLASSES)

    oof = pd.DataFrame(np.nan, index=X.index, columns=_PROBA_COLS)

    for tr_idx, val_idx in tscv.split(X_f):
        m = _random_forest_classifier()
        m.fit(X_f.iloc[tr_idx], y[tr_idx], sample_weight=sample_weight.iloc[tr_idx])
        proba = m.predict_proba(X_f.iloc[val_idx])
        if proba.shape[1] != n_classes:
            padded = np.zeros((proba.shape[0], n_classes))
            for col_idx, cls in enumerate(m.classes_):
                padded[:, int(cls)] = proba[:, col_idx]
            proba = padded
        oof.iloc[val_idx] = proba

    covered = oof.dropna().shape[0]
    logger.info(f"OOF RF probabilities: {covered:,}/{len(oof):,} rows covered.")
    return oof


def _select_blend_alpha(cb_oof: pd.DataFrame, rf_oof: pd.DataFrame, y: np.ndarray,
                         index: pd.Index) -> tuple:
    """Grid-search the CatBoost/RF blend weight (alpha = CatBoost's share)
    that minimizes OOF log-loss — chosen from data, not guessed. Only scores
    rows both models have OOF coverage for (fold boundaries match by
    construction, so this is normally all-or-nothing per row anyway)."""
    covered = cb_oof.dropna().index.intersection(rf_oof.dropna().index)
    y_series = pd.Series(y, index=index)
    y_covered = y_series.loc[covered].values
    cb_p = cb_oof.loc[covered].values
    rf_p = rf_oof.loc[covered].values
    n_classes = cb_p.shape[1]

    best_alpha, best_loss = 1.0, float("inf")
    for alpha in np.linspace(0.0, 1.0, 11):
        blended = alpha * cb_p + (1 - alpha) * rf_p
        loss = log_loss(y_covered, blended, labels=list(range(n_classes)))
        if loss < best_loss:
            best_alpha, best_loss = float(alpha), loss

    logger.info(f"Direction blend: CatBoost weight={best_alpha:.2f} "
                f"(OOF log-loss={best_loss:.4f}, n={len(covered):,})")
    return best_alpha, best_loss


# ── STAGE 1C: ORDINAL (2 cumulative binary classifiers, blended with CatBoost
# multiclass — 2026-08-25, ADR-09 Action Item 3) ──────────────────────────────
# See OrdinalDirectionClassifier's docstring in smf_features.py for the full
# rationale. Reuses the SAME Optuna-tuned hyperparameters as the multiclass
# classifier (loss_function swapped to Logloss, classes_count dropped) rather
# than running a separate search per binary sub-model — keeps this a
# comparable-cost alternative architecture, not a second full tuning pass.

def _ordinal_binary_classifier(params: dict) -> CatBoostClassifier:
    return CatBoostClassifier(
        **params,
        loss_function="Logloss",
        auto_class_weights="Balanced",
        random_state=42,
        verbose=False,
    )


def _generate_oof_ordinal_probabilities(X: pd.DataFrame, y_rank: np.ndarray,
                                         sample_weight: pd.Series, params: dict) -> pd.DataFrame:
    """OOF probabilities for the ordinal architecture — same TimeSeriesSplit
    discipline as _generate_oof_direction_probabilities(), decoded via
    ordinal_cumulative_to_class_proba() into the same 3-column _PROBA_COLS
    layout every other architecture produces."""
    tscv = TimeSeriesSplit(n_splits=3)
    X_f = _to_float(X)
    oof = pd.DataFrame(np.nan, index=X.index, columns=_PROBA_COLS)

    y_ge_balance = (y_rank >= 1).astype(int)
    y_ge_surplus = (y_rank >= 2).astype(int)

    for tr_idx, val_idx in tscv.split(X_f):
        m_bal = _ordinal_binary_classifier(params)
        m_bal.fit(X_f.iloc[tr_idx], y_ge_balance[tr_idx], sample_weight=sample_weight.iloc[tr_idx])
        m_sur = _ordinal_binary_classifier(params)
        m_sur.fit(X_f.iloc[tr_idx], y_ge_surplus[tr_idx], sample_weight=sample_weight.iloc[tr_idx])

        p_bal = m_bal.predict_proba(X_f.iloc[val_idx])[:, 1]
        p_sur = m_sur.predict_proba(X_f.iloc[val_idx])[:, 1]
        oof.iloc[val_idx] = ordinal_cumulative_to_class_proba(p_bal, p_sur, DIRECTION_CLASSES)

    covered = oof.dropna().shape[0]
    logger.info(f"OOF ordinal direction probabilities: {covered:,}/{len(oof):,} rows covered.")
    return oof


def train_direction_classifier(df: pd.DataFrame) -> tuple:
    """Returns (final_model, oof_probabilities_df, y_encoded, accuracy_metrics).
    Stage 1 architecture controlled by DIRECTION_ARCHITECTURE ("plain" |
    "blend" | "ordinal") — see that constant's comment above."""
    features = [c for c in DIRECTION_FEATURE_COLS if c in df.columns]
    y_raw = df["system_direction"].values
    y = pd.Categorical(y_raw, categories=DIRECTION_CLASSES).codes  # fixed encoding

    X = df[features]
    sample_weight = compute_recency_weights(df.index)

    split_date = df.index.max() - pd.Timedelta(days=30)
    train_mask = df.index < split_date
    test_mask  = ~train_mask

    X_train, y_train = X[train_mask], y[train_mask]
    X_test,  y_test  = X[test_mask],  y[test_mask]
    y_raw_train = y_raw[train_mask]
    w_train = sample_weight[train_mask]

    logger.info(f"Direction — Train: {len(X_train):,} rows | Test (last 30d): {len(X_test):,} rows | "
                f"Features: {len(features)} | Architecture: {DIRECTION_ARCHITECTURE}")

    if _OPTUNA_AVAILABLE:
        logger.info(f"Running Optuna ({OPTUNA_N_TRIALS} trials) for direction classifier...")
        best_params = _optimise_classifier_hyperparams(X_train, y_train, w_train)
    else:
        logger.warning("Optuna not installed — using default classifier hyperparameters.")
        best_params = dict(_DEFAULT_CLF_PARAMS)

    n_classes = len(DIRECTION_CLASSES)
    alpha = None

    if DIRECTION_ARCHITECTURE == "ordinal":
        y_rank_train = np.array([ORDINAL_RANK[v] for v in y_raw_train])
        oof_proba = _generate_oof_ordinal_probabilities(X_train, y_rank_train, w_train, best_params)

        y_ge_balance = (y_rank_train >= 1).astype(int)
        y_ge_surplus = (y_rank_train >= 2).astype(int)

        m_bal = _ordinal_binary_classifier(best_params)
        m_bal.fit(_to_float(X_train), y_ge_balance, sample_weight=w_train)
        m_sur = _ordinal_binary_classifier(best_params)
        m_sur.fit(_to_float(X_train), y_ge_surplus, sample_weight=w_train)

        final_model = OrdinalDirectionClassifier(m_bal, m_sur, DIRECTION_CLASSES)
        imp_model = m_bal  # P(not deficit) importances — the primary threshold
    else:
        cb_final = _catboost_classifier(best_params, n_classes)
        cb_final.fit(_to_float(X_train), y_train, sample_weight=w_train)

        if DIRECTION_ARCHITECTURE == "blend":
            cb_oof = _generate_oof_direction_probabilities(X_train, y_train, w_train, best_params)
            rf_oof = _generate_oof_rf_probabilities(X_train, y_train, w_train)
            alpha, _ = _select_blend_alpha(cb_oof, rf_oof, y_train, X_train.index)
            oof_proba = alpha * cb_oof + (1 - alpha) * rf_oof

            rf_final = _random_forest_classifier()
            rf_final.fit(_to_float(X_train), y_train, sample_weight=w_train)

            final_model = BlendedDirectionClassifier(cb_final, rf_final, alpha)
        else:
            alpha = 1.0
            oof_proba = _generate_oof_direction_probabilities(X_train, y_train, w_train, best_params)
            final_model = cb_final

        imp_model = cb_final

    # Held-out accuracy vs. naive "same direction as T-24h" baseline.
    test_proba = final_model.predict_proba(_to_float(X_test))
    test_pred  = test_proba.argmax(axis=1)
    acc = accuracy_score(y_test, test_pred)

    naive_lookup_ts = df.index[test_mask] - pd.Timedelta(hours=24)
    naive_direction_raw = df["system_direction"].reindex(naive_lookup_ts).values
    naive_direction_24h = pd.Categorical(naive_direction_raw, categories=DIRECTION_CLASSES).codes
    valid_naive = naive_direction_24h != -1
    naive_acc = accuracy_score(y_test[valid_naive], naive_direction_24h[valid_naive]) if valid_naive.any() else float("nan")

    # Severe-miss rate: predicted/actual land on OPPOSITE ordinal extremes
    # (deficit called surplus or vice versa) — the specific error class ADR-09's
    # ordinal architecture targets, and DUY Madde 28's asymmetric settlement
    # formula punishes hardest. Computed for every architecture (not just
    # "ordinal") so it's directly comparable across them.
    _deficit_code = list(DIRECTION_CLASSES).index("ENERGY_DEFICIT")
    _surplus_code = list(DIRECTION_CLASSES).index("ENERGY_SURPLUS")
    severe_miss_rate = float((
        ((y_test == _deficit_code) & (test_pred == _surplus_code)) |
        ((y_test == _surplus_code) & (test_pred == _deficit_code))
    ).mean())

    alpha_str = f", alpha={alpha:.2f}" if DIRECTION_ARCHITECTURE == "blend" else ""
    logger.info(f"Direction classifier ({DIRECTION_ARCHITECTURE}{alpha_str}) — "
                f"accuracy: {acc:.3f} | naive T-24h accuracy: {naive_acc:.3f} | "
                f"severe miss rate: {severe_miss_rate:.3f}")

    imp_df = pd.DataFrame({
        "col_name": features,
        "feature_importance_vals": imp_model.feature_importances_,
    }).sort_values("feature_importance_vals", ascending=False)

    metrics = {
        "accuracy": acc, "naive_accuracy": naive_acc, "blend_alpha": alpha,
        "severe_miss_rate": severe_miss_rate,
    }
    return final_model, oof_proba, features, imp_df, metrics


# ── STAGE 2: PRICE REGRESSOR ───────────────────────────────────────────────────

_DEFAULT_REG_PARAMS = {
    "n_estimators":     800,
    "learning_rate":    0.03,
    "max_depth":        6,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "gamma":            0.1,
    "reg_alpha":        0.1,
    "reg_lambda":       2.0,
    "random_state":     42,
    "objective":        "reg:squarederror",
    "early_stopping_rounds": 50,
}


def _optimise_regressor_hyperparams(X_train: pd.DataFrame, y_train: pd.Series,
                                     sample_weight: pd.Series,
                                     n_trials: int = OPTUNA_N_TRIALS) -> dict:
    """Identical structure to ptf_trainer.py's _optimise_hyperparams()."""
    tscv = TimeSeriesSplit(n_splits=3)
    X_f = _to_float(X_train)

    def objective(trial):
        params = {
            "n_estimators":      trial.suggest_int("n_estimators", 200, 1200),
            "max_depth":         trial.suggest_int("max_depth", 3, 10),
            "learning_rate":     trial.suggest_float("learning_rate", 0.005, 0.2, log=True),
            "subsample":         trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.4, 1.0),
            "min_child_weight":  trial.suggest_int("min_child_weight", 1, 10),
            "gamma":             trial.suggest_float("gamma", 0.0, 1.0),
            "reg_alpha":         trial.suggest_float("reg_alpha", 0.0, 1.0),
            "reg_lambda":        trial.suggest_float("reg_lambda", 0.5, 5.0),
            "random_state":      42,
            "objective":         "reg:squarederror",
        }
        scores = []
        for tr_idx, val_idx in tscv.split(X_f):
            m = xgb.XGBRegressor(**params)
            m.fit(X_f.iloc[tr_idx], y_train.iloc[tr_idx],
                  sample_weight=sample_weight.iloc[tr_idx], verbose=False)
            preds = m.predict(X_f.iloc[val_idx])
            scores.append(mean_absolute_error(y_train.iloc[val_idx], preds))
        return float(np.mean(scores))

    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=OPTUNA_SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    logger.info(f"Optuna best MAE (CV, price): {study.best_value:.2f} TL | params: {study.best_params}")
    return study.best_params


def train_price_regressor(df: pd.DataFrame, oof_proba: pd.DataFrame) -> tuple:
    """Trains Stage 2 on rows with OOF direction-probability coverage only
    (see _generate_oof_direction_probabilities' docstring)."""
    target = "smf_try"

    df_with_proba = df.join(oof_proba, how="left")
    covered = df_with_proba.dropna(subset=_PROBA_COLS)
    logger.info(f"Price regressor training set (OOF-covered rows): {len(covered):,}/{len(df):,}")

    features = [c for c in PRICE_FEATURE_COLS if c in covered.columns]

    split_date = covered.index.max() - pd.Timedelta(days=30)
    X_train = _to_float(covered.loc[covered.index <  split_date, features])
    y_train = covered.loc[covered.index <  split_date, target]
    X_test  = _to_float(covered.loc[covered.index >= split_date, features])
    y_test  = covered.loc[covered.index >= split_date, target]

    sample_weight = compute_recency_weights(X_train.index)

    logger.info(f"Price — Train: {len(X_train):,} rows | Test (last 30d): {len(X_test):,} rows | "
                f"Features: {len(features)}")

    if _OPTUNA_AVAILABLE:
        logger.info(f"Running Optuna ({OPTUNA_N_TRIALS} trials) for price regressor...")
        best_params = _optimise_regressor_hyperparams(X_train, y_train, sample_weight)
        best_params.pop("early_stopping_rounds", None)
        best_params.pop("random_state", None)
    else:
        logger.warning("Optuna not installed — using default regressor hyperparameters.")
        best_params = {k: v for k, v in _DEFAULT_REG_PARAMS.items()
                       if k not in ("early_stopping_rounds", "random_state")}

    model = xgb.XGBRegressor(**best_params, early_stopping_rounds=50, random_state=42)
    model.fit(
        X_train, y_train,
        sample_weight=sample_weight,
        eval_set=[(X_test, y_test)],
        verbose=100,
    )

    preds = pd.Series(model.predict(X_test), index=y_test.index)
    mae   = mean_absolute_error(y_test, preds)
    err   = y_test - preds
    bias  = -err.mean()

    denom = (y_test.abs() + preds.abs()) / 2
    smape = err.abs().div(denom.replace(0, float("nan"))).mean()

    naive_pred = covered.loc[y_test.index, "smf_try_lag_24h"]
    naive_mae  = mean_absolute_error(y_test, naive_pred)
    mase = mae / naive_mae if naive_mae > 0 else float("nan")

    logger.info(
        f"Price regressor — MAE: {mae:.2f} TL/MWh | sMAPE: {smape*100:.2f}% | "
        f"Bias: {bias:+.2f} TL/MWh | MASE (vs T-24h naive): {mase:.3f}"
    )

    imp_df = pd.DataFrame({
        "col_name": features,
        "feature_importance_vals": model.feature_importances_,
    }).sort_values("feature_importance_vals", ascending=False)

    metrics = {"mae": mae, "smape": smape, "bias": bias, "mase": mase}
    return model, features, imp_df, metrics


# ── GCS UPLOAD ────────────────────────────────────────────────────────────────

def upload_to_gcs(local_path: str, gcs_path: str) -> None:
    # get_gcs_client() (not bare storage.Client()) — matches get_bq_client()'s
    # credential resolution (config.py's _resolve_credentials): works locally
    # via credentials/gcp-key.json AND in production via ADC, unlike a bare
    # storage.Client() call which only works where ADC happens to be configured.
    bucket = get_gcs_client().bucket(GCS_BUCKET)
    bucket.blob(gcs_path).upload_from_filename(local_path)
    logger.info(f"Uploaded {local_path} -> gs://{GCS_BUCKET}/{gcs_path}")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run():
    df_raw = extract_training_data()

    MIN_ROWS = 168 + (30 * 24) + 48
    if len(df_raw) < MIN_ROWS:
        logger.warning(
            f"Insufficient data: {len(df_raw):,} rows, need {MIN_ROWS:,}. "
            f"Backfill more history first. Skipping."
        )
        return

    df_engineered = engineer_features(df_raw)

    direction_model, oof_proba, direction_features, direction_imp, direction_metrics = \
        train_direction_classifier(df_engineered)
    price_model, price_features, price_imp, price_metrics = \
        train_price_regressor(df_engineered, oof_proba)

    logger.info(
        f"Training complete — Direction accuracy: {direction_metrics['accuracy']:.3f} "
        f"(naive: {direction_metrics['naive_accuracy']:.3f}) | "
        f"Price MASE: {price_metrics['mase']:.3f}"
    )

    local_model = f"{LOCAL_TMP}/smf_xgb_model.joblib"
    local_direction_imp = f"{LOCAL_TMP}/smf_direction_importance.csv"
    local_price_imp     = f"{LOCAL_TMP}/smf_price_importance.csv"

    joblib.dump({
        "direction_model":    direction_model,
        "price_model":        price_model,
        "direction_classes":  DIRECTION_CLASSES,
        "direction_features": direction_features,
        "price_features":     price_features,
    }, local_model)
    direction_imp.to_csv(local_direction_imp, index=False)
    price_imp.to_csv(local_price_imp, index=False)

    upload_to_gcs(local_model, MODEL_GCS_PATH)
    upload_to_gcs(local_direction_imp, DIRECTION_IMPORTANCE_GCS_PATH)
    upload_to_gcs(local_price_imp, PRICE_IMPORTANCE_GCS_PATH)
    logger.info("Training job complete.")


if __name__ == "__main__":
    run()
