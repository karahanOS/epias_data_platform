"""
smf_direction_model_comparison.py — SMF Direction Classifier Model Comparison
================================================================================
Trains and evaluates 4 classifiers on the same train/test split to benchmark
XGBoost (production Stage-1 model) against alternatives, mirroring
model_comparison.py's pattern for the PTF regressor.

Models:
    1. Random Forest — non-boosted ensemble baseline
    2. LightGBM      — gradient boosting competitor to XGBoost
    3. CatBoost      — gradient boosting competitor, native categorical handling
    4. XGBoost       — production model (smf_trainer.py's default params, no Optuna)

Run:
    python src/smf_direction_model_comparison.py
Output:
    models/smf_direction_model_comparison_results.csv
"""

import importlib.util
import logging
import warnings
import sys
import os

import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.preprocessing import label_binarize

# Availability checked via find_spec (no import) — confirmed 2026-08-16 that
# actually importing lightgbm and catboost into the SAME process (regardless
# of instantiation order, even if one is never fit) triggers an intermittent
# native access-violation crash on Windows when LightGBM later builds its
# Dataset. Root cause not fully pinned down; the reliable fix is to never
# import either module unless that specific model is about to be trained —
# see the lazy imports inside build_models()'s lambdas below, and run(only=...)
# for training one model per process (invoke this script once per model name).
_LGB_AVAILABLE = importlib.util.find_spec("lightgbm") is not None
_CATBOOST_AVAILABLE = importlib.util.find_spec("catboost") is not None
if not _LGB_AVAILABLE:
    logging.warning("LightGBM not installed. Run: pip install lightgbm")
if not _CATBOOST_AVAILABLE:
    logging.warning("CatBoost not installed. Run: pip install catboost")

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, get_bq_client
from smf_features import build_smf_features, DIRECTION_FEATURE_COLS, DIRECTION_CLASSES

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SMFDirectionModelComparison")

RESULTS_PATH = "models/smf_direction_model_comparison_results.csv"


# ── DATA ──────────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    logger.info("Loading data from BigQuery...")
    client = get_bq_client()
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_smf_lag_features`
        WHERE date IS NOT NULL AND smf_try IS NOT NULL
        ORDER BY date, hour
    """
    df = client.query(query).to_dataframe()

    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_localize(None)
    else:
        df["datetime"] = (
            pd.to_datetime(df["date"].astype(str))
            + pd.to_timedelta(df["hour"], unit="h")
        )
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    logger.info(f"Loaded {len(df):,} rows | {df.index.min()} -> {df.index.max()}")
    return df


def prepare(df: pd.DataFrame):
    """Feature engineering + train/test split identical to smf_trainer.py's
    train_direction_classifier()."""
    df = build_smf_features(df)

    required = [c for c in ["smf_try", "smf_try_lag_24h", "smf_try_lag_168h", "system_direction"]
                if c in df.columns]
    df.dropna(subset=required, inplace=True)

    features = [c for c in DIRECTION_FEATURE_COLS if c in df.columns]
    # .codes defaults to int8 (only 3 categories) — LightGBM's native label
    # setter crashes on Windows with a raw access violation on that narrow
    # dtype (confirmed 2026-08-16). int32 is safe across all 4 libraries here.
    y = pd.Categorical(df["system_direction"], categories=DIRECTION_CLASSES).codes.astype(np.int32)

    def _to_float(d):
        return d.apply(pd.to_numeric, errors="coerce").fillna(0).astype("float64")

    split_date = df.index.max() - pd.Timedelta(days=30)
    train_mask = df.index < split_date
    test_mask = ~train_mask

    X_train = _to_float(df.loc[train_mask, features])
    y_train = y[train_mask]
    X_test  = _to_float(df.loc[test_mask, features])
    y_test  = y[test_mask]

    logger.info(
        f"Train: {len(X_train):,} rows | Test: {len(X_test):,} rows | "
        f"Features: {len(features)}"
    )
    return X_train, X_test, y_train, y_test, features


# ── METRICS ───────────────────────────────────────────────────────────────────

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray, name: str) -> dict:
    labels = list(range(len(DIRECTION_CLASSES)))
    acc = accuracy_score(y_true, y_pred)
    macro_f1 = f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
    macro_precision = precision_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
    macro_recall = recall_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)

    try:
        macro_auc = roc_auc_score(y_true, y_proba, multi_class="ovr", average="macro", labels=labels)
    except ValueError:
        macro_auc = float("nan")

    logger.info(
        f"[{name}] accuracy={acc:.3f}  macro_F1={macro_f1:.3f}  "
        f"macro_precision={macro_precision:.3f}  macro_recall={macro_recall:.3f}  macro_AUC={macro_auc:.3f}"
    )
    return {
        "model": name,
        "accuracy": round(acc, 4),
        "macro_f1": round(macro_f1, 4),
        "macro_precision": round(macro_precision, 4),
        "macro_recall": round(macro_recall, 4),
        "macro_auc": round(macro_auc, 4) if pd.notna(macro_auc) else None,
    }


# ── MODELS ────────────────────────────────────────────────────────────────────

def _make_xgboost(n_classes: int):
    import xgboost as xgb  # lazy — kept consistent with the other two, see comment above
    return xgb.XGBClassifier(
        n_estimators=600, learning_rate=0.05, max_depth=5,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
        objective="multi:softprob", num_class=n_classes,
        random_state=42, verbosity=0,
    )


def _make_lightgbm(n_classes: int):
    import lightgbm as lgb  # lazy — see module-level comment on _LGB_AVAILABLE
    # n_jobs=1: LightGBM's default multi-threaded histogram building crashes
    # with a raw access violation on this Windows install/environment
    # (confirmed 2026-08-16 via isolated repro — not a cross-library conflict,
    # single-threaded LightGBM alone with the same data/params is stable).
    # Slower per model but reliable; fine at this dataset size (~13k rows).
    return lgb.LGBMClassifier(
        n_estimators=600, learning_rate=0.05, max_depth=5, num_leaves=31,
        subsample=0.8, colsample_bytree=0.8, class_weight="balanced",
        objective="multiclass", num_class=n_classes,
        random_state=42, verbose=-1, n_jobs=1,
    )


def _make_catboost():
    from catboost import CatBoostClassifier  # lazy — see _CATBOOST_AVAILABLE comment
    return CatBoostClassifier(
        iterations=600, learning_rate=0.05, depth=6,
        loss_function="MultiClass", auto_class_weights="Balanced",
        random_state=42, verbose=False,
    )


def build_models(n_classes: int) -> dict:
    # Returns (name, constructor) pairs, NOT instantiated model objects, and
    # LightGBM/CatBoost aren't even imported until their constructor actually
    # runs — see module-level comment on _LGB_AVAILABLE/_CATBOOST_AVAILABLE
    # for why (a Windows-only native crash when both libraries are loaded in
    # the same process).
    specs = [
        ("Random Forest", lambda: RandomForestClassifier(
            n_estimators=300, max_depth=10, min_samples_leaf=5,
            class_weight="balanced", n_jobs=-1, random_state=42,
        )),
        ("XGBoost", lambda: _make_xgboost(n_classes)),
    ]
    if _LGB_AVAILABLE:
        specs.append(("LightGBM", lambda: _make_lightgbm(n_classes)))
    if _CATBOOST_AVAILABLE:
        specs.append(("CatBoost", _make_catboost))
    return specs


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(only: str = None):
    """only=None trains every available model in one process. Pass a single
    model name (e.g. "LightGBM") to train just that one — see module
    docstring's note on the CatBoost/LightGBM native-library conflict: this
    lets each gradient-boosting library run in its own isolated process
    (invoked separately per model), which sidesteps the crash entirely
    regardless of exactly which library state it stems from. Results merge
    into the same CSV across runs, keyed on model name."""
    df = load_data()
    X_train, X_test, y_train, y_test, features = prepare(df)
    n_classes = len(DIRECTION_CLASSES)
    specs = build_models(n_classes)
    if only:
        specs = [(n, m) for n, m in specs if n == only]
        if not specs:
            raise ValueError(f"Unknown model name: {only!r}")

    results = []
    for name, make_model in specs:
        logger.info(f"Training {name}...")
        model = make_model()
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        preds = np.asarray(preds).reshape(-1)  # CatBoost returns a (n,1) array
        proba = model.predict_proba(X_test)
        results.append(compute_metrics(y_test, preds, proba, name))

    os.makedirs("models", exist_ok=True)
    new_df = pd.DataFrame(results)
    if os.path.exists(RESULTS_PATH):
        existing = pd.read_csv(RESULTS_PATH)
        existing = existing[~existing["model"].isin(new_df["model"])]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined = combined.sort_values("macro_f1", ascending=False)
    combined.to_csv(RESULTS_PATH, index=False)

    print("\n" + "=" * 80)
    print("SMF DIRECTION CLASSIFIER COMPARISON (Test: last 30 days)")
    print("=" * 80)
    print(combined.to_string(index=False))
    print(f"\nResults saved to {RESULTS_PATH}")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
