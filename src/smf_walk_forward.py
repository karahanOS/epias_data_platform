"""
smf_walk_forward.py — 5-window walk-forward validation for the SMF 2-stage
forecaster (ADR-09, 2026-08-25 — see plans/09-smf-forecaster-validation-methodology.md).

Three feature/architecture attempts on 2026-08-25 (gip_gop_spread_lag24,
direction_persistence_lag5h, CatBoost+RF blend) were all evaluated on a single
~30-day held-out window and all landed the same "wash" way — one metric
improves while another degrades. ptf_trainer.py solved the same "is a
single-window comparison trustworthy" problem back on 2026-08-03 with a
5-window walk-forward backtest (train strictly before each of 5 sequential
30-day windows, evaluate held-out per window) — but that validation was a
one-off script, never committed as reusable code (confirmed via git log/grep:
no walk-forward function exists anywhere in history). This ports the same
METHODOLOGY as permanent, reusable infrastructure instead.

Design: reuses smf_trainer.py's real production functions
(extract_training_data, engineer_features, train_direction_classifier,
train_price_regressor) completely unmodified. Each window is produced by
truncating the already-engineered dataframe to end at that window's boundary
and letting those functions' existing "last 30 days is test" convention do
the rest — recency weighting and the naive-T-24h baseline both naturally
re-anchor to each window's own truncated "now" this way. No GCS upload; this
is a validation tool, not the trainer. To re-test a feature/architecture
change through this harness (ADR-09 Action Item 2), monkeypatch
smf_trainer.DIRECTION_FEATURE_COLS / PRICE_FEATURE_COLS (or USE_DIRECTION_BLEND)
before calling run(), same pattern the 2026-08-25 ablation scripts used for
the single-window comparisons.

Cost: ~5x a single training run per stage (~75-150 min for 5 windows) — the
explicit trade-off ADR-09 accepts in exchange for a trustworthy result.
"""
import pandas as pd

import smf_trainer as T

N_WINDOWS_DEFAULT = 5
WINDOW_DAYS = 30


def run(n_windows: int = N_WINDOWS_DEFAULT, label: str = "") -> pd.DataFrame:
    tag = f" [{label}]" if label else ""
    print(f"Pulling + engineering full training data once{tag}...")
    df_raw = T.extract_training_data()
    df_full = T.engineer_features(df_raw)

    latest = df_full.index.max()
    rows = []

    for i in range(n_windows):
        window_end   = latest - pd.Timedelta(days=WINDOW_DAYS * i)
        window_start = window_end - pd.Timedelta(days=WINDOW_DAYS)
        df_window = df_full[df_full.index < window_end]

        if df_window.empty or df_window.index.min() > window_start:
            print(f"Window {i} ({window_start.date()} -> {window_end.date()}): "
                  f"insufficient history before this window, stopping.")
            break

        print(f"\n{'='*70}\nWindow {i}{tag}: test = {window_start.date()} -> "
              f"{window_end.date()} | rows available: {len(df_window):,}\n{'='*70}")

        direction_model, oof_proba, direction_features, _, direction_metrics = \
            T.train_direction_classifier(df_window)
        _, _, _, price_metrics = \
            T.train_price_regressor(df_window, oof_proba)

        rows.append({
            "window": i,
            "test_start": window_start.date(),
            "test_end": window_end.date(),
            "dir_accuracy": round(direction_metrics["accuracy"], 4),
            "dir_naive": round(direction_metrics["naive_accuracy"], 4),
            "price_mae": round(price_metrics["mae"], 2),
            "price_smape": round(price_metrics["smape"] * 100, 2),
            "price_bias": round(price_metrics["bias"], 2),
            "price_mase": round(price_metrics["mase"], 4),
        })

    result = pd.DataFrame(rows)
    print(f"\n\n{'='*70}\nWALK-FORWARD RESULTS{tag} ({len(result)} windows)\n{'='*70}")
    print(result.to_string(index=False))

    if not result.empty:
        dir_wins  = int((result["dir_accuracy"] > result["dir_naive"]).sum())
        mase_wins = int((result["price_mase"] < 1.0).sum())
        print(f"\nDirection beats naive T-24h in {dir_wins}/{len(result)} windows.")
        print(f"Price beats naive T-24h (MASE<1.0) in {mase_wins}/{len(result)} windows.")
        print(f"Mean dir_accuracy: {result['dir_accuracy'].mean():.4f} "
              f"(std: {result['dir_accuracy'].std():.4f})")
        print(f"Mean price_mase:   {result['price_mase'].mean():.4f} "
              f"(std: {result['price_mase'].std():.4f})")

    return result


if __name__ == "__main__":
    out = run()
    out.to_csv("models/smf_walk_forward_results.csv", index=False)
    print("\nSaved to models/smf_walk_forward_results.csv")
