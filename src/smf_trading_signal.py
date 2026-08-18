"""
smf_trading_signal.py — Option B: probabilistic expected-value trading signal
(ADR-08, plans/08-smf-trading-signal-architecture.md)

Computes, for each hour, the expected cost/value of "let an imbalance settle
at (predicted) SMF" vs "true up now in GİP" under DUY Madde 28's asymmetric
settlement formula:
    deficit: pay  GREATEST(PTF, SMF) x 1.03
    surplus: get  LEAST(PTF, SMF)    x 0.97

Properly accounts for the price regressor's forecast uncertainty, not just
its point estimate — E[max(PTF,SMF)] >= max(PTF, E[SMF]) whenever SMF is
uncertain (Jensen's inequality on the MAX/MIN payoff), so a naive
point-estimate signal systematically understates deficit cost / overstates
surplus value right at the PTF/SMF crossover — exactly the region where the
decision matters most. Uncertainty is estimated cheaply from backtest
residuals bucketed by predicted_direction (no new model training needed).

This module's run_backtest() is the gate ADR-08 requires before any of this
becomes a live dashboard signal: it simulates "follow the signal" against
realized settlements vs. two baselines (always true-up, always let-settle)
and a perfect-foresight upper bound. Do not build the dashboard signal page
until this shows genuine, non-fluke edge over a meaningful sample.
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import norm
from google.cloud import bigquery

from config import GCP_PROJECT_ID as PROJECT_ID, BQ_GOLD_DATASET as DATASET_ID, get_bq_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SMFTradingSignal")

DEFICIT_MULTIPLIER = 1.03
SURPLUS_MULTIPLIER = 0.97

BACKTEST_TABLE = f"{PROJECT_ID}.{DATASET_ID}.gold_smf_trading_backtest"


# ── EXPECTED VALUE UNDER UNCERTAINTY ────────────────────────────────────────

def expected_max(mu: np.ndarray, sigma: np.ndarray, a: np.ndarray) -> np.ndarray:
    """E[max(X, a)] for X ~ N(mu, sigma^2), elementwise — closed-form (same
    structure as a call option's expected payoff). sigma -> 0 collapses to
    the plain max(mu, a) a naive point-estimate signal would use."""
    sigma = np.maximum(sigma, 1e-6)
    d = (mu - a) / sigma
    return a + sigma * norm.pdf(d) + (mu - a) * norm.cdf(d)


def expected_min(mu: np.ndarray, sigma: np.ndarray, a: np.ndarray) -> np.ndarray:
    """E[min(X, a)] = mu + a - E[max(X, a)]."""
    return mu + a - expected_max(mu, sigma, a)


# ── DATA LOADING ─────────────────────────────────────────────────────────────

def load_residual_sigma() -> pd.Series:
    """Backtest residual std per predicted_direction class — the cheap,
    no-new-infra uncertainty estimate ADR-08 flags as a prerequisite. Uses
    stg_smf (not gold_smf_predictions' own join) so it reflects the
    currently-corrected, complete settlement history (see the SMF Silver
    gap fixes earlier this session)."""
    client = get_bq_client()
    q = f"""
        SELECT p.predicted_direction, p.predicted_smf, s.smf_try
        FROM `{PROJECT_ID}.{DATASET_ID}.gold_smf_predictions` p
        JOIN `{PROJECT_ID}.{DATASET_ID}.stg_smf` s
          ON s.date = p.predicted_date AND s.hour = p.hour
    """
    df = client.query(q).to_dataframe()
    df["residual"] = df["smf_try"] - df["predicted_smf"]

    sigma = df.groupby("predicted_direction")["residual"].std()
    n = df.groupby("predicted_direction")["residual"].count()
    overall_sigma = df["residual"].std()

    # Fall back to the overall residual std for any class with too few
    # samples for a stable per-class estimate (n<20 is a rough floor).
    sigma = sigma.where(n >= 20, overall_sigma)

    logger.info("Residual sigma by direction (TL/MWh):")
    for direction in sigma.index:
        logger.info(f"  {direction}: sigma={sigma[direction]:.1f}  n={n.get(direction, 0)}")
    logger.info(f"  overall: sigma={overall_sigma:.1f}  n={len(df)}")
    return sigma


def load_backtest_frame() -> pd.DataFrame:
    """Every backtested hour with a known real settlement, PTF, and GİP VWAP
    — everything needed to both compute the signal (from predicted_smf) and
    score it against what actually happened (from actual_smf)."""
    client = get_bq_client()
    q = f"""
        SELECT
            p.predicted_date AS date, p.hour,
            p.predicted_smf, p.predicted_direction,
            s.smf_try AS actual_smf,
            pr.ptf_try,
            g.gip_vwap_try
        FROM `{PROJECT_ID}.{DATASET_ID}.gold_smf_predictions` p
        JOIN `{PROJECT_ID}.{DATASET_ID}.stg_smf` s
          ON s.date = p.predicted_date AND s.hour = p.hour
        JOIN `{PROJECT_ID}.{DATASET_ID}.stg_pricing` pr
          ON pr.date = p.predicted_date AND pr.hour = p.hour
        JOIN `{PROJECT_ID}.{DATASET_ID}.mart_gip_hourly_reference` g
          ON g.date = p.predicted_date AND g.hour = p.hour
        ORDER BY p.predicted_date, p.hour
    """
    df = client.query(q).to_dataframe()
    logger.info(f"Loaded {len(df)} backtest hours with real SMF + PTF + GİP VWAP.")
    return df


# ── SIGNAL ────────────────────────────────────────────────────────────────────

def build_signal(df: pd.DataFrame, sigma_by_direction: pd.Series) -> pd.DataFrame:
    """Adds the EV-based signal columns. Computed from PREDICTED info only
    (predicted_smf, ptf_try, gip_vwap_try as of decision time) — actual_smf
    is carried through untouched for later scoring, never used here."""
    df = df.copy()
    overall_sigma = sigma_by_direction.mean()
    df["sigma"] = df["predicted_direction"].map(sigma_by_direction).fillna(overall_sigma)

    mu = df["predicted_smf"].to_numpy()
    sigma = df["sigma"].to_numpy()
    ptf = df["ptf_try"].to_numpy()

    e_max = expected_max(mu, sigma, ptf)
    e_min = expected_min(mu, sigma, ptf)

    df["ev_deficit_settle"] = e_max * DEFICIT_MULTIPLIER
    df["ev_deficit_true_up"] = df["gip_vwap_try"]
    df["signal_deficit"] = np.where(
        df["ev_deficit_true_up"] < df["ev_deficit_settle"], "TRUE_UP_GIP", "LET_SETTLE")

    df["ev_surplus_settle"] = e_min * SURPLUS_MULTIPLIER
    df["ev_surplus_true_up"] = df["gip_vwap_try"]
    df["signal_surplus"] = np.where(
        df["ev_surplus_true_up"] > df["ev_surplus_settle"], "TRUE_UP_GIP", "LET_SETTLE")

    return df


# ── STRATEGY BACKTEST (the gate) ────────────────────────────────────────────

def run_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """Scores the signal against REALIZED settlements — never uses
    predicted_smf here, only actual_smf. Deficit is a cost (lower=better),
    surplus is a value (higher=better); reported separately since a
    per-hour position direction isn't known without a live position feed
    (see ADR-08)."""
    df = df.copy()
    realized_max = np.maximum(df["ptf_try"], df["actual_smf"])
    realized_min = np.minimum(df["ptf_try"], df["actual_smf"])

    df["realized_deficit_settle"] = realized_max * DEFICIT_MULTIPLIER
    df["realized_deficit_true_up"] = df["gip_vwap_try"]
    df["cost_always_settle"] = df["realized_deficit_settle"]
    df["cost_always_true_up"] = df["realized_deficit_true_up"]
    df["cost_follow_signal"] = np.where(
        df["signal_deficit"] == "TRUE_UP_GIP",
        df["realized_deficit_true_up"], df["realized_deficit_settle"])
    df["cost_perfect_foresight"] = np.minimum(
        df["realized_deficit_settle"], df["realized_deficit_true_up"])

    df["realized_surplus_settle"] = realized_min * SURPLUS_MULTIPLIER
    df["realized_surplus_true_up"] = df["gip_vwap_try"]
    df["value_always_settle"] = df["realized_surplus_settle"]
    df["value_always_true_up"] = df["realized_surplus_true_up"]
    df["value_follow_signal"] = np.where(
        df["signal_surplus"] == "TRUE_UP_GIP",
        df["realized_surplus_true_up"], df["realized_surplus_settle"])
    df["value_perfect_foresight"] = np.maximum(
        df["realized_surplus_settle"], df["realized_surplus_true_up"])

    return df


def summarize_backtest(df: pd.DataFrame) -> None:
    n = len(df)
    logger.info(f"=== Strategy backtest — {n} hours ===")

    logger.info("--- Deficit side: avg cost TL/MWh (lower is better) ---")
    for col, label in [
        ("cost_always_settle", "Always let settle"),
        ("cost_always_true_up", "Always true-up GİP"),
        ("cost_follow_signal", "Follow EV signal"),
        ("cost_perfect_foresight", "Perfect foresight (upper bound)"),
    ]:
        logger.info(f"  {label:32s}: {df[col].mean():8.2f}")

    edge_vs_settle = df["cost_always_settle"].mean() - df["cost_follow_signal"].mean()
    edge_vs_true_up = df["cost_always_true_up"].mean() - df["cost_follow_signal"].mean()
    logger.info(f"  Signal edge vs always-settle : {edge_vs_settle:+8.2f} TL/MWh")
    logger.info(f"  Signal edge vs always-true-up: {edge_vs_true_up:+8.2f} TL/MWh")

    logger.info("--- Surplus side: avg value TL/MWh (higher is better) ---")
    for col, label in [
        ("value_always_settle", "Always let settle"),
        ("value_always_true_up", "Always true-up GİP"),
        ("value_follow_signal", "Follow EV signal"),
        ("value_perfect_foresight", "Perfect foresight (upper bound)"),
    ]:
        logger.info(f"  {label:32s}: {df[col].mean():8.2f}")

    edge_vs_settle_s = df["value_follow_signal"].mean() - df["value_always_settle"].mean()
    edge_vs_true_up_s = df["value_follow_signal"].mean() - df["value_always_true_up"].mean()
    logger.info(f"  Signal edge vs always-settle : {edge_vs_settle_s:+8.2f} TL/MWh")
    logger.info(f"  Signal edge vs always-true-up: {edge_vs_true_up_s:+8.2f} TL/MWh")

    n_true_up_deficit = (df["signal_deficit"] == "TRUE_UP_GIP").sum()
    n_true_up_surplus = (df["signal_surplus"] == "TRUE_UP_GIP").sum()
    logger.info(f"--- Signal mix: TRUE_UP_GIP called {n_true_up_deficit}/{n} hours (deficit side), "
                f"{n_true_up_surplus}/{n} hours (surplus side) ---")


# ── WRITER ────────────────────────────────────────────────────────────────────

_BACKTEST_SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("hour", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("predicted_smf", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("predicted_direction", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("actual_smf", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ptf_try", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("gip_vwap_try", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("sigma", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("signal_deficit", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("signal_surplus", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cost_always_settle", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("cost_always_true_up", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("cost_follow_signal", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("cost_perfect_foresight", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("value_always_settle", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("value_always_true_up", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("value_follow_signal", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("value_perfect_foresight", "FLOAT64", mode="REQUIRED"),
]

_WRITE_COLS = [f.name for f in _BACKTEST_SCHEMA]


def write_backtest(df: pd.DataFrame) -> None:
    client = get_bq_client()
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table = bigquery.Table(dataset_ref.table("gold_smf_trading_backtest"), schema=_BACKTEST_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="date")
    client.create_table(table, exists_ok=True)

    job = client.load_table_from_dataframe(
        df[_WRITE_COLS], BACKTEST_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    logger.info(f"Wrote {len(df)} rows to {BACKTEST_TABLE}")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    sigma_by_direction = load_residual_sigma()
    df = load_backtest_frame()
    df = build_signal(df, sigma_by_direction)
    df = run_backtest(df)
    summarize_backtest(df)
    write_backtest(df)
    return df


if __name__ == "__main__":
    run()
