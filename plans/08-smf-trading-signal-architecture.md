# ADR-08: SMF Trading Signal & Opportunity Layer

**Status:** Proposed
**Date:** 2026-08-18
**Deciders:** Project owner

## Context

The project already has a working 2-stage SMF forecaster (CatBoost direction classifier → XGBoost price
regressor) producing `predicted_smf` / `predicted_direction` / class probabilities, backtested
(`gold_smf_predictions`) and live-forward (`gold_smf_forward_predictions`, plus a fixed-5h-lead snapshot
in `gold_smf_forward_snapshot_5h`). Current backtest quality: MAE ≈715 TL/MWh, MASE ≈0.656 (beats T-24h
naive), direction accuracy ≈64.8%, macro F1 ≈0.647, macro AUC ≈0.739 — a genuine but noisy edge, not a
precise instrument. None of this is a trading opportunity yet — it's a forecast sitting in a database.

The question is what sits *between* "the model has an opinion about SMF" and "a trader does something
profitable with it." In this market, that gap is governed by DUY Madde 28's settlement formula, already
implemented in `mart_imbalance_cost.sql`:

- **Deficit** (produced/procured less than day-ahead commitment): pay `GREATEST(PTF, SMF) × 1.03`
- **Surplus** (produced/procured more than day-ahead commitment): receive `LEAST(PTF, SMF) × 0.97`

This formula is *structurally* punitive in both directions — it's designed to discourage deviating from
your day-ahead position, not to reward it. So the real trading question a BSP (Dengeden Sorumlu Grup)
asks every hour isn't "will I profit from imbalance" — it's:

> **"Given where I currently sit relative to my day-ahead commitment, is it cheaper to true up in GİP
> (intraday) right now, or to accept the imbalance and let it settle at SMF?"**

That's a genuine, recurring, quantifiable decision — and it's the one this architecture should be built
to support. Two real gaps stand in the way today: the platform has no live GİP quote feed, and it has no
notion of a trader's current position. Both are called out explicitly below rather than assumed away.

**Constraint, stated upfront:** this is a decision-support system, not an execution system. Nothing here
places orders or moves money. A human (or an existing execution system this platform doesn't own) makes
the final call — the same posture as every other production safeguard already in this codebase
(`email_on_failure`, `--fail-fast` dbt gates, manual confirmation before force-pushes, etc.).

## Decision

Add a **Signal Layer** on top of the existing forecast tables, following the same medallion pattern
already used everywhere else in this project (dbt mart → Python job → Airflow DAG → Streamlit page),
rather than building a parallel system. Concretely:

1. A new Gold mart, `mart_smf_trading_signal.sql`, that turns forecasts + PTF into an **expected-value
   comparison** between "true up now" and "let it settle" — not a naive threshold on the price point
   estimate.
2. A **strategy backtest** that replays this signal against `gold_smf_predictions`' real historical
   settlements, to prove (or disprove) that it has positive edge net of realistic GİP transaction costs
   *before* anyone treats it as real.
3. A dashboard signal page + optional email alert (reusing the DAG's existing SMTP config) for
   high-confidence opportunities — informational only.

## Options Considered

### Option A: Point-estimate threshold signal
Flag an "opportunity" whenever `|predicted_smf − ptf_try|` exceeds some threshold and direction-class
probability exceeds some confidence cutoff.

| Dimension | Assessment |
|---|---|
| Complexity | Low — a few lines of SQL on existing tables |
| Cost | Near zero — no new data sources |
| Build time | ~1 day |
| Correctness risk | **High** |

**Pros:** trivial to build, easy to explain, ships immediately.
**Cons:** ignores the asymmetric `MAX`/`MIN` structure of the settlement formula entirely, and — more
importantly — is *systematically biased*. Because deficit cost is `MAX(PTF, SMF)` and SMF is a noisy
point estimate (±715 TL/MWh MAE against SMF levels typically in the 2,000–4,500 TL/MWh range), Jensen's
inequality means `E[MAX(PTF, SMF)] ≥ MAX(PTF, E[SMF])` whenever there's real uncertainty near the PTF/SMF
crossover. A signal built on the point estimate alone will *understate* true expected imbalance cost
right around the threshold that matters most — exactly the hours where the decision is close enough to
be worth making carefully.

### Option B: Probabilistic expected-value engine (recommended)
Combine the direction classifier's 3-class probabilities with the price regressor's point estimate (plus
a residual-based uncertainty band, see Action Items) to compute the **expected cost of each available
action** directly from the DUY Madde 28 formula, then recommend whichever minimizes expected cost:

```
EV(let_settle)  = P(deficit) · E[MAX(PTF, SMF) | deficit] · 1.03
                 − P(surplus) · E[MIN(PTF, SMF) | surplus] · 0.97
EV(true_up_GIP) = GIP_quote − PTF   (known/quoted, not forecast)

recommended_action = argmin( EV(let_settle), EV(true_up_GIP) )
```

| Dimension | Assessment |
|---|---|
| Complexity | Medium — one new mart, one new ingestion source (live GİP quotes) |
| Cost | Low — GİP is already a source category in `epias_sources.py`'s market family; no new infra class |
| Build time | ~1–2 weeks including backtest validation |
| Correctness risk | Medium — bounded by current model quality, not by the signal logic itself |

**Pros:** correctly prices the asymmetric payoff, degrades gracefully (falls back to "no clear edge, do
nothing" when confidence is low — a valid and common output, not a bug), reuses the model's existing
outputs almost entirely as-is.
**Cons:** needs a live/near-live GİP quote feed the platform doesn't currently ingest, and needs *some*
uncertainty measure on the price regression beyond the current point estimate (see Action Items — this is
the one real prerequisite gap).

### Option C: Full systematic strategy with position sizing (Kelly-style)
Treat this as a proper quant strategy — bankroll management, confidence-scaled position sizing, drawdown
limits, the works.

| Dimension | Assessment |
|---|---|
| Complexity | High |
| Cost | Medium-high — needs a real backtesting/risk framework, not just a mart |
| Build time | Weeks-to-months |
| Correctness risk | Premature at current model quality |

**Pros:** the "correct" end state if the signal proves out.
**Cons:** sizing a position confidently on a 64.8%-accuracy, 3-class model with no proven backtested edge
yet is how real money gets lost. This is where Option B graduates *to*, not where you start.

## Trade-off Analysis

The deciding factor isn't build cost — Option A and B are both cheap relative to the model work already
done this session. It's that **Option A's simplicity is illusory**: a threshold signal that ignores
forecast uncertainty will look fine in a demo and lose money in the asymmetric payoff it's actually
exposed to, precisely because the formula it's approximating is convex in the forecast error. Option B
costs maybe a week more and produces a signal whose sign you can actually trust. Option C is the right
target eventually, but sizing capital against an unvalidated signal is the wrong order of operations —
prove edge with B's backtest first.

## Consequences

- **Easier:** every future refinement to the SMF forecaster (better MAE, calibrated probabilities, the
  planned fixed-5h snapshot already shipped this session) flows straight through to signal quality — no
  separate model needed for "trading" vs. "forecasting."
- **Harder:** the platform now needs a live GİP quote feed it doesn't have today — this is new ingestion
  surface, new `DATA_DELAYS`/schedule considerations, and a new Silver source, not a trivial add.
- **Must revisit:** the price regressor is currently point-estimate only. Cheapest path to an uncertainty
  band without retraining: bucket backtest residuals by hour-of-day and by predicted_direction class, use
  the empirical residual distribution per bucket. A real quantile regressor is the better long-term
  answer but isn't a blocker to shipping Option B.
- **Explicit non-goal:** no automated order placement. The dashboard/alert surfaces `recommended_action`
  + `expected_value_try_per_mwh` + `confidence` for a human to act on — the same posture as every
  execution-adjacent boundary elsewhere in this session's work.

## Action Items

1. [ ] Ingest live/near-live GİP quotes as a new Bronze/Silver source (mirrors the existing
       `EPIAS_SOURCES` pattern — GİP company-activity ingestion already exists as a precedent to extend).
2. [ ] Add a per-bucket residual uncertainty estimate to the price regressor's output (hour-of-day ×
       predicted_direction bucketing of backtest residuals is the cheap first pass).
3. [ ] Build `mart_smf_trading_signal.sql`: joins `gold_smf_forward_snapshot_5h` (or live forward,
       depending on desired horizon) against `mart_ptf_realized` and the new GİP source, computes the EV
       formula above.
4. [ ] Build a strategy backtest against `gold_smf_predictions`' real settlements: cumulative P&L of
       "follow the signal" vs. "always true up" vs. "perfect foresight" upper bound, net of a realistic
       GİP bid-ask assumption. **This gates everything else** — don't ship the dashboard signal until this
       shows positive, non-fluke edge over a meaningful sample.
5. [ ] Dashboard: new "💰 Trading Sinyalleri" page — current signal, EV, confidence, and the backtest's
       historical performance chart, mirroring the existing SMF/PTF page structure.
6. [ ] Optional: email alert via the DAG's existing SMTP config for high-EV, high-confidence hours only —
       informational, not an order.
