# ADR-09: SMF Forecaster Accuracy — Validate Before You Iterate

**Status:** Proposed
**Date:** 2026-08-25
**Deciders:** Project owner

## Context

The SMF 2-stage forecaster (CatBoost direction classifier → XGBoost price regressor) currently
scores **dir_acc=0.707** (naive T-24h baseline: 0.570) and **price MASE=0.766** (beats naive) on
its standard held-out window (last 30 days, ~707-807 hours). Today, following a real production
incident — a 16+ hour sustained deficit streak under-predicted by 2,000-3,550 TL/MWh, with the
direction classifier eventually flipping to `ENERGY_SURPLUS` mid-streak — three separate,
independently-motivated improvement attempts were designed, implemented, and rigorously validated
(after first fixing an unseeded-Optuna bug that had earlier produced a false-alarm ±170 TL/MWh
bias swing between identical-config runs — see [[smf_model_quality]]):

1. **`gip_gop_spread_lag24`**: a literature-backed cross-market spread feature already computed in
   the Gold layer, previously just never wired into this model. Result across 2 seeds: direction
   down ~0.3-0.4pt, price very slightly better. Small and mixed — not shipped.
2. **`direction_persistence_lag5h`**: a regime run-length feature, purpose-built to address the
   diagnosed incident. Correctly identified the failure mechanism (`smf_try_lag_24h` anchoring to
   the previous day's opposite regime) — but made direction classification WORSE specifically on
   the 17h+ sustained-streak bucket it targeted (80.5%→73.2%). A follow-up "price-only" attempt
   also failed, and that failure traced to a bug in the validation script itself, not a real
   effect — a second-order lesson about ablation methodology risk, not just the feature itself.
3. **CatBoost+Random Forest blend for Stage 1**: grounded in a real complementary-strength finding
   from an earlier model comparison (CatBoost wins accuracy/F1, RF wins macro AUC by ~4pt).
   Technically sound end-to-end (OOF-log-loss-chosen blend weight, pickle round-trip validated) —
   but net negative: direction improved only 0.3pt (within noise) while price degraded
   meaningfully (MAE +26 TL/MWh, bias nearly doubled worse).

All three were reverted; production remains on the plain CatBoost/XGBoost baseline. All three
implementations stay in the repo, gated off (excluded from active feature lists;
`USE_DIRECTION_BLEND = False`) — nothing is lost, but nothing is live either.

**The pattern worth naming:** three architecturally-reasonable, properly-seeded, honestly-measured
attempts in a row all landed the same way — a "wash" where one stage or metric improves while
another degrades, never a clean net win. That's either (a) genuine evidence the current
architecture is close to a real ceiling on this feature set, or (b) evidence that a single,
~750-hour held-out window — which happens to include the sustained-regime incident itself, an
atypical period — isn't a trustworthy enough instrument to detect real effects at the ~0.5-2pt
scale these attempts move things. `ptf_trainer.py` already solved this exact problem: its own
2026-08-03 leakage-audit work adopted a **5-window walk-forward** validation (train strictly
before each of 5 sequential 30-day windows, evaluate held-out on each) specifically because a
single most-recent-window comparison wasn't trustworthy enough — confirmed genuine skill survived
across a full crash/recovery price cycle (5/5 windows beat naive, mean MASE 0.623). `smf_trainer.py`
has never adopted the equivalent practice.

## Decision

Before trying another feature or architecture change on the SMF forecaster, port `ptf_trainer.py`'s
walk-forward validation methodology to `smf_trainer.py`, and re-run today's three "washed" attempts
through it before deciding whether any deserve a second look. Only after that instrument is
trustworthy does it make sense to invest in the next real architecture change — most likely an
ordinal-aware direction loss (see Consequences), since it's the one untried path from the existing
["SMF Accuracy Roadmap"](#) artifact whose mechanism (changing what a wrong prediction *costs*,
not adding another lagged/ensemble signal) is genuinely different from the three approaches that
already washed out.

## Options Considered

### Option A: Walk-forward validation infrastructure first (recommended)
Port the 5-window walk-forward harness from `ptf_trainer.py` to `smf_trainer.py` — same technique,
applied to both stages. Re-test today's three reverted attempts through it before trying anything
new.

| Dimension | Assessment |
|---|---|
| Complexity | Low-Medium — a validation harness, no model architecture change |
| Cost | ~5x compute per thing evaluated (5 retrains instead of 1, ~75min vs ~15min per candidate) |
| Team familiarity | High — directly precedented, PTF already proved this works |
| Correctness risk | Low |

**Pros:** directly answers the "is my single-window comparison trustworthy" doubt raised by three
wash-pattern results in a row; reuses a pattern already proven in this exact codebase; makes
*every* future SMF attempt (ordinal loss, live GİP ingestion, anything else) more trustworthy to
evaluate, not just one candidate — a force-multiplier, not a one-off fix.
**Cons:** doesn't itself improve the model; 5x compute cost per candidate tested; delays trying the
next real idea by however long re-validating today's three attempts takes.

### Option B: Try ordinal-aware direction loss next, on the current single-window evaluation
Skip straight to the most-differentiated untried roadmap path — restructure Stage 1 from a single
3-way softmax to two cumulative binary classifiers (`P(≥ balance)`, `P(≥ surplus)`) or a custom
ordinal loss, so a deficit-predicted-as-surplus miss costs more than a deficit-predicted-as-balance
miss — directly targeting the DUY Madde 28 asymmetric-cost concern already on record in ADR-08.

| Dimension | Assessment |
|---|---|
| Complexity | Medium-High — genuine Stage-1 restructure, not a feature-list swap |
| Cost | ~1-2 days build + validation time |
| Team familiarity | Low — new pattern for this codebase |
| Correctness risk | Medium — needs careful threshold calibration for the cumulative-binary variant |

**Pros:** mechanism is fundamentally different from what's already failed three times (loss
function, not input signal) — plausibly a genuinely different outcome rather than another wash;
directly serves the trading-signal work's own stated concern.
**Cons:** without fixing the evaluation instrument first, a positive-looking single-window result
here carries the exact same trust problem the three failed attempts already exposed — real risk of
chasing a fourth false signal, just a more expensive one to build.

### Option C: Redirect effort to ADR-08's trading-signal layer instead
Stop optimizing the forecaster directly — dir_acc=0.707 already clearly beats naive (0.570), and
ADR-08's trading-signal layer (`mart_smf_trading_signal.sql`, strategy backtest, dashboard page) is
fully designed, un-started, and arguably where forecast quality actually converts into
decision-support value.

| Dimension | Assessment |
|---|---|
| Complexity | Medium — ADR-08's own scope, not part of this decision |
| Cost | ~1-2 weeks per ADR-08's own estimate |
| Team familiarity | Medium |
| Correctness risk | Medium — bounded by current model quality, per ADR-08 itself |

**Pros:** ready to start today, doesn't require resolving the "is 0.707 near the ceiling" question
at all, likely higher near-term thesis/portfolio value than another 0.5-2pt accuracy attempt.
**Cons:** doesn't answer whether more accuracy is achievable; ADR-08's own Action Item 2 (a price
uncertainty band) is itself model-adjacent work, so this isn't a full stop on model work either —
more a change of which model question gets prioritized.

## Trade-off Analysis

Option A costs the least and de-risks everything downstream of it, including Option B if pursued
later — a walk-forward harness doesn't compete with Option C's trading-signal work for the same
"is this accuracy gain real" evaluation bandwidth, since ADR-08 doesn't require a more-accurate
model, just the current one plus an uncertainty band. That makes A and C genuinely compatible to
pursue in parallel rather than a strict either/or. B is the higher-risk, higher-effort option of
the three specifically *because* it's evaluated with the same untrustworthy instrument the other
three attempts exposed — building a Stage-1 restructure on top of an unproven evaluation method
risks repeating today's exact pattern at higher cost.

## Consequences

- **Easier:** every future SMF feature/architecture attempt gets a real trustworthiness check
  before anyone decides "helps" or "doesn't" — the same question asked three times today with
  progressively more care (fix the seed, seed multiple times, break down by bucket, catch a
  validation-script bug) gets answered by the harness itself instead of by hand each time.
- **Harder:** every candidate now costs ~5x the compute/wall-clock to evaluate properly — today's
  ~15-30min validate-then-decide loop becomes ~75-150min per candidate.
- **Must revisit:** whether today's three reverted attempts (`gip_gop_spread_lag24`,
  `direction_persistence_lag5h`, CatBoost+RF blend) actually wash out under walk-forward too, or
  whether one of them was a real, small win that a single noisy window couldn't detect — that's the
  first thing to check once the harness exists, before building anything new.
- **Explicit non-goal:** this ADR doesn't decide whether Option B (ordinal loss) or Option C
  (trading signal) happens next — only that Option A happens before Option B specifically, and that
  Option C can proceed on its own timeline regardless.

## Action Items

1. [ ] Port `ptf_trainer.py`'s 5-window walk-forward validation (train strictly before each of 5
       sequential 30-day windows, evaluate held-out per window) into a reusable harness for
       `smf_trainer.py`'s direction+price stages.
2. [ ] Re-run today's three reverted attempts (`gip_gop_spread_lag24`,
       `direction_persistence_lag5h` full-add, CatBoost+RF blend) through the new harness — confirm
       they still wash out, or discover one was a real small win the single-window test missed.
3. [ ] If the walk-forward result confirms the current architecture's ceiling, proceed to Option B
       (ordinal-aware direction loss) evaluated through the same harness — not the old single-window
       method.
4. [ ] Apply the same `optuna.samplers.TPESampler(seed=OPTUNA_SEED)` fix to `ptf_trainer.py` (same
       unseeded-Optuna pattern confirmed present there too, not yet checked or fixed).
5. [ ] Independently of 1-4: ADR-08's trading-signal layer can proceed in parallel — it doesn't
       block on or get blocked by this decision.
