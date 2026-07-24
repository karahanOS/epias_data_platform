# ADR-0003: Consolidate Per-Source Dataproc Batches to Fix Hourly-Schedule Latency

**Status:** Accepted
**Date:** 2026-07-24
**Deciders:** Mehmet Karahan Çetinkaya

## Context

ADR-0002 moved Silver-layer Spark execution from always-on docker-compose containers to Dataproc Serverless batches, one `DataprocCreateBatchOperator` task per source. A full manual run of `epias_medallion_pipeline_v3` (2026-07-23 data) was used to validate this end-to-end after fixing three unrelated IAM issues and a CPU-quota problem (see ADR-0002's Pilot Results).

That validation run surfaced a new problem, unrelated to correctness: **every one of the 24 Dataproc Silver batches took 3–4.5 minutes**, regardless of source. Actual per-source data volume is tiny (tens of rows/hour), so this is not compute time — it is Dataproc Serverless's fixed cold-start cost (provisioning compute, pulling the runtime container image, initializing a fresh Spark session) charged independently to every single batch.

Because the CPU quota (24 vCPU, see ADR-0002) only permits 2 concurrent 12-vCPU batches, the `dataproc_batches` Airflow pool caps concurrency at 2. With 24 batches at ~4 minutes each, that's 12 sequential waves — roughly **40 minutes** just for the Silver layer, which matched the observed run duration exactly.

This is disqualifying for the stated goal (hourly schedule, ADR-0002 action item 9): with `max_active_runs=1`, a run that takes 40–60+ minutes leaves no safety margin against the next hour's trigger, and any slippage compounds run over run.

## Decision

Stop submitting one Dataproc batch per source. Instead, group the ~24 daily-eligible sources into a small number of batches (sized to match the pool's concurrency, currently 2) and run each group's sources **sequentially within a single shared Spark session** per batch. Cold-start cost is paid once per group instead of once per source, and the two groups still run in parallel against the two pool slots.

Concretely:
- `spark_utils.BaseEpiasSparkJob` gains an optional `spark: SparkSession` constructor parameter. When provided, the job reuses that session instead of building its own, and does not stop it when done — lifecycle is owned by whoever passed the session in. Existing standalone usage (`python bronze_to_silver_x.py`) is unaffected: no `spark` argument means the job builds and stops its own session exactly as before.
- A new orchestrator, `spark_jobs/silver_batch_runner.py`, takes a run date and a comma-separated list of source keys, creates **one** SparkSession, discovers and runs each source's existing `*SilverJob` class (via inspection of `bronze_to_silver_<source>.py` — the individual job files themselves do not change their transform logic at all), and stops the session once at the end.
- `epias_sources.py` splits `ALL_SOURCES` (and `BACKFILL_SOURCES`) into N groups at DAG-build time (`N` = the pool's slot count, currently 2) and submits one `DataprocCreateBatchOperator` per group calling `silver_batch_runner.py`, instead of one per source. Grouping is computed from the live source list, not hardcoded, so adding/removing a source never requires updating a group manifest by hand.
- DAG dependency wiring changes from 1:1 (`bronze_save_tasks[key] >> silver_t`) to many:1 (every bronze save task for sources in a group must complete before that group's batch runs).

## Options Considered

### Option A: Increase pool/quota concurrency instead of consolidating
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — request more CPU quota, resize the `dataproc_batches` pool |
| Cost | Scales linearly — 8 concurrent batches needs ~96 vCPU quota, well beyond a personal-project's likely approval ceiling |
| Latency | Reduces wave count proportionally (24 batches / 8 concurrent = 3 waves ≈ 12–18 min), but never removes the fixed per-batch cold start |
| Risk | None to existing code |

**Pros:** No code changes, just infra config.
**Cons:** Doesn't address the root cause (cold start × 24), just buys it down with more parallelism and more quota spend. Diminishing returns once concurrency exceeds what's needed to hide cold start behind other work.

### Option B (chosen): Consolidate sources into fewer, larger batches
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — `BaseEpiasSparkJob` needs a session-injection option, plus a new orchestrator script and DAG wiring change; per-source transform logic is untouched |
| Cost | Lower than today and lower than Option A — cold start is paid ~2 times instead of ~24, and per-batch vCPU cost is unchanged |
| Latency | ~2 waves × (1 cold start + 24/2 sources × seconds each) ≈ 7–9 minutes for the whole Silver layer, versus ~40 minutes today |
| Risk | A single failing source inside a group could be mis-diagnosed as a group-wide failure if logs aren't inspected per-source; mitigated by keeping each source's own try/except and logging inside the shared run |

**Pros:** Directly attacks the actual cost driver (repeated cold starts). Cheaper AND faster at the same time — not a trade-off between them. Keeps the real Dataproc/Spark story (still genuinely distributed batch compute, just sensibly batched).
**Cons:** More moving parts than a pure config change; needs care so one source's exception doesn't silently swallow the rest of its group.

### Option C: Abandon Dataproc Serverless for this workload, move to pandas/Cloud Run (revisit ADR-0002's rejected Option D)
| Dimension | Assessment |
|-----------|------------|
| Complexity | High — full rewrite of ~24 job scripts |
| Cost | Lowest possible (near-zero cold start) |
| Latency | Likely under a minute total for all sources |
| Risk | Discards the portfolio/interview value of the Spark/Dataproc story, which was the explicit reason Option D was rejected in ADR-0002 |

**Pros:** Fastest and cheapest by far.
**Cons:** Same objection as ADR-0002 — rejected there for the same reason, and that reasoning hasn't changed. Kept only as a fallback if Option B still isn't fast enough after implementation.

## Trade-off Analysis

Option A treats the symptom (too few concurrent slots) rather than the cause (paying cold-start 24 times). Option C solves it completely but re-opens a decision already made deliberately in ADR-0002 for non-technical (portfolio) reasons that still hold. Option B is the one that actually targets the cost driver identified in this run's data, is cheaper AND faster simultaneously (not a trade-off), and requires no change to any individual source's transform logic — only how the 24 jobs are packaged into Dataproc submissions. It also degrades gracefully: if 2 groups still isn't fast enough, the group count can be tuned independently of anything else (e.g., 3–4 groups if quota is later increased), without revisiting this decision.

## Consequences

- **Easier:** Silver layer wall-clock time drops from ~40 minutes to an estimated ~7–9 minutes, making an hourly schedule actually viable. Total Dataproc billed compute time also drops (fewer cold starts), independent of the quota question.
- **Harder:** Debugging shifts slightly — a failure inside one source's transform, while running inside a shared session for its group, needs per-source log clarity (each source's run must log its own start/end and exception clearly) so a group-level failure can still be attributed to the one source that actually broke.
- **To revisit:** The group count (currently sized to the pool's 2 slots) should be revisited if the CPU quota is increased later (ADR-0002 action item left open) — more quota could support more groups running in parallel, trading a bit more cost for further latency reduction.

## Action Items

1. [x] Add an optional `spark: SparkSession` constructor parameter to `BaseEpiasSparkJob` (`spark_jobs/spark_utils.py`); track whether the session was self-created; add a `finish()` method that only stops the session if self-created.
2. [x] Replace `self.spark.stop()` with `self.finish()` across all `bronze_to_silver_*.py` job files — mechanical, no logic change, preserves standalone (`python bronze_to_silver_x.py`) usage.
3. [x] Write `spark_jobs/silver_batch_runner.py`: accepts a run date (or `--backfill`) and a comma-separated source list, creates one SparkSession, discovers and runs each source's `*SilverJob` class in sequence with clear per-source start/end/exception logging, stops the session once at the end.
4. [x] Update `epias_sources.py` to split `ALL_SOURCES`/`BACKFILL_SOURCES` into `N` groups (`N` = `dataproc_batches` pool size, currently 2) computed from the live source list, and submit one `DataprocCreateBatchOperator` per group targeting `silver_batch_runner.py`.
5. [x] Update DAG wiring in `epias_dag.py` and `epias_backfill_dag.py`: each group's batch task depends on every bronze save task for sources in that group (many:1), replacing the current 1:1 wiring.
6. [x] Upload `silver_batch_runner.py` (and confirm all existing job files) to `gs://epias-data-lake/dataproc/jobs/`.
7. [x] Re-run `epias_medallion_pipeline_v3` for a test date and measure actual Silver-layer wall-clock time against the ~7–9 minute estimate before considering ADR-0002 action item 9 (hourly schedule) unblocked.

## Validation Results (2026-07-24)

Ran `epias_medallion_pipeline_v3` against a live date. Results:

- **`silver_batch_0`: 3m23s. `silver_batch_1`: 3m46s — running in parallel** (both started 13:12:34). Whole Silver layer: **~3m46s**, versus ~40 minutes pre-consolidation — matches the ADR's estimate and comfortably fits an hourly schedule.
- **Fault-isolation gap found and fixed during this test:** the first attempt used the default Airflow trigger rule (`all_success`) on each group's batch task. Three sources (`smf`, `idm_transactions`, `outages`) hit an unrelated EPIAS API rule (endDate must be in the past — irrelevant when testing against "today"), which failed their bronze fetch and, under `all_success`, caused their **entire group's Dataproc batch to be skipped** — silently blocking every other source sharing that group, a real regression versus the old 1:1 architecture where one source's bronze failure only affected that one source.
  - **Fix:** `make_silver_batch_task` now sets `trigger_rule="all_done"` — the batch runs regardless of individual bronze task outcomes, and `silver_batch_runner.py`'s existing per-source try/except (a missing/unreadable bronze file for one source raises and is caught, logged, and skipped) isolates the failure to just that source.
  - **Verified directly:** after the fix, `silver_batch_0`/`1` still reported `up_for_retry` (because 3 of their sources genuinely had no bronze data to read), but a live GCS check confirmed the *other* sources in the same batches (`pricing`, `consumption`, `weather`, `dams`, `generation`) all wrote fresh Silver partitions inside the batch's execution window — proving per-source isolation now works correctly inside the shared-session design.
- **Known cosmetic side effect:** because a Dataproc batch either fully succeeds or fully fails as an Airflow task, a retry of a partially-failed batch reprocesses every source in the group again, including ones that already succeeded. Harmless (idempotent — `write_silver` always overwrites the target partition) but slightly wasteful. Not addressed here; worth revisiting only if it becomes a real cost concern.

ADR-0002 action item 9 (hourly schedule) is now unblocked by this ADR's findings.
