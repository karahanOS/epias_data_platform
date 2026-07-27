# ADR-0004: Closing the Data Quality Guarantee Gaps (Prevention, Detection, Alerting)

**Status:** Proposed
**Date:** 2026-07-27
**Deciders:** Mehmet Karahan Çetinkaya

## Context

After the 2026-07-25/26 Silver-dedup investigation (see `silver_dedup_gaps` memory) and the sustainability hardening that followed — 25 staging models now have uniqueness tests, and a `run_dbt_tests` task runs `dbt test` after every hourly `dbt run` — three gaps remain where "we can't fully guarantee data quality":

1. **4 tables have an unresolved true grain** (`stg_idm_transactions`, `stg_outages`, `stg_outages_daily`, `stg_supply_demand_curve`). Their new uniqueness tests fail every hour, correctly, because the real duplication root cause in each case isn't understood yet well enough to fix safely.
2. **Detection ≠ prevention.** `dbt test` runs *after* `dbt run` as a separate Airflow task. By the time a test fails, the (possibly bad) data has already landed in the Gold table — nothing currently stops it from being written, or stops a *downstream* model from building on top of it.
3. **No active alerting.** `on_failure_callback=notify_failure` only writes an `logger.error(...)` line into the Airflow task log. Nobody is notified unless they open the Airflow UI and look.

This is a personal/learning project — the stakes of a few stale/duplicate Gold rows are low, and cost-consciousness has been a running theme all session (GCP budget alert, VM sizing). Any fix here should be evaluated against that: the "right" answer is not necessarily the most robust one, it's the best guarantee-per-dollar (and per-hour-of-effort).

## Decision

Treat the three gaps as three independent, low-cost, incremental hardening steps rather than one big effort:

1. **Gap 1 (unresolved grain):** finish the investigation for the 4 open tables using the same method already proven today, rather than adding any new infrastructure.
2. **Gap 2 (prevention):** switch the two-task `run_dbt_gold_models` + `run_dbt_tests` pair to a single `dbt build` invocation, with the 4 known-open tests demoted to `severity: warn` (config in `schema.yml`) so they don't block anything while still unresolved, and `--fail-fast` enabled so any *other* (currently-passing) test failing for real stops that hour's downstream models from building on top of bad data.
3. **Gap 3 (alerting):** turn on Airflow's built-in `email_on_failure` with Gmail SMTP (App Password) rather than building custom notification code.

## Options Considered

### Gap 1 — the 4 unresolved-grain tables

#### Option A: Investigate and fix now (same method as the other 21 tables)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — same investigative pattern already used all session (inspect raw Silver, find the true key, fix or add QUALIFY dedup) |
| Cost | $0 — my time only, no new infra |
| Risk | Low if done carefully (today's session showed a wrong-grain guess is easy to make — e.g. the `id`-alone mistake for `outages`/`idm_transactions` — so this needs real data inspection, not another guess) |

**Pros:** Closes the gap completely, no lingering "known issue."
**Cons:** Time cost today; these are lower-traffic/lower-value tables (`outages`, `supply_demand_curve` aren't in the PTF-prediction critical path) — same effort could arguably wait.

#### Option B: Leave as documented, tests at `severity: warn`, revisit only when someone actually needs correct data from these 4 tables
| Dimension | Assessment |
|-----------|------------|
| Complexity | None |
| Cost | $0 |
| Risk | These 4 tables' downstream marts (`mart_gip_company_activity`, `mart_supply_shock_index`, etc.) may already have wrong numbers — accepted as pre-existing/unquantified risk |

**Pros:** Zero effort now; matches "fix what actually matters" for a learning project — none of these 4 feed the PTF forecasting path that's the project's actual centerpiece.
**Cons:** Kicks the can — the numbers in those specific marts stay unverified indefinitely unless something forces the issue.

### Gap 2 — detection vs. prevention

#### Option A: `dbt build` with `--fail-fast`, known-open tests demoted to `warn`
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — `dbt build` is a built-in dbt command (already available in dbt-core 1.7.18), and per-test `severity: warn` is one YAML key per test |
| Cost | $0 — no new infra, marginally *less* Airflow runtime (one task instead of two, and tests run interleaved with builds instead of as a full second pass) |
| Effect | A test failure on a model with default (error) severity now stops the *rest of that hour's build* (`--fail-fast` aborts the whole invocation) — this is real prevention: a bad `stg_pricing` row, for instance, would stop `mart_ml_features`/`mart_ptf_lag_features` from building on top of it that hour, rather than silently propagating |

**Pros:** Idiomatic dbt (this is literally what `dbt build` is for), zero added cost, meaningfully closes the "test after the damage is done" gap for everything except the 4 already-known exceptions.
**Cons:** `--fail-fast` is blunt — ANY unexpected test failure anywhere now aborts the *entire* hour's Gold refresh (not just the affected branch), trading a small availability cost for the correctness guarantee. For a hobby project this seems like the right trade, but worth confirming.

#### Option B: Keep `run_dbt_gold_models` + `run_dbt_tests` as two tasks, no `--fail-fast`
| Dimension | Assessment |
|-----------|------------|
| Complexity | None (status quo) |
| Cost | $0 |
| Effect | Purely detection, as today — no prevention |

**Pros:** No behavior change, no risk of a single flaky test taking down an entire hourly refresh.
**Cons:** Doesn't actually close Gap 2 at all.

#### Option C: Pre-merge "quarantine" staging pattern (validate a batch before merging into the real incremental table)
| Dimension | Assessment |
|-----------|------------|
| Complexity | High — custom SQL/orchestration per model, meaningfully more code to maintain |
| Cost | $0 in infra, high in engineering time |

**Pros:** Most robust — bad data literally never reaches the real table.
**Cons:** Disproportionate engineering investment for a personal project at this data volume; not worth it unless the stakes rise significantly.

### Gap 3 — alerting

#### Option A: Airflow's built-in `email_on_failure` + Gmail SMTP (App Password)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — set `AIRFLOW__SMTP__*` env vars + `email_on_failure: True` and an `email` list in `default_args`; no new code, Airflow already supports this natively |
| Cost | $0 — Gmail SMTP is free; one Gmail App Password (needs 2FA enabled on the account, a few minutes of one-time setup) |
| Reach | Email, to `mehmetkarahanc@gmail.com` — already the account tied to this project |

**Pros:** Zero cost, zero new code, uses infrastructure (Airflow + Gmail) already in place.
**Cons:** Email as a channel is easy to miss/mute; App Password setup is a manual one-time step outside my reach (needs the user's Google account 2FA settings).

#### Option B: Slack or Discord webhook on failure
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low-Medium — a webhook POST from a `PythonOperator`/`on_failure_callback`, needs a workspace + incoming webhook URL |
| Cost | $0 (free tier) but requires the user to already use/set up Slack or Discord for this |

**Pros:** More immediate/visible than email if the user already lives in Slack/Discord.
**Cons:** Extra account/workspace setup if not already using one for this project; more moving parts than Option A for the same outcome.

#### Option C: GCP Cloud Monitoring alerting policy on Airflow logs
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium-High — Airflow's logs are local files on the VM today, not in Cloud Logging; would need the Ops Agent configured to ship logs first |
| Cost | Low $ (Cloud Logging has a free tier) but real setup effort |

**Pros:** Most "cloud-native," integrates with the GCP budget-alert pattern already used for cost.
**Cons:** Disproportionate setup cost for what Option A already solves for free.

## Trade-off Analysis

Across all three gaps, the through-line is: **prefer configuration over new code, and new code over new infrastructure.** Gap 1's Option A/B choice is really "do we care about `outages`/`idm_transactions`/`supply_demand_curve` correctness right now" — a scope question, not a cost question, since both options cost the same (roughly $0, differing only in when the investigative time gets spent). Gap 2's Option A is close to strictly better than the status quo (Option B) at zero added cost, with the one caveat that `--fail-fast` changes failure blast radius from "one branch" to "the whole hourly run" — acceptable for a low-stakes personal project, worth a second look if this ever becomes something other tools rely on. Gap 3's Option A is the standout: genuinely free, uses infrastructure that already exists, and directly answers "how would I actually find out if something breaks" without asking the user to adopt a new tool.

## Consequences

- **Easier:** A real test failure (not one of the 4 known-open ones) now stops that hour's Gold refresh instead of quietly building on top of bad data, and an email lands in the user's inbox when it happens — closing the "silently rotting for months" failure mode that caused this whole investigation in the first place.
- **Harder:** `--fail-fast` means a single flaky/wrong test can black out an entire hour's Gold refresh (not just its own branch) — if that turns out to be too aggressive in practice, the fallback is to drop `--fail-fast` and accept plain `dbt build`'s default (test failures don't block sibling/downstream models).
- **To revisit:** the 4 open-grain tables should get their own investigation pass; if any of `outages`/`idm_transactions`/`supply_demand_curve` ever needs to be trusted (e.g., a future mart depends on it), that's the trigger to stop deferring Gap 1's Option B and do Option A instead.

## Action Items

1. [ ] Investigate `stg_idm_transactions`'s true grain (probably `(date, contractName, id)` per the job's original, since-simplified `primary_keys` comment) against real data, and fix or add a `QUALIFY` self-dedup.
2. [ ] Investigate `stg_outages`/`stg_outages_daily`'s true grain (id likely resets per API call/page, not per day) and fix similarly.
3. [ ] Investigate `stg_supply_demand_curve` — likely missing an `hour` dimension entirely; confirm against raw Silver `supply_demand` and add it if so.
4. [ ] Mark the 4 above tests' severity as `warn` in `schema.yml` (config: severity: warn) so they don't trip `--fail-fast` while open.
5. [ ] Replace `run_dbt_gold_models` + `run_dbt_tests` in `dags/epias_dag.py` with a single `dbt build --fail-fast` task (same `--exclude` list).
6. [ ] Set up Gmail App Password, configure `AIRFLOW__SMTP__*` env vars in `docker-compose.yml`/`.env`, and set `email_on_failure: True` + `email: ["mehmetkarahanc@gmail.com"]` in `default_args`.
