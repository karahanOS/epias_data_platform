# ADR-0002: Spark/Airflow Hosting Migration — From Laptop docker-compose to Always-On Cloud

**Status:** Accepted — fully implemented 2026-07-25 (all action items 1-9 done; item 10, a GCP budget alert, remains open as a low-priority follow-up)
**Date:** 2026-07-21 (pilot completed 2026-07-24)
**Deciders:** Mehmet Karahan Çetinkaya

## Context

The pipeline (`epias_medallion_pipeline_v3` DAG, scheduled daily at 05:00 UTC) runs entirely inside a local `docker-compose` stack: `spark-master`, `spark-worker`, `postgres` (Airflow metadata), `airflow-webserver`, `airflow-scheduler` — all `restart: unless-stopped`, all on one machine (the developer's laptop).

This has already caused a real incident: when the laptop is off or asleep, Airflow does not run, and because the DAG is defined with `catchup=False`, missed days are **not** backfilled automatically — they require a manual run of `epias_historical_backfill`. A data-completeness audit earlier in this project found ~40 days of missed ingestion across nearly every hourly table, traced directly to this.

Goals for the next iteration:
- Move to a schedule that can run **hourly** without depending on the laptop being on.
- Keep cost low — this is a personal/learning project, not a funded production system.
- Preserve the "real" data-engineering stack (Spark, Airflow, dbt, medallion architecture) because it has direct value when explaining/presenting this project (interviews, portfolio), even though the actual data volume (order of 10–100 rows/hour per source, ~20 sources) does not by itself require Spark-scale compute.

The tension: an always-on VM that keeps `spark-master`/`spark-worker` running 24/7 to serve a job that only needs to execute for 1–2 minutes/hour is the most expensive part of the stack relative to the work it actually does.

## Decision

Move Spark execution from **always-on local containers** to **GCP Dataproc Serverless for Spark** (pay-per-batch, no idle cost), while keeping Airflow's scheduler/webserver on a small always-on VM (or later, revisit whether Airflow itself needs to be always-on). The existing PySpark job scripts (`bronze_to_silver_*.py`) do not need to be rewritten — only the Airflow operator that submits them changes (`SparkSubmitOperator` → `DataprocCreateBatchOperator` or equivalent).

This directly answers both constraints raised in discussion: migration effort is limited to the DAG's operator layer (not a rewrite of 15–20 job scripts to pandas), and the stack retains a managed Spark service — arguably a *stronger* portfolio point than local docker-compose Spark, since Dataproc is how Spark is actually run in most real GCP-based data platforms.

## Options Considered

### Option A: Status quo (laptop-hosted docker-compose)
| Dimension | Assessment |
|-----------|------------|
| Complexity | None — already built |
| Cost | $0 direct hosting cost, but BigQuery/GCS costs continue regardless |
| Scalability | None — depends on laptop uptime |
| Team familiarity | High (already running) |

**Pros:** Zero migration effort, zero added cloud spend.
**Cons:** The exact failure mode that caused ~40 days of data loss. Cannot support an hourly schedule reliably. Not viable if the stated goal is "real scenario, hourly."

### Option B: Lift-and-shift — same docker-compose stack on an always-on GCE VM
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — no code changes, just re-host |
| Cost | ~$25–30/month (e2-medium, 2 vCPU/4GB, sized for Spark master+worker+Postgres+Airflow together) |
| Scalability | Fine for this volume, but pays 24/7 for Spark containers idle ~98% of the time |
| Team familiarity | High |

**Pros:** Simplest possible fix for Faz 0 — smallest engineering effort.
**Cons:** Pays continuously for Spark compute that is only briefly needed each hour. Doesn't reduce cost, just moves the uptime problem to a bill.

### Option C: Cloud Composer (managed Airflow)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low to adopt, but a full platform migration (Composer has its own DAG deployment model) |
| Cost | ~$300+/month minimum, even for the smallest environment |
| Scalability | Excessive for one DAG at this volume |
| Team familiarity | New tool to learn |

**Pros:** Fully managed, no VM to maintain, GCP-native.
**Cons:** Cost is wildly disproportionate to the workload. Rejected outright for this project's scale.

### Option D: Drop Spark entirely — pandas/BigQuery transform in Cloud Run/Cloud Functions + Cloud Scheduler
| Dimension | Assessment |
|-----------|------------|
| Complexity | High — rewrite of ~15–20 `bronze_to_silver_*.py` jobs, re-plumb the Airflow DAG or replace Airflow outright |
| Cost | ~$0–5/month (likely inside GCP free tier — Cloud Run free tier covers far more invocations/compute-seconds than 24 runs/day would use) |
| Scalability | More than sufficient for current volume; would need revisiting only at much higher scale |
| Team familiarity | Requires new serverless patterns, but simpler individual code |

**Pros:** Cheapest possible option by a wide margin.
**Cons:** Discards the Spark/Airflow story this project is explicitly meant to demonstrate. Highest migration effort of all options (full rewrite, not just a config/operator change). Rejected per explicit discussion — cost savings don't offset the loss of "industry-standard stack" narrative value and the rewrite cost.

### Option E (chosen): Dataproc Serverless for Spark + small always-on VM for Airflow
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — swap Airflow operator (`SparkSubmitOperator` → `DataprocCreateBatchOperator`), adjust IAM/service account for Dataproc, no changes to the PySpark job files themselves |
| Cost | Dataproc Serverless billed per batch (~1–2 min/hour of actual Spark compute) — a few dollars/month at most; Airflow VM shrinks to e2-micro/e2-small (~$0–14/month, e2-micro is Always-Free-tier eligible in supported regions) |
| Scalability | Scales automatically per job; no capacity planning needed |
| Team familiarity | New GCP service to learn (Dataproc Batches API), but Spark job code is unchanged |

**Pros:** Keeps the real Spark/Airflow/dbt/medallion story intact. Removes the single biggest cost driver (idle Spark containers) without a large rewrite. Airflow VM can be downsized since it no longer hosts Spark.
**Cons:** Introduces a new GCP service (Dataproc) and its IAM/networking setup. Airflow itself still needs an always-on host for the scheduler (a smaller, cheaper one, but not zero).

## Trade-off Analysis

The real decision is between **Option B** (fastest, but doesn't solve cost), **Option D** (cheapest, but discards the stack's teaching/portfolio value and costs the most engineering time), and **Option E** (moderate effort, keeps the stack, removes most of the idle-cost problem).

Given the explicit constraint from discussion — "industry-standard stack matters for explaining the project" — Option D is rejected despite being cheapest. Between B and E, E is preferred because the cost problem specifically comes from Spark idling 24/7; Option E fixes that root cause directly, while Option B just relocates the same inefficiency to a cloud bill. The added complexity of E (learning Dataproc's Batches API, IAM for a Dataproc service account) is a one-time cost, not recurring, and is itself a reasonable thing to learn since Dataproc is a standard tool in GCP-based data platforms.

Airflow's own always-on requirement is a separate, smaller problem, deliberately left open in this ADR (see Consequences) rather than solved immediately, since it's a much smaller cost lever than the Spark question.

## Pilot Results (2026-07-24)

The `dams` source (chosen for its low volume) was migrated to `dags/epias_dataproc_pilot_dag.py` and run against a historical date (`2026-05-29`, already processed by the existing Spark-on-docker-compose path). Result: **success**, batch runtime 3m36s end-to-end (submit → provision → run → write). Output written to `gs://epias-data-lake/silver/dams/year=2026/month=05/day=29/` as a single clean Parquet file (3 rows, one per basin, deduplicated correctly per `primary_keys=["date","basinName"]`) — `bronze_to_silver_dams.py` required zero code changes.

GCS connectivity worked natively under Dataproc Serverless's default runtime (version `2.2`), confirming the assumption in action item 4 — no manual `gcs-connector.jar` needed, unlike the current docker-compose Spark image.

### IAM corrections found during the pilot (action item 1 was under-scoped)

The original action item 1 recommended granting the Dataproc service account only `roles/dataproc.worker` at the project level plus `roles/storage.objectAdmin` on the bucket. This turned out to be **insufficient** — three separate permission errors had to be resolved in sequence before the pilot succeeded:

1. **Dataproc API disabled** — expected (action item 1's first step), just needed enabling and a short propagation wait.
2. **`dataproc.batches.create` denied** — `roles/dataproc.worker` does not include permission to *create* a batch, only to run *as* one. Had to additionally grant **`roles/dataproc.editor`** at the project level to the same service account.
3. **`User not authorized to act as service account '...-compute@developer.gserviceaccount.com'`** — without an explicit `execution_config.service_account` in the batch config, Dataproc defaulted to the project's default Compute Engine service account, which our dedicated service account has no rights to impersonate. Fixed by explicitly setting `environment_config.execution_config.service_account` in the batch config to our own service account (`epias-dataproc@...`), keeping everything on the one dedicated identity rather than granting rights on the broader default Compute SA.
4. **Self-impersonation denied** — even after (3), GCP still required the service account to hold **`roles/iam.serviceAccountUser` on itself** (granted via the service account's own Permissions tab, not the project IAM page) before it could submit a batch that runs as itself.

**Corrected minimum IAM setup for the Dataproc service account** (supersedes the original action item 1):
- Project-level: `roles/dataproc.editor` (covers `dataproc.worker`'s needs too — a project-level `dataproc.worker` binding was left in place but is redundant once `editor` is granted)
- Bucket-level (`gs://epias-data-lake`): `roles/storage.objectAdmin`
- Self-binding (on the service account's own Permissions tab): `roles/iam.serviceAccountUser`
- Batch config must explicitly set `environment_config.execution_config.service_account` — do not rely on the Dataproc default

This corrected set should be applied directly (not rediscovered by trial and error) when the same service account is reused for the full rollout (action item 6).

## Consequences

- **Easier:** Spark cost becomes usage-proportional instead of flat 24/7. The always-on VM only needs to be sized for Airflow (scheduler + webserver) + Postgres, shrinking from e2-medium to e2-micro/e2-small.
- **Harder:** A new GCP service (Dataproc) enters the stack, with its own IAM service account, and its own Batches API to learn instead of the existing `SparkSubmitOperator` + `spark_default` connection. GCS connector JAR / dependency handling may need re-verification under Dataproc Serverless's runtime environment.
- **To revisit:** Whether Airflow's scheduler truly needs to be always-on for a single hourly DAG, or whether it can eventually be replaced by Cloud Scheduler directly invoking a Dataproc batch (dropping Airflow's own hosting cost too) — deferred because it's a bigger architectural change than this ADR's scope, and Airflow itself remains valuable to keep for the portfolio narrative.

## Action Items

1. [x] Enable the Dataproc API on the GCP project and create a dedicated Dataproc service account. **Corrected scope** (see Pilot Results): `roles/dataproc.editor` at project level + `roles/storage.objectAdmin` on `epias-data-lake` + `roles/iam.serviceAccountUser` on itself — the originally-planned `dataproc.worker`-only setup was insufficient.
2. [x] Pick one low-risk source job (`bronze_to_silver_dams`, small volume) as the migration pilot.
3. [x] Write a `DataprocCreateBatchOperator` equivalent for that one job in a scratch/test DAG (`dags/epias_dataproc_pilot_dag.py`), pointing at the existing `bronze_to_silver_dams.py` unchanged. Must set `environment_config.execution_config.service_account` explicitly (see Pilot Results item 3).
4. [x] Validate GCS connector / dependency availability under Dataproc Serverless's default runtime — confirmed native, no manual JAR needed.
5. [x] Run the pilot end-to-end once against a historical date (`2026-05-29`) — succeeded, output verified correct and deduplicated.
6. [x] Replace `SparkSubmitOperator` calls in `epias_dag.py` and `epias_backfill_dag.py` with the Dataproc equivalent for all sources. **Superseded by ADR-0003** (plans/03-dataproc-batch-consolidation.md): rather than one `DataprocCreateBatchOperator` per source (~24 batches, ~40 min wall-clock due to repeated cold starts), sources are grouped into `DATAPROC_POOL_SIZE` (2) consolidated batches via `silver_batch_runner.py` — same IAM/service-account setup, ~4 min Silver layer instead of ~40.
7. [x] Remove `spark-master`/`spark-worker` from `docker-compose.yml` (2026-07-24): both services deleted, `airflow-scheduler`'s `depends_on: spark-master` removed, the `spark_default` connection bootstrap in `airflow-init`'s command removed, `JAVA_HOME` dropped from `airflow-common`'s environment, and the now-dead `SPARK_CONN_ID`/`GCS_CONNECTOR_JAR`/`SPARK_UTILS_PATH` constants removed from `epias_sources.py` (unreferenced by any DAG once `SparkSubmitOperator` was replaced by `DataprocCreateBatchOperator` in action item 6). Also pared down `Dockerfile`: removed the `openjdk-17-jre-headless` apt install, the Spark tarball download/extract, the GCS connector JAR fetch, the `spark-submit` symlink, and the `apache-airflow-providers-apache-spark`/`pyspark` pip installs — none of it is needed since this container only submits Dataproc batches, it doesn't run pyspark itself. (Trade-off: `docker exec ... python bronze_to_silver_x.py` standalone-in-container testing is no longer possible; the `bronze_to_silver_*.py` files are unchanged and still run fine directly on Dataproc.) The remaining stack (`postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`) has no more local Spark/Java dependency and a meaningfully smaller image, clearing the way to actually resize the host once it's on a VM (item 8).
8. [x] Provision the small VM and move the docker-compose stack there (2026-07-25). Started as e2-micro (`epias-airflow-host`, us-central1, Always-Free) but hit sustained ~90% CPU and near-full memory running Postgres + Airflow webserver + scheduler together — SSH itself became unresponsive under the load. Resized to **e2-small** (2 vCPU shared, 2GB RAM, ~$13-14/month) via stop → edit machine type → start, which resolved it. Also fixed along the way: a missing `roles/iap.tunnelResourceAccessor`-equivalent firewall rule for Cloud IAP's SSH range (`35.235.240.0/20:22`, needed for Console's browser SSH to work reliably from any network); a `logs/` bind-mount ownership mismatch (`docker-compose.yml`'s `./logs` dir auto-created as root by Docker, blocking Airflow's UID-50000 process — fixed with `chown -R 50000:0 logs`); and a stale firewall source IP (`allow-airflow-ui` was scoped to the deployer's home IP, which rotated mid-setup — updated to the current IP; noted as needing occasional updates since it's a dynamic residential IP, not a static one). Verified: Airflow UI reachable at the VM's external IP on port 8080, DAGs loaded via `git clone` (repo is public; `credentials/` and `.env` transferred separately via Cloud Console's browser-SSH file upload, never through git). Original laptop docker-compose stack stopped by the user once the VM copy was confirmed working — Faz 0's root cause (Airflow depending on the laptop being on) is now resolved.
9. [x] Change the DAG schedule from daily to hourly. Done as a split rather than a single schedule edit, once user intent was clarified (fresh hourly EPIAS data feeding next-hour PTF prediction, not just resilience): `epias_medallion_pipeline_v3` (bronze→silver→dbt) now runs hourly (`0 * * * *`); `train_ptf_model` was pulled out into its own new weekly DAG (`epias_ptf_training_weekly.py`, Monday 03:00 UTC) since running a 10-30 min XGBoost retrain 24x/day contradicted its own documented weekly cadence for no benefit. `run_ptf_inference` needed no new DAG — `dags/ptf_inference_dag.py` (`ptf_hourly_inference`, schedule `0 * * * *`) already existed as a standalone hourly inference DAG; an initial attempt to create a duplicate was caught and removed once the pre-existing one was found.
10. [ ] Set a GCP budget alert (e.g. $20/month threshold) to catch any cost surprise early, given this is the first time Dataproc billing enters the picture.
