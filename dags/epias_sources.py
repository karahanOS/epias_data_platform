"""
Single source of truth for EPIAS data source configuration.

Imported by epias_dag.py (daily pipeline) and epias_backfill_dag.py.

Tuple fields: (method_name, gcs_path, allow_empty, backfill_eligible, daily_eligible)
  - backfill_eligible : include in epias_backfill_dag.py runs
  - daily_eligible    : include in epias_dag.py daily runs
"""
from __future__ import annotations  # PEP 563 — defers annotation evaluation (Python 3.8 compat)

# ── DATAPROC SERVERLESS (ADR-0002: dags/../plans/02-spark-airflow-hosting-migration.md) ──
# Replaces the always-on docker-compose spark-master/spark-worker containers with
# pay-per-batch Dataproc Serverless jobs. See the ADR's "Pilot Results" section for
# the IAM setup this depends on (project-level roles/dataproc.editor,
# roles/storage.objectAdmin on the bucket, and roles/iam.serviceAccountUser on itself).
GCP_PROJECT_ID           = "epias-data-platform"
DATAPROC_REGION          = "europe-west1"           # matches the epias-data-lake bucket's region
DATAPROC_SERVICE_ACCOUNT = f"epias-dataproc@{GCP_PROJECT_ID}.iam.gserviceaccount.com"
DATAPROC_JOBS_PREFIX     = "gs://epias-data-lake/dataproc/jobs"
DATAPROC_RUNTIME_VERSION = "2.2"

EPIAS_SOURCES: dict[str, tuple[str, str, bool, bool, bool]] = {
    # key: (method_name, gcs_path, allow_empty, backfill_eligible, daily_eligible)
    #
    # pricing/dam_clearing/price_ind_bid: allow_empty=True since 2026-07-29.
    # These are Gün Öncesi Piyasası (day-ahead market) endpoints — EPIAS's own
    # API returns a business-logic 400 ("... saat 14 öncesinde mevcut değil")
    # for the current day's data until that day's auction/publish cutoff, and
    # epias_client.py's _post() already anticipates this exact case (matches
    # errorCode "BUS*" or "veri bulunmamaktadır") and gracefully returns
    # {"items": []} instead of raising. With allow_empty=False, that graceful
    # empty response still hit save_to_gcs_callable's hard ValueError, which
    # cascaded into silver_batch_0/1 failing and run_dbt_gold_models/
    # load_silver_to_bigquery getting marked upstream_failed — blocking the
    # ENTIRE hourly Gold refresh for every run before the publish cutoff (was
    # 7 consecutive hourly failures every single day, 2026-07-29 03:00-09:00
    # UTC, self-resolving by 10:00 UTC once EPIAS published). This pattern
    # existed long before today but was invisible until ADR-0004's
    # email_on_failure alerting started surfacing it. `smf` uses a different
    # endpoint (BPM, not day-ahead) and was NOT affected — confirmed via the
    # same failing runs — so it intentionally keeps allow_empty=False.
    #
    # supply_demand: allow_empty=True added 2026-07-30, proactively — same
    # `/v1/markets/dam/*` (Gün Öncesi Piyasası) endpoint family as pricing/
    # dam_clearing/price_ind_bid above, same delay=0 same-day fetch pattern,
    # so it carries the identical "not published before ~14:00 TRT" risk even
    # though it hadn't actually failed yet as of this fix (found by auditing
    # every /v1/markets/dam/* method against EPIAS_SOURCES, not from an
    # observed failure — see the Turkish Electricity Day-Ahead Market: PTF is
    # officially announced daily at 14:00 TRT, confirmed via EPİAŞ's own
    # public documentation).
    "pricing":          ("get_ptf",                          "bronze/pricing",          True,  True,  True),
    # interim_mcp: K.PTF (itiraz-öncesi PTF) — DATA_DELAYS'te get_interim_mcp
    # için -1 (lead) tanımlı (epias_dag.py), yani her saatlik run YARININ
    # K.PTF'ini çekmeye çalışır. GÖP açık artırması kapanmadan (~14:00 TRT
    # öncesi) boş döner (allow_empty=True zaten bunu karşılıyor). Geçmiş
    # tarihlerde interim her zaman final ile birebir aynı (canlı test
    # edildi, 2026-08) — backfill'de stg_pricing'e göre fazladan bilgi
    # taşımıyor, bu yüzden backfill_eligible=False.
    "interim_mcp":      ("get_interim_mcp",                  "bronze/interim_mcp",      True,  False, True),
    "smf":              ("get_smf",                         "bronze/smf",              False, True,  True),
    "consumption":      ("get_realtime_consumption",        "bronze/consumption",      False, True,  True),
    "supply_demand":    ("get_supply_demand",               "bronze/supply_demand",    True,  True,  True),
    "dam_clearing":     ("get_dam_clearing_quantity",       "bronze/dam_clearing",     True,  True,  True),
    "price_ind_bid":    ("get_price_independent_bid",       "bronze/price_ind_bid",    True,  True,  True),
    "idm_transactions": ("get_idm_transaction_history",     "bronze/idm_transactions", False, True,  True),
    "order_up":         ("get_order_summary_up",            "bronze/order_up",         False, True,  True),
    "order_down":       ("get_order_summary_down",          "bronze/order_down",       False, True,  True),
    "system_direction": ("get_system_direction",            "bronze/system_direction", False, True,  True),
    "dpp":              ("get_dpp",                         "bronze/dpp",              False, True,  True),
    "aic":              ("get_aic",                         "bronze/aic",              False, True,  True),
    "imbalance":        ("get_imbalance_quantity",          "bronze/imbalance",        False, True,  True),
    "res_forecast":     ("get_res_generation_and_forecast", "bronze/res_forecast",     False, True,  True),
    "generation":       ("get_realtime_generation",         "bronze/generation",       False, True,  True),
    "load_estimation":  ("get_load_estimation_plan",        "bronze/load_estimation",  False, True,  True),
    "outages":          ("get_outages",                     "bronze/outages",          False, True,  True),
    "dams":             ("get_dams",                        "bronze/dams",             False, True,  True),
    # daily-only: slow bulk / not historically meaningful
    "injection":        ("get_injection_quantity",          "bronze/injection",        True,  False, True),
    "uevcb_list":       ("get_uevcb_list",                  "bronze/uevcb_list",       True,  False, True),
    # unlicensed: backfill_eligible flipped True 2026-07-30. This source is
    # published monthly (~35-day lag per get_unlicensed_generation()'s own
    # docstring), and the daily pipeline's single-day T-35 rolling window
    # structurally misses most days (only catches a date if it happens to
    # land exactly on/after that month's settlement). A periodic full-range
    # historical backfill (which derives partitions from the actual date in
    # the data, not from the requested ds) is the only way to catch up —
    # re-run `epias_historical_backfill` scoped to "unlicensed" periodically
    # (e.g. monthly) until this gets a proper recurring catch-up mechanism.
    # allow_empty flipped True 2026-07-30 too: the most recent 1-2 months are
    # legitimately not settled/published yet at any given backfill time (same
    # "not published yet" shape as the day-ahead-market allow_empty fix
    # earlier today), so a strict raise-on-empty here would permanently fail
    # the tail end of every historical backfill run.
    "unlicensed":       ("get_unlicensed_generation",       "bronze/unlicensed",       True,  True,  True),
    # static reference data: runs daily but not backfilled (no date dimension)
    "participants":     ("get_market_participants",         "bronze/participants",     True,  False, True),
    # backfill-only: not yet promoted to daily pipeline
    "sbfgp":            ("get_sbfgp",                       "bronze/sbfgp",            False, True,  False),
    # dam_clearing_by_org: ADR-0007 Faz 1 (plans/07-company-level-market-activity-kgup.md).
    # Şirket bazlı GÖP eşleşme miktarı (matchedBids/matchedOffers). GİP'te
    # organizasyon atfı hiç yok (canlı doğrulandı) ama GÖP'te clearing-quantity
    # organizationId filtresi destekliyor — tek eksik roster'ı, ki onu da
    # clearing-quantity-organization-list veriyor. Bulk endpoint YOK: roster'daki
    # HER organizasyon için ayrı bir POST demek (canlı testte 1629 organizasyon,
    # ~20 dakika/gün @ ~80 req/dk limiti).
    #
    # ⚠️  daily_eligible=False BİLEREK: bu flag'in adı yanıltıcı — epias_dag.py'de
    # "daily_eligible=True" olan HER kaynak, ALL_SOURCES üzerinden asıl hourly
    # (schedule_interval="0 * * * *") pipeline'ın HER çalışmasına giriyor, günde
    # bir kez değil (bkz. epias_dag.py:236 ALL_SOURCES = {... if v[4]}). Bu kaynağı
    # oraya koymak 20 dakikalık işi saatte bir tekrarlamak, yani günde ~8 saat API
    # çağrısı ve muhtemelen üst üste binen DAG run'ları demek olurdu (aynı sınıf
    # hata epias_ptf_training_weekly.py'nin ayrılma gerekçesiyle — bkz. o dosyanın
    # docstring'i). Bunun yerine dedicated bir günlük DAG kullanılıyor:
    # dags/epias_gop_company_activity_dag.py (schedule_interval="30 11 * * *",
    # yani ~14:30 TRT — GÖP açık artırması kapandıktan sonra). Bu dict girdisi
    # sadece method_name/gcs_path için tek doğru kaynak (DRY) olarak burada duruyor.
    # backfill_eligible=False: N gün × 1629 org backfill maliyeti aşırı; geriye
    # dönük doldurma gerekirse ayrı, hız sınırlı bir script ile yapılmalı.
    "dam_clearing_by_org": ("get_dam_clearing_quantity_by_organization", "bronze/dam_clearing_by_org", True, False, False),
}

# Models excluded from dbt runs while their Silver backfill is incomplete.
# Remove a model from this list once its backfill is complete and dbt builds cleanly.
#
# Status as of 2026-07-29:
#   stg_dpp          ✅ UNBLOCKED — the "Hive partition schema mismatch" was gone by the
#                        time this was re-investigated (fixed incidentally by an external
#                        table re-registration run during an unrelated backfill). The
#                        model itself was still missing a dedup step every sibling hourly
#                        model already has — added the same cross-Hive-partition-boundary
#                        QUALIFY/ROW_NUMBER self-heal (6 duplicate (date,hour) rows found
#                        at hours 00/01/02, the usual UTC-vs-Turkish-local-day pattern).
#                        Verified: dbt full-refresh + all 4 schema.yml tests pass clean.
#                        Removed stg_dpp from this list.
#   stg_res_forecast ✅ Gold  has 73 803 rows  — UNBLOCKED, removed from list (earlier)
#   stg_sbfgp        ❌ Silver table not found  — still excluded
#   stg_order_down   ✅ UNBLOCKED — the stale-partition issue documented here turned out
#                        to be a much broader Silver-wide duplication (all 526 days,
#                        2-3.6x) plus a nanosecond-timestamp precision bug affecting
#                        every file, not just the one flagged day. Fixed via a full
#                        dedup + timestamp-precision rewrite of the whole table.
#                        `mart_ml_features`/`mart_ptf_lag_features` also needed
#                        `stg_load_vs_actual` (consumption) deduped the same way, and a
#                        `CAST(... AS NUMERIC)` in stg_imbalance.sql fixed to FLOAT64
#                        (BigQuery NUMERIC -> pandas Decimal/object breaks XGBoost).
#                        Verified: dbt full-refresh of the whole chain succeeds, and
#                        ptf_hourly_inference runs and predicts successfully. Removed
#                        stg_order_down + its 7 downstream models from this list.
#   mart_production_plan → still excluded; needs stg_dpp + stg_sbfgp backfills, unrelated
#                        to the stg_order_down fix above
DBT_EXCLUDE_PENDING_BACKFILL: list[str] = [
    "stg_sbfgp",
    "mart_production_plan",
]


# ── DATAPROC BATCH CONSOLIDATION (ADR-0003: plans/03-dataproc-batch-consolidation.md) ──
# A full validation run (2026-07-23) showed every Dataproc Serverless batch takes
# 3-4.5 minutes almost entirely due to cold start (session provisioning, runtime
# image pull) — actual per-source data volume is tiny. With one batch per source
# and the dataproc_batches pool limited to 2 concurrent slots (12 vCPU/batch,
# 24-vCPU quota — see ADR-0002), ~24 sources took ~40 minutes serialized in waves
# of 2, incompatible with an hourly schedule. Fix: group sources into POOL_SIZE
# batches, each running its sources sequentially inside ONE shared Spark session
# via silver_batch_runner.py, so cold start is paid POOL_SIZE times, not once per
# source. DATAPROC_POOL_SIZE must stay in sync with the actual Airflow pool's
# slot count (`airflow pools set dataproc_batches <n> ...` / Admin -> Pools).
DATAPROC_POOL      = "dataproc_batches"
DATAPROC_POOL_SIZE = 2


def group_sources(keys: list, n_groups: int = DATAPROC_POOL_SIZE) -> list:
    """Split source keys into n_groups roughly-equal, order-preserving chunks."""
    n_groups = max(1, min(n_groups, len(keys)))
    groups = [[] for _ in range(n_groups)]
    for i, key in enumerate(keys):
        groups[i % n_groups].append(key)
    return [g for g in groups if g]


def make_silver_batch_task(task_id: str, sources: list, ds_args: list):
    """
    One Dataproc Serverless batch running several sources' bronze_to_silver_*.py
    transforms sequentially inside a single shared Spark session (see
    spark_jobs/silver_batch_runner.py) — instead of one batch, and one ~3-4 min
    cold start, per source. Each source's own transform logic is unchanged.
    """
    # Lazy import keeps this module importable without Airflow (e.g. in unit tests).
    from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
    safe_id = task_id.replace("_", "-")
    python_file_uris = [f"{DATAPROC_JOBS_PREFIX}/spark_utils.py"] + [
        f"{DATAPROC_JOBS_PREFIX}/bronze_to_silver_{s}.py" for s in sources
    ]
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"{DATAPROC_JOBS_PREFIX}/silver_batch_runner.py",
                "python_file_uris": python_file_uris,
                "args": ds_args + [f"--sources={','.join(sources)}"],
            },
            "runtime_config": {
                "version": DATAPROC_RUNTIME_VERSION,
                # NOTE: Dataproc Serverless enforces a hard minimum of 2 executors
                # (confirmed empirically — "Serverless Spark must have at least 2
                # initial executors" when spark.executor.instances=1 was tried).
                # Combined with the 4-cores-per-unit default, 12 vCPU (1 driver +
                # 2 executors) is the practical floor for a batch — not reducible.
                # Fits within the increased CPUS_ALL_REGIONS quota (see ADR-0002).
            },
            "environment_config": {
                # Must be explicit — without it Dataproc defaults to the project's
                # default Compute Engine service account, which our dedicated
                # service account has no rights to impersonate (see ADR-0002).
                "execution_config": {"service_account": DATAPROC_SERVICE_ACCOUNT},
            },
        },
        # Dataproc batch IDs must be unique and DNS-safe (lowercase, hyphens only).
        # Must include try_number — a batch_id tied only to the logical timestamp
        # is IDENTICAL across retries/clears of the same task instance, so a task
        # that failed once (e.g. hit a transient quota error) would just re-attach
        # to that same already-failed batch on every subsequent retry forever,
        # instead of submitting a fresh one (observed directly during ADR-0002
        # rollout — "Batch with given id already exists" on every retry).
        batch_id=f"epias-{safe_id}-{{{{ ts_nodash | lower }}}}-{{{{ ti.try_number }}}}",
        # Cap concurrent Dataproc batch submissions at DATAPROC_POOL_SIZE (pool's
        # slot count) so total requested vCPU (12/batch) never exceeds the
        # 24-vCPU CPUS_ALL_REGIONS quota, regardless of the DAG's max_active_tasks.
        # Bronze fetch/save tasks are unaffected — only Dataproc-submitting
        # tasks use this pool. See ADR-0002.
        pool=DATAPROC_POOL,
        # ADR-0003 fault isolation: with per-source batches, one source's bronze
        # fetch failing only blocked that one source's silver task. Grouping
        # sources into shared batches means the default "all_success" trigger
        # rule would skip the WHOLE group's batch (and every other source in
        # it) if even one source's bronze task fails — observed directly on a
        # test run where get_smf/get_idm_transactions/get_outages hit an
        # unrelated EPIAS API date-validity error and silently took down every
        # other source sharing their group. "all_done" restores per-source
        # isolation: silver_batch_runner.py's own try/except already skips a
        # missing/unreadable bronze file for one source without affecting the
        # rest of the group.
        trigger_rule="all_done",
    )
