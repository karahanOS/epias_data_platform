"""
Single source of truth for EPIAS data source configuration.

Imported by epias_dag.py (daily pipeline) and epias_backfill_dag.py.

Tuple fields: (method_name, gcs_path, allow_empty, backfill_eligible, daily_eligible)
  - backfill_eligible : include in epias_backfill_dag.py runs
  - daily_eligible    : include in epias_dag.py daily runs
"""

SPARK_CONN_ID       = "spark_default"
GCS_CONNECTOR_JAR   = "/opt/spark/jars/gcs-connector.jar"
SPARK_UTILS_PATH    = "/opt/airflow/spark/spark_utils.py"

EPIAS_SOURCES: dict[str, tuple[str, str, bool, bool, bool]] = {
    # key: (method_name, gcs_path, allow_empty, backfill_eligible, daily_eligible)
    "pricing":          ("get_ptf_smf_sdf",                 "bronze/pricing",          False, True,  True),
    "smf":              ("get_smf",                         "bronze/smf",              False, True,  True),
    "consumption":      ("get_realtime_consumption",        "bronze/consumption",      False, True,  True),
    "supply_demand":    ("get_supply_demand",               "bronze/supply_demand",    False, True,  True),
    "dam_clearing":     ("get_dam_clearing_quantity",       "bronze/dam_clearing",     False, True,  True),
    "price_ind_bid":    ("get_price_independent_bid",       "bronze/price_ind_bid",    False, True,  True),
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
    "unlicensed":       ("get_unlicensed_generation",       "bronze/unlicensed",       False, False, True),
    # static reference data: runs daily but not backfilled (no date dimension)
    "participants":     ("get_market_participants",         "bronze/participants",     True,  False, True),
    # backfill-only: not yet promoted to daily pipeline
    "sbfgp":            ("get_sbfgp",                       "bronze/sbfgp",            False, True,  False),
}

# Models excluded from dbt runs while their Silver backfill is incomplete.
# Remove a model from this list once its backfill is complete and dbt builds cleanly.
#   stg_dpp          → remove after silver_dpp_backfill completes
#   stg_sbfgp        → remove after silver_sbfgp_backfill completes
#   stg_res_forecast → remove after silver_res_forecast_backfill completes
#   mart_production_plan → remove after all upstream staging backfills complete
DBT_EXCLUDE_PENDING_BACKFILL: list[str] = [
    "stg_dpp",
    "stg_sbfgp",
    "stg_res_forecast",
    "mart_production_plan",
]


def make_silver_task(dag, source_key: str, is_backfill: bool = False):
    # Lazy import keeps this module importable without Airflow (e.g. in unit tests).
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    suffix = "_backfill" if is_backfill else ""
    args   = ["1970-01-01", "--backfill"] if is_backfill else ["{{ ds }}"]
    return SparkSubmitOperator(
        task_id=f"silver_{source_key}{suffix}",
        application=f"/opt/airflow/spark/bronze_to_silver_{source_key}.py",
        py_files=SPARK_UTILS_PATH,
        jars=GCS_CONNECTOR_JAR,
        conn_id=SPARK_CONN_ID,
        application_args=args,
        deploy_mode="client",
        name=f"epias_silver_{source_key}{suffix}",
    )
