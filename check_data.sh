#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# check_data.sh — EPIAŞ veri kalitesi hızlı kontrol
#
# Kullanım (Airflow container içinde):
#   bash check_data.sh          # tam rapor
#   bash check_data.sh quick    # özet satırlar
#   bash check_data.sh gaps     # boşluk detayı
#   bash check_data.sh dbt      # dbt source freshness + testler
# ─────────────────────────────────────────────────────────────────────────────

set -e
cd /opt/airflow

MODE="${1:-full}"

case "$MODE" in
  quick)
    python src/data_quality_check.py --quick
    ;;
  gaps)
    python src/data_quality_check.py --gaps
    ;;
  dbt)
    echo "=== dbt source freshness ==="
    cd epias_dbt && dbt source freshness --profiles-dir .
    echo ""
    echo "=== dbt tests ==="
    dbt test --profiles-dir . --select \
      assert_no_hourly_gaps \
      assert_ptf_positive \
      assert_generation_positive
    ;;
  *)
    python src/data_quality_check.py
    ;;
esac
