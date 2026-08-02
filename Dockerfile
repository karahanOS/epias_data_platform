FROM apache/airflow:2.8.0

USER root

# 1. Sistem paketleri
#    Java/Spark kurulumu kaldırıldı (2026-07-24, ADR-0002 action item 7):
#    Silver katmanı artık Dataproc Serverless'te çalışıyor
#    (DataprocCreateBatchOperator), bu container'ın kendisi pyspark
#    çalıştırmıyor — sadece batch submit ediyor.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. /home/airflow izinlerini düzelt (pip izin hatasını önler)
RUN mkdir -p /home/airflow/.local && \
    chown -R airflow:root /home/airflow/.local

USER airflow

ENV PATH="${PATH}:/home/airflow/.local/bin"

# 3. Python paketleri — _PIP_ADDITIONAL_REQUIREMENTS'a gerek kalmaz
#    optuna ve holidays eklendi (2026-08-02): ikisi de src/ptf_trainer.py ve
#    src/ptf_features.py tarafından try/except ile "gracefully" atlanıyordu —
#    yani hiçbir hata vermeden, hiperparametre araması (regularization dahil)
#    ve resmi tatil özelliği sessizce hiç çalışmadan production'da aylarca
#    kaldılar. Bkz. ml_model_quality memory notu.
RUN pip install --no-cache-dir --upgrade \
    "apache-airflow-providers-google" \
    "dbt-bigquery<1.8.0" \
    "dbt-core<1.8.0" \
    pandas numpy requests google-cloud-bigquery pyarrow \
    xgboost scikit-learn openmeteo-requests requests-cache retry-requests \
    optuna holidays