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
#    catboost eklendi (2026-08-18): smf_trainer.py'nin yön sınıflandırıcısı
#    XGBoost'tan CatBoost'a geçti; joblib.load() unpickle sırasında bu paketi
#    import edebilmeli, yoksa smf_inference.py ModuleNotFoundError ile çöker
#    (production'da 2026-08-16 gecesi böyle oldu — bkz. ml_model_quality notu).
#    xgboost/scikit-learn/catboost pinned (2026-08-26): bu üçü daha önce
#    pinsiz kuruluyordu, image her rebuild'de PyPI'daki en güncel sürümü
#    alıyordu — trainer'ın (yerel, daha yeni Python) kullandığı sürümlerden
#    sessizce sürüklenip aynı model dosyası + aynı feature'larla production'da
#    çok daha düşük fiyat tahminleri üretmişti, hatasız ama yanlış.
#    Buradaki sürümler apache/airflow:2.8.0'ın Python 3.8'iyle uyumlu en
#    yeni sürümler (xgboost 3.x/scikit-learn 1.4+ Python 3.8'i desteklemiyor)
#    — trainer da modeli bu sürümlerle eşleşen bir ortamda eğitmeli
#    (bkz. venv_prod_match/), yoksa aynı sürüklenme tekrar yaşanır.
RUN pip install --no-cache-dir --upgrade \
    "apache-airflow-providers-google" \
    "dbt-bigquery<1.8.0" \
    "dbt-core<1.8.0" \
    pandas numpy requests google-cloud-bigquery pyarrow \
    "xgboost==2.1.4" "scikit-learn==1.3.2" openmeteo-requests requests-cache retry-requests \
    "catboost==1.2.10"