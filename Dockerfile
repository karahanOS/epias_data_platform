FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow
# dbt'nin yerini sisteme tanıtıyoruz
ENV PATH="${PATH}:/home/airflow/.local/bin"

RUN pip install --no-cache-dir --upgrade \
    "dbt-bigquery<1.8.0" \
    "dbt-core<1.8.0" \
    openmeteo-requests \
    requests-cache \
    retry-requests \
    shap \
    xgboost \
    scikit-learn \
    pandas \
    numpy \
    docker \
    optuna \
    joblib \
    google-cloud-bigquery \
    pyarrow \
    holidays \
    scipy \
    statsmodels