FROM apache/airflow:2.8.0

USER root
# Docker CLI kurulumu (Zaten yapmıştık, dokunma)
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.0.0.tgz -o docker.tgz \
    && tar -xzvf docker.tgz --strip-components=1 -C /usr/local/bin docker/docker \
    && rm docker.tgz

# ... (Üst kısımlar aynı kalıyor) ...

USER airflow

# Tüm ML ve Optimizasyon kütüphanelerini tek seferde kuruyoruz
RUN pip install --no-cache-dir --upgrade \
    docker \
    dbt-bigquery \
    pandas \
    numpy \
    xgboost \
    optuna \
    scikit-learn \
    joblib \
    google-cloud-bigquery \
    pyarrow \
    holidays \
    scipy \
    statsmodels