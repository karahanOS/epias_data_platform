FROM apache/airflow:2.8.0

USER root

# 1. Sistem paketleri ve Java (Spark için şart)
# HATA DÜZELTİLDİ: "openjdk-17-jr-headless" → "openjdk-17-jre-headless"
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Değişken Tanımları
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${JAVA_HOME}/bin:$PATH"

# 3. Spark İndir ve Kur
RUN curl -fL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar -xz -C /opt && \
    mv "/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" "$SPARK_HOME"

# 4. Yetki ver ve symlink oluştur
RUN chmod -R 755 /opt/spark && \
    ln -sf /opt/spark/bin/spark-submit /usr/bin/spark-submit

# 5. /home/airflow izinlerini düzelt (pip izin hatasını önler)
RUN mkdir -p /home/airflow/.local && \
    chown -R airflow:root /home/airflow/.local

USER airflow
ENV PATH="${PATH}:/home/airflow/.local/bin"

# 6. Python paketleri — _PIP_ADDITIONAL_REQUIREMENTS'a gerek kalmaz
RUN pip install --no-cache-dir --upgrade \
    "apache-airflow-providers-apache-spark" \
    "pyspark==3.5.0" \
    "dbt-bigquery<1.8.0" \
    "dbt-core<1.8.0" \
    pandas numpy requests google-cloud-bigquery pyarrow \
    xgboost scikit-learn openmeteo-requests requests-cache retry-requests