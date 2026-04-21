# spark_utils.py
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    Tüm projedeki Spark job'ları için merkezi, GCS yetkili SparkSession oluşturur.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/credentials/gcp-key.json") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
        
    return spark