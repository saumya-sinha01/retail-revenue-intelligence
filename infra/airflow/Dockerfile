FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.1 boto3 requests psycopg2-binary

# Pre-download Spark S3 JARs so jobs don't re-download ~280MB on every run
RUN python - <<'EOF'
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("jar-prefetch")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .getOrCreate()
)
spark.stop()
EOF
