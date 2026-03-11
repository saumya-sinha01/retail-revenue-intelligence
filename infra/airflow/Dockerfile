FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.1

# Add Hadoop S3 connector jars
RUN mkdir -p /opt/spark/jars

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar