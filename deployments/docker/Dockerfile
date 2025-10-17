##############################################
# STAGE 1 — Spark Base Build
##############################################
FROM eclipse-temurin:17-jre-alpine AS base

ARG SPARK_VERSION=3.5.6
ARG SPARK_MAJOR_VERSION=3.5
ARG HADOOP_VERSION=3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.466
ARG ICEBERG_VERSION=1.9.0
ARG SCALA_VERSION=2.12

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"

RUN apk add --no-cache bash curl tar  && \
    mkdir -p ${SPARK_HOME}/jars && \
    echo "Downloading Spark..." && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/spark --strip-components=1 && \
    echo "Downloading Iceberg and AWS connectors..." && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" -o ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" -o ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

##############################################
# STAGE 2 — Spark Runtime
##############################################
FROM python:3.11-bookworm AS spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

COPY --from=base /opt/spark /opt/spark

RUN pip install --no-cache-dir pyspark==3.5.6 jupyter==1.1.0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

##############################################
# STAGE 3 — Airflow with Spark Integration
##############################################
FROM apache/airflow:3.1.0-python3.11 AS airflow

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

COPY --from=base /opt/spark /opt/spark
RUN chown -R airflow ${SPARK_HOME}

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.3.2 pyspark==3.5.6
