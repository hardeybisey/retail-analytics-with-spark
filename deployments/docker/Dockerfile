##############################################
# STAGE 1 — Spark Base Build
##############################################
FROM alpine/curl:8.14.1 AS base

ARG SPARK_VERSION=4.0.2
ARG SPARK_MAJOR_VERSION=4.0
ARG HADOOP_VERSION=3
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_VERSION=2.42.9
ARG ICEBERG_VERSION=1.10.1
ARG SCALA_VERSION=2.13
ARG KAFKA_VERSION=0-10
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.13.1

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"

RUN mkdir -p ${SPARK_HOME}/jars && \
    echo "Downloading Spark..." && \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/spark --strip-components=1 && \
    echo "Downloading Iceberg and AWS connectors..." && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" -o ${SPARK_HOME}/jars/bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" -o ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" -o ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    echo "Downloading Kafka connectors..." && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-${KAFKA_VERSION}_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-${KAFKA_VERSION}_${SCALA_VERSION}-${SPARK_VERSION}.jar" -o ${SPARK_HOME}/jars/spark-sql-kafka-${KAFKA_VERSION}_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-${KAFKA_VERSION}_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-${KAFKA_VERSION}_${SCALA_VERSION}-${SPARK_VERSION}.jar" -o ${SPARK_HOME}/jars/spark-token-provider-kafka-${KAFKA_VERSION}_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" -o ${SPARK_HOME}/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" -o ${SPARK_HOME}/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar


##############################################
# STAGE 2 — Spark Runtime
##############################################
FROM python:3.13-bookworm AS spark

ARG TARGETARCH
RUN echo "Building for $TARGETARCH"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

COPY --from=base /opt/spark /opt/spark

RUN pip install --no-cache-dir pyspark==4.0.2 jupyter==1.1.0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

##############################################
# STAGE 3 — Airflow with Spark Integration
##############################################
FROM apache/airflow:3.1.8-python3.13 AS airflow

ARG TARGETARCH

RUN echo "Building for $TARGETARCH"

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
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

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==6.0.0 pyspark==4.0.2
