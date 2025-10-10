##############################################
# STAGE 1 — Base Spark Build
##############################################
FROM alpine:3.20 AS base

RUN apk add --no-cache curl tar

ARG SPARK_VERSION=3.5.6
ARG SPARK_MAJOR_VERSION=3.5
ARG HADOOP_VERSION=3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.466
ARG ICEBERG_VERSION=1.9.0
ARG SCALA_VERSION=2.12

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

RUN mkdir -p ${SPARK_HOME}/jars

# --- Download Spark ---
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /spark.tgz
RUN tar -xzf /spark.tgz -C ${SPARK_HOME} --strip-components 1 && rm /spark.tgz

# --- Download Iceberg & AWS connectors ---
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/


##############################################
# STAGE 2 — Spark Runtime Image
##############################################
FROM python:3.11-bullseye AS spark

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      openjdk-17-jdk \
      iputils-ping \
      procps \
      vim && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

COPY --from=base /opt/spark /opt/spark

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN pip install --no-cache-dir pyspark==3.5.6 jupyter==1.1.0


ENTRYPOINT ["/entrypoint.sh"]

##############################################
# STAGE 3 — Airflow with Spark integration
##############################################
FROM apache/airflow:3.1.0-python3.11 AS airflow

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      openjdk-17-jdk \
      iputils-ping \
      procps \
      vim && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

COPY --from=base /opt/spark /opt/spark

RUN chown -R airflow ${SPARK_HOME}

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.3.2 pyspark==3.5.6
