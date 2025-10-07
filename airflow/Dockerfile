FROM apache/airflow:3.1.0-python3.11

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.3.2 pyspark==3.5.6

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/opt/spark"
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN mkdir -p ${SPARK_HOME}/jars

ARG SPARK_VERSION=3.5.6

ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /spark.tgz
RUN tar -xvzf /spark.tgz --directory ${SPARK_HOME} --strip-components 1 && rm /spark.tgz

RUN chown -R airflow ${SPARK_HOME}

USER airflow
