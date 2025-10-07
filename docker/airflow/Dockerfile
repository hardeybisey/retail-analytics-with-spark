FROM apache/airflow:3.1.0-python3.11

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.3.2

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
# ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

ENV SPARK_VERSION=3.5.6
ARG SPARK_MAJOR_VERSION=3.5
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.466
ARG ICEBERG_VERSION=1.9.0
ARG SCALA_VERSION=2.12

# Creating Spark home dir..
RUN mkdir -p ${SPARK_HOME}/jars

# Downloading Spark...
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz /spark.tgz
RUN tar -xvzf /spark.tgz --directory ${SPARK_HOME} --strip-components 1 && rm /spark.tgz

# Iceberg Spark Runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/

# Iceberg AWS Bundle (Provides AWS SDK v2)
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar ${SPARK_HOME}/jars/

# Hadoop S3A Connector (Provides the S3AFileSystem class)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar ${SPARK_HOME}/jars/

# AWS SDK for Java v1 (Dependency for hadoop-aws.jar)
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${SPARK_HOME}/jars/

RUN chown -R airflow ${SPARK_HOME}

USER airflow
