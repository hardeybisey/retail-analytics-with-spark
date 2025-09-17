#!/bin/bash
set -e

if [ "$1" = "master" ]; then
  echo "Starting Spark master"
  start-history-server.sh
  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master -p 7077
elif [ "$1" = "worker" ]; then
  echo "Starting Spark worker..."
  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
elif [ "$1" = "notebook" ]; then
  echo "Starting notebook server..."
  exec pyspark
else
  echo "Running custom command: $@"
  exec "$@"
fi
