#!/bin/bash
set -e

SPARK_MASTER_URL="spark://spark-master:7077"

if [ "$1" = "master" ]; then
  echo "Starting Spark master"
  # start-master.sh -p 7077
  start-history-server.sh
  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master -p 7077
elif [ "$1" = "worker" ]; then
  echo "Starting Spark worker..."
  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
# elif [ "$1" = "driver" ]; then
#   echo "Starting driver server..."
else
  echo "Running custom command: $@"
  exec "$@"
fi
