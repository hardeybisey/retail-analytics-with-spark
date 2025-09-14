# Project Overview

Building Extract, Transform, and Load (ETL) workloads is a common pattern in Apache Airflow. This template shows an example pattern for defining an ETL workload using DuckDB as the data warehouse of choice.

Astronomer is the best place to host Apache Airflow -- try it out with a free trial at [astronomer.io](https://www.astronomer.io/).

# Learning Paths

To learn more about data engineering with Apache Airflow, make a few changes to this project! For example, try one of the following:

1. Changing the data warehouse to a different provider (Snowflake, AWS Redshift, etc.)
2. Adding a different data source and integrating it with this ETL project
3. Adding the [EmailOperator](https://registry.astronomer.io/providers/apache-airflow/versions/2.8.1/modules/EmailOperator) to the project and notifying a user on job completion

# Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes two example DAGs:
  - `example_etl_galaxies`: This example demonstrates an ETL pipeline using Airflow. The pipeline extracts data about galaxies, filters the data based on the distance from the Milky Way, and loads the filtered data into a DuckDB database.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploying to Production

### ❗Warning❗

This template used DuckDB, an in-memory database, for running dbt transformations. While this is great to learn Airflow, your data is not guaranteed to persist between executions! For production applications, use a _persistent database_ instead (consider DuckDB's hosted option MotherDuck or another database like Postgres, MySQL, or Snowflake).

## Commands
astro dev object import


spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/dim_customer.py

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.466,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0 \
  --executor-cores 1 \
  --num-executors 1 \
  --executor-memory 500m \
  --conf spark.executor.instances=1 \
  --conf  spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.catalog.demo.warehouse=s3a://warehouse/ \
  --conf spark.sql.catalog.demo.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.defaultCatalog=demo \
  --conf spark.sql.catalog.demo.default-namespace=retail_analytics \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --name dim_customers_job \
  --conf spark.driver.host=etl_b8fc85-scheduler-1 \
  /usr/local/airflow/dags/scripts/dim_customer.py


spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/dim_customer.py
spark-submit --master spark://spark-master:7077 --executor-memory 2G --total-executor-cores 2 --executor-cores 2 --deploy-mode client /home/scripts/dim_seller.py
spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/dim_product.py
spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/dim_product_category.py
spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/stg_orders.py
spark-submit --master spark://spark-master:7077 --deploy-mode client /home/scripts/stg_order_items.py
