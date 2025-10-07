from datetime import datetime
from airflow import DAG
import os
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_NAME = os.environ["SPARK_CONN_NAME"]

DEFAULT_CONF = {
    "spark.executor.memory": "512m",
    # "spark.executor.instances": "2",
    "spark.driver.memory": "1G",
    "spark.driver.bindAddress": "0.0.0.0",
}


with DAG(
    dag_id="retail_model_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "conn_id": SPARK_CONN_NAME,
        "verbose": False,
        "deploy_mode": "client",
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --- Run Migration to create fact and dimension tables ---
    run_migrations = SparkSubmitOperator(
        task_id="run_migrations",
        name="run_migrations_v1",
        application="/opt/airflow/scripts/migrations/v1.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    dim_date = SparkSubmitOperator(
        task_id="dim_date",
        name="dim_date_job",
        application="/opt/airflow/scripts/dim_date.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    # --- Dimension tables ---
    dim_customers = SparkSubmitOperator(
        task_id="dim_customers",
        name="dim_customers_job",
        application="/opt/airflow/scripts/dim_customer.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )
    # start >> run_migrations >> [dim_customers] >> end

    dim_sellers = SparkSubmitOperator(
        task_id="dim_sellers",
        name="dim_seller_job",
        application="/opt/airflow/scripts/dim_seller.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    dim_products = SparkSubmitOperator(
        task_id="dim_products",
        name="dim_product_job",
        application="/opt/airflow/scripts/dim_product.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    stg_order_order_items = SparkSubmitOperator(
        task_id="stg_order_order_items",
        name="stg_order_order_item_job",
        application="/opt/airflow/scripts/stg_order_order_item.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.driver.memory": "1G",
        },
    )

    # --- Fact table ---
    fct_order_items = SparkSubmitOperator(
        task_id="fct_order_items",
        name="fct_order_item_job",
        application="/opt/airflow/scripts/fct_order_items.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.executor.instances": "2",
        },
    )

    fct_order_summary = SparkSubmitOperator(
        task_id="fct_order_summary",
        name="fct_order_summary_job",
        application="/opt/airflow/scripts/fct_order_summary.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.executor.instances": "2",
        },
    )

    # --- Set dependencies ---

    start >> [run_migrations, dim_date, stg_order_order_items]

    dim_date >> [dim_customers, dim_sellers, dim_products]

    [dim_sellers, dim_products, stg_order_order_items] >> fct_order_items

    [dim_customers, stg_order_order_items] >> fct_order_summary

    [fct_order_items, fct_order_summary] >> end
