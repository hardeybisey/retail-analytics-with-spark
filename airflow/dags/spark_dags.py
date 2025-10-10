from datetime import datetime
from airflow import DAG
import os
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_NAME = os.environ["SPARK_CONN_NAME"]
DEPLOY_MODE = "client"


with DAG(
    dag_id="retail_model_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "conn_id": SPARK_CONN_NAME,
        "deploy_mode": DEPLOY_MODE,
        "verbose": False,
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_migrations = SparkSubmitOperator(
        task_id="run_migrations",
        name="run_migrations_v1",
        application="/opt/airflow/spark/migrations_v1.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    dim_customers = SparkSubmitOperator(
        task_id="dim_customers",
        name="dim_customers_job",
        application="/opt/airflow/spark/dim_customer.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    dim_sellers = SparkSubmitOperator(
        task_id="dim_sellers",
        name="dim_seller_job",
        application="/opt/airflow/spark/dim_seller.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    dim_products = SparkSubmitOperator(
        task_id="dim_products",
        name="dim_product_job",
        application="/opt/airflow/spark/dim_product.py",
        conf={
            "spark.driver.memory": "512m",
        },
    )

    stg_order_order_items = SparkSubmitOperator(
        task_id="stg_order_order_items",
        name="stg_order_order_item_job",
        application="/opt/airflow/spark/stg_order_order_item.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.driver.memory": "1G",
        },
    )

    fact_order_items = SparkSubmitOperator(
        task_id="fact_order_items",
        name="fact_order_item_job",
        application="/opt/airflow/spark/fact_order_items.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.executor.instances": "2",
        },
    )

    fact_order_summary = SparkSubmitOperator(
        task_id="fact_order_summary",
        name="fact_order_summary_job",
        application="/opt/airflow/spark/fact_order_summary.py",
        conf={
            "spark.executor.memory": "2G",
            "spark.executor.instances": "2",
        },
    )

    start >> run_migrations

    run_migrations >> [stg_order_order_items, dim_customers, dim_sellers, dim_products]

    [dim_sellers, dim_products, stg_order_order_items] >> fact_order_items >> end

    [dim_customers, stg_order_order_items] >> fact_order_summary >> end
