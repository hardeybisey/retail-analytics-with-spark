from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="retail_model_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --- Dimension tables ---
    dim_customers = SparkSubmitOperator(
        task_id="dim_customers",
        application="/usr/local/airflow/dags/scripts/dim_customer.py",
        conn_id="spark_cluster",
        # verbose=True,
        deploy_mode="client",
        # driver_memory="512m",
        # executor_memory="512m",
        # executor_cores=1,
        # num_executors=2,
        name="dim_customers_job",
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.466"
        ),
        conf={
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.memory": "512m",
            "spark.driver.host": "etl_b8fc85-scheduler-1",
        },
    )
    start >> dim_customers >> end

    # dim_sellers = SparkSubmitOperator(
    #     task_id="dim_sellers",
    #     application="/usr/local/airflow/dags/scripts/dim_seller.py",
    #     conn_id="spark_cluster",
    # )

    # dim_products = SparkSubmitOperator(
    #     task_id="dim_products",
    #     application="/usr/local/airflow/dags/scripts/dim_product.py",
    #     conn_id="spark_cluster",
    # )

    # dim_product_category = SparkSubmitOperator(
    #     task_id="dim_product_category",
    #     application="/usr/local/airflow/dags/scripts/dim_product_category.py",
    #     conn_id="spark_cluster",
    # )

    # stg_orders = SparkSubmitOperator(
    #     task_id="stg_orders",
    #     application="/usr/local/airflow/dags/scripts/stg_orders.py",
    #     conn_id="spark_cluster",
    # )

    # stg_order_items = SparkSubmitOperator(
    #     task_id="stg_order_items",
    #     application="/usr/local/airflow/dags/scripts/stg_order_items.py",
    #     conn_id="spark_cluster",
    # )

    # # --- Set dependencies ---
    # (
    #     start
    #     >> [
    #         dim_customers,
    #         dim_products,
    #         dim_sellers,
    #         dim_product_category,
    #         stg_orders,
    #         stg_order_items,
    #     ]
    #     >> end
    # )

#     # --- Fact table ---
#     fact_orders = SparkSubmitOperator(
#         task_id="fact_orders",
#         application="/usr/local/airflow/dags/scripts/fact_orders.py",
#         conn_id="spark_cluster",
#     )

#     # --- Set dependencies ---
#     [dim_customers, dim_products, dim_sellers] >> fact_orders
