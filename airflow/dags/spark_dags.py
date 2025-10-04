from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# SPARK_CONN_NAME = os.environ["SPARK_CONN_NAME"]
SPARK_CONN_NAME = "spark_conn"

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
        application="/opt/airflow/scripts/dim_customer.py",
        conn_id=SPARK_CONN_NAME,
        verbose=False,
        deploy_mode="client",
        name="dim_customers_job",
        conf={
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.memory": "1G",
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
