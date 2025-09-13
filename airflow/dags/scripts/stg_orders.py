import logging

from pyspark.sql.functions import col
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from schema import get_orders_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as spark_sum, min as spark_min, max as
# spark_max, count, monotonically_increasing_id


def create_stg_orders_table():
    logger.info("Starting create_stg_orders_table")
    _spark = create_spark_session(caller="stg_orders")

    df = extract_raw_data(_spark, "csv-input", "orders.csv", get_orders_schema())

    window_spec = Window.partitionBy("order_id").orderBy("order_approved_date")

    df = (
        df.withColumnsRenamed(
            {
                "order_purchase_date": "order_date",
                "order_approved_at": "order_approved_date",
                "order_delivered_carrier_date": "delivered_to_carrier_date",
                "order_delivered_customer_date": "delivered_to_customer_date",
                "order_estimated_delivery_date": "estimated_delivery_date",
            }
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_approved_date",
            "delivered_to_carrier_date",
            "delivered_to_customer_date",
            "estimated_delivery_date",
            "order_status",
        )
    )
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "stg_orders")


if __name__ == "__main__":
    logger.info("Starting ETL process for stg_orders")
    create_stg_orders_table()
    logger.info("ETL process completed for stg_orders")
