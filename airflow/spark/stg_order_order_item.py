from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    write_parquet,
    create_logger,
    read_parquet,
)
import os

S3_INPUTS_BUCKET = os.environ["S3_INPUTS_BUCKET"]
S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]


logger = create_logger("stg_order_order_item.etl")


def create_stg_order_item_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw order_items data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """

    df = read_parquet(
        spark_context=spark, file_name="order_items.parquet", s3_bucket=S3_INPUTS_BUCKET
    )

    df = (
        df.dropDuplicates(["order_id", "order_item_id"])
        .withColumnsRenamed(
            {
                "price": "item_value",
            }
        )
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "item_value",
            "freight_value",
            "shipping_limit_date",
        )
    )
    write_parquet(
        data_frame=df, file_name="stg_order_items.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_stg_orders_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw order data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    df = read_parquet(
        spark_context=spark,
        file_name="orders.parquet",
        s3_bucket=S3_INPUTS_BUCKET,
    )
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
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("order_id").orderBy("order_approved_date")
            ),
        )
        .filter(F.col("row_num") == 1)
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
    write_parquet(
        data_frame=df, file_name="stg_orders.parquet", s3_bucket=S3_STG_BUCKET
    )


def run_etl():
    """Run the full ETL pipeline for stg_order and stg_order_item."""
    logger.info("Starting ETL process for stg_order and stg_order_item")
    sc = get_or_create_spark_session()
    create_stg_orders_table(sc)
    create_stg_order_item_table(sc)
    logger.info("Finished ETL process for stg_order and stg_order_item")


if __name__ == "__main__":
    run_etl()
