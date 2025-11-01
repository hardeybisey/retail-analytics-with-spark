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


logger = create_logger("stg_order_items.etl")


def create_stg_order_item_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw order_items data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    df_order_items = read_parquet(
        spark_session=spark, file_name="order_items.parquet", s3_bucket=S3_INPUTS_BUCKET
    )

    df_order = read_parquet(
        spark_session=spark, file_name="orders.parquet", s3_bucket=S3_INPUTS_BUCKET
    )

    df = df_order_items.join(
        df_order.select("order_id", "order_purchase_date"),
        on="order_id",
        how="inner",
    )

    df = (
        df.withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy(["order_id", "order_item_id"]).orderBy(
                    "order_purchase_date"
                )
            ),
        )
        .filter(F.col("row_num") == 1)
        .withColumnsRenamed(
            {
                "price": "item_value",
                "order_purchase_date": "order_date",
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
            "order_date",
        )
    )
    write_parquet(
        data_frame=df, file_name="stg_order_items.parquet", s3_bucket=S3_STG_BUCKET
    )


def run_etl():
    """Run the full ETL pipeline for stg_order_items."""
    logger.info("Starting ETL process for stg_order_items")
    sc = get_or_create_spark_session()
    create_stg_order_item_table(sc)
    logger.info("Finished ETL process for stg_order_items")


if __name__ == "__main__":
    run_etl()
