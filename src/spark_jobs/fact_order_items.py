from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    load_to_iceberg,
    read_from_iceberg,
    create_logger,
    null_safe_eq,
    read_parquet,
)
import os

S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]


logger = create_logger("fact_order_items")


def fct_order_items_table(spark: SparkSession) -> None:
    """Create the order items fact table by joining `stg_orders`, `stg_order_items`,
    `dim_date`, `dim_product`, and `dim_seller` tables.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_order_items = read_parquet(
        spark_session=spark,
        file_name="stg_order_items.parquet",
        s3_bucket=S3_STG_BUCKET,
    )

    dim_date = read_from_iceberg(spark_session=spark, table_name="dim_date")
    dim_product = read_from_iceberg(spark_session=spark, table_name="dim_product")
    dim_seller = read_from_iceberg(spark_session=spark, table_name="dim_seller")

    df = (
        stg_order_items.alias("order_items")
        .join(
            dim_date.alias("order_date"),
            on=null_safe_eq(F.col("order_items.order_date"), F.col("order_date.date")),
            how="left",
        )
        .join(
            dim_date.alias("shipping_date"),
            on=null_safe_eq(
                F.col("order_items.shipping_limit_date"), F.col("shipping_date.date")
            ),
            how="left",
        )
        .join(
            dim_seller.alias("sellers"),
            on=[
                F.col("order_items.seller_id") == F.col("sellers.seller_id"),
                F.col("order_items.order_date") >= F.col("sellers.effective_from"),
                (F.col("order_items.order_date") < F.col("sellers.effective_to"))
                | (F.col("sellers.is_current") == F.lit(True)),
            ],
            how="left",
        )
        .join(
            dim_product.alias("products"),
            on=[
                F.col("order_items.product_id") == F.col("products.product_id"),
                F.col("order_items.order_date") >= F.col("products.effective_from"),
                (F.col("order_items.order_date") < F.col("products.effective_to"))
                | (F.col("products.is_current") == F.lit(True)),
            ],
            how="left",
        )
    )

    df = df.select(
        F.monotonically_increasing_id().alias("order_item_sk"),
        F.col("sellers.seller_sk").alias("seller_sk"),
        F.col("products.product_sk").alias("product_sk"),
        F.col("order_items.order_id").alias("order_id"),
        F.col("order_items.order_item_id").alias("order_item_id"),
        F.col("order_items.item_value").alias("item_value"),
        F.col("order_items.freight_value").alias("freight_value"),
        F.col("order_date.date_key").alias("order_date_key"),
        F.col("shipping_date.date_key").alias("shipping_limit_date_key"),
    )

    load_to_iceberg(data_frame=df, table_name="fact_order_items", mode="append")


def run_etl() -> None:
    """Run the full ETL pipeline for `fact_order_items` table."""
    logger.info("Starting ETL process for fact_order_items")
    sc = get_or_create_spark_session()
    fct_order_items_table(sc)
    logger.info("Finished ETL process for fact_order_items")


if __name__ == "__main__":
    run_etl()
