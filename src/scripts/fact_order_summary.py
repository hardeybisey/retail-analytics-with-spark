from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
    read_parquet,
)
import os

S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]


logger = create_logger("fact_order_items")


def fct_order_summary_table(spark: SparkSession) -> None:
    """Create the order summary fact table by joining `stg_order_items`, `stg_orders`,
    `dim_customer`, and `dim_date` tables.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_orders = read_parquet(
        spark_context=spark,
        file_name="stg_orders.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    stg_order_items = read_parquet(
        spark_context=spark,
        file_name="stg_order_items.parquet",
        s3_bucket=S3_STG_BUCKET,
    )

    dim_date = read_from_iceberg_table(spark_context=spark, table_name="dim_date")
    dim_customer = read_from_iceberg_table(
        spark_context=spark, table_name="dim_customer"
    )
    order_item_aggregate = stg_order_items.groupBy("order_id").agg(
        F.sum("item_value").alias("total_order_value"),
        F.sum("freight_value").alias("total_freight_value"),
        F.min("shipping_limit_date").alias("min_shipping_limit_date"),
        F.max("shipping_limit_date").alias("max_shipping_limit_date"),
        F.count("order_item_id").alias("item_count"),
    )

    df = (
        stg_orders.alias("orders")
        .join(order_item_aggregate.alias("order_item_agg"), on="order_id", how="left")
        .join(
            dim_date.alias("order_date"),
            on=F.col("orders.order_date") == F.col("order_date.date"),
            how="left",
        )
        .join(
            dim_date.alias("approved_date"),
            on=F.col("orders.order_approved_date") == F.col("approved_date.date"),
            how="left",
        )
        .join(
            dim_date.alias("carrier_ddate"),
            on=F.col("orders.delivered_to_carrier_date") == F.col("carrier_ddate.date"),
            how="left",
        )
        .join(
            dim_date.alias("customer_ddate"),
            on=F.col("orders.delivered_to_customer_date")
            == F.col("customer_ddate.date"),
            how="left",
        )
        .join(
            dim_date.alias("estimated_ddate"),
            on=F.col("orders.estimated_delivery_date") == F.col("estimated_ddate.date"),
            how="left",
        )
        .join(
            dim_date.alias("min_shipping_ldate"),
            on=F.col("order_item_agg.min_shipping_limit_date")
            == F.col("min_shipping_ldate.date"),
            how="left",
        )
        .join(
            dim_date.alias("max_shipping_ldate"),
            on=F.col("order_item_agg.max_shipping_limit_date")
            == F.col("max_shipping_ldate.date"),
            how="left",
        )
        .join(
            dim_customer.alias("customer"),
            on=[
                F.col("orders.customer_id") == F.col("customer.customer_id"),
                F.col("orders.order_date") >= F.col("customer.effective_from"),
                (F.col("orders.order_date") < F.col("customer.effective_to"))
                | (F.col("customer.is_current") == F.lit(True)),
            ],
            how="left",
        )
    )

    df = df.select(
        F.monotonically_increasing_id().alias("order_summary_sk"),
        F.col("customer.customer_sk").alias("customer_sk"),
        F.col("orders.order_id").alias("order_id"),
        F.col("orders.order_status").alias("order_status"),
        F.col("order_item_agg.total_order_value").alias("total_order_value"),
        F.col("order_item_agg.total_freight_value").alias("total_freight_value"),
        F.col("order_item_agg.item_count").alias("item_count"),
        F.col("order_date.date_key").alias("order_date_key"),
        F.col("approved_date.date_key").alias("order_approved_date_key"),
        F.col("carrier_ddate.date_key").alias("delivered_to_carrier_date_key"),
        F.col("customer_ddate.date_key").alias("delivered_to_customer_date_key"),
        F.col("estimated_ddate.date_key").alias("estimated_delivery_date_key"),
        F.col("min_shipping_ldate.date_key").alias("min_shipping_limit_date_key"),
        F.col("max_shipping_ldate.date_key").alias("max_shipping_limit_date_key"),
    )

    load_data_to_iceberg_table(df, table_name="fct_order_summary", mode="overwrite")


def run_etl() -> None:
    """Run the full ETL pipeline for `fact_order_summary` table."""
    logger.info("Starting ETL process for fact_order_summary")
    sc = get_or_create_spark_session()
    fct_order_summary_table(sc)
    logger.info("Finished ETL process for fact_order_summary")


if __name__ == "__main__":
    run_etl()
