from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
    read_parquet,
)

logger = create_logger("fact_order_items")


def create_stg_order_item_table(spark: SparkSession) -> None:
    """Create the staging table for order items data

    This function creates a `stg_order_items` table by
    reading from a Parquet file, renaming columns and deduplicating the data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    df = read_parquet(
        spark_context=spark,
        object_name="order_items.parquet",
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
    load_data_to_iceberg_table(df, table_name="stg_order_items", mode="overwrite")


def fct_order_items_table(spark: SparkSession) -> None:
    """Create the fact table for order items

    This function creates a `fct_order_items` table by
    combining data from `stg_order_items`, `stg_orders`, `dim_product`, `dim_seller`, and `dim_date`.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    dim_date = read_from_iceberg_table(spark, "dim_date")
    stg_orders = read_from_iceberg_table(spark, "stg_orders")
    stg_order_items = read_from_iceberg_table(spark, "stg_order_items")
    dim_product = read_from_iceberg_table(spark, "dim_product")
    dim_seller = read_from_iceberg_table(spark, "dim_seller")

    df = (
        stg_order_items.alias("soi")
        .join(stg_orders.alias("so"), on="order_id", how="left")
        .join(
            dim_date.alias("od"),
            on=F.col("so.order_date") == F.col("od.date"),
            how="left",
        )
        .join(
            dim_date.alias("sld"),
            on=F.col("soi.shipping_limit_date") == F.col("sld.date"),
            how="left",
        )
        .join(
            dim_seller.alias("ds"),
            on=[
                F.col("soi.seller_id") == F.col("ds.seller_id"),
                F.col("so.order_date") >= F.col("ds.effective_from"),
                (F.col("so.order_date") < F.col("ds.effective_to"))
                | (F.col("ds.is_current") == F.lit(True)),
            ],
            how="left",
        )
        .join(
            dim_product.alias("dp"),
            on=[
                F.col("soi.product_id") == F.col("dp.product_id"),
                F.col("so.order_date") >= F.col("dp.effective_from"),
                (F.col("so.order_date") < F.col("dp.effective_to"))
                | (F.col("dp.is_current") == F.lit(True)),
            ],
            how="left",
        )
    )

    df = df.select(
        F.monotonically_increasing_id().alias("order_item_sk"),
        F.col("ds.seller_sk").alias("seller_sk"),
        F.col("dp.product_sk").alias("product_sk"),
        F.col("so.order_id").alias("order_id"),
        F.col("soi.order_item_id").alias("order_item_id"),
        F.col("soi.item_value").alias("item_value"),
        F.col("soi.freight_value").alias("freight_value"),
        F.col("od.date_key").alias("order_date_key"),
        F.col("sld.date_key").alias("shipping_limit_date_key"),
    )

    load_data_to_iceberg_table(df, table_name="fct_order_items", mode="overwrite")


def run_etl():
    logger.info("Starting ETL process for fact_order_items")
    sc = get_or_create_spark_session()
    create_stg_order_item_table(sc)
    fct_order_items_table(sc)
    logger.info("Finished ETL process for fact_order_items")


if __name__ == "__main__":
    run_etl()
