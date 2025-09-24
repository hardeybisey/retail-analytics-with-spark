from pyspark.sql import functions as F
from schema import get_order_items_schema
from utils import (
    get_or_create_spark_session,
    # extract_raw_data,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
    read_csv,
)

logger = create_logger("stg_orders.etl")


def create_stg_order_item_table(spark):
    df = read_csv(
        spark_context=spark,
        object_name="order_items.csv",
        schema=get_order_items_schema(),
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


def fct_order_items_table(spark):
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
    logger.info("Starting ETL process for stg_order_items")
    sc = get_or_create_spark_session()
    sc.sql(
        """
        CREATE TABLE IF NOT EXISTS fct_order_items (
            order_item_sk STRING NOT NULL,
            seller_sk STRING NOT NULL,
            product_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_item_id STRING NOT NULL,
            item_value DECIMAL(10, 2) NOT NULL,
            freight_value DECIMAL(10, 2) NOT NULL,
            order_date_key DATE NOT NULL,
            shipping_limit_date_key DATE NOT NULL
        ) USING ICEBERG
        """
    )
    create_stg_order_item_table(sc)
    fct_order_items_table(sc)
    logger.info("Finished ETL process for stg_order_items")


if __name__ == "__main__":
    run_etl()
