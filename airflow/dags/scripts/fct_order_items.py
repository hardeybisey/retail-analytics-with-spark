from pyspark.sql import functions as F
from utils import (
    get_or_create_spark_session,
    read_from_iceberg_table,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("fct_order_items.etl")


def transform_order_items_data(
    stg_order_items, stg_orders, dim_product, dim_seller, dim_date
):
    logger.info("Transforming order items data for fact table")
    fct_order_items = (
        stg_order_items.join(stg_orders, on="order_id", how="left")
        .join(dim_product, on="product_id", how="left")
        .join(dim_seller, on="seller_id", how="left")
        .join(
            dim_date.alias("od"),
            stg_orders.order_date == F.col("od.date"),
            how="left",
        )
        .join(
            dim_date.alias("edd"),
            stg_order_items.shipping_limit_date == F.col("edd.date"),
            how="left",
        )
        .select(
            stg_order_items.order_id,
            stg_order_items.order_item_id,
            stg_order_items.product_id,
            stg_order_items.seller_id,
            stg_order_items.item_value,
            stg_order_items.freight_value,
            F.col("od.date").alias("order_date"),
            F.col("edd.date").alias("shipping_limit_date"),
        )
    )
    logger.info("Transformed order items data successfully")
    return fct_order_items


def run_etl():
    logger.info("Starting ETL process for fct_order_items")
    sc = get_or_create_spark_session()

    stg_order_items = read_from_iceberg_table(
        spark_context=sc, table_name="stg_order_items"
    )
    dim_date = read_from_iceberg_table(spark_context=sc, table_name="dim_date")
    dim_product = read_from_iceberg_table(spark_context=sc, table_name="dim_product")
    dim_seller = read_from_iceberg_table(spark_context=sc, table_name="dim_seller")
    stg_orders = read_from_iceberg_table(spark_context=sc, table_name="stg_orders")

    fct_order_items = transform_order_items_data(
        stg_order_items, stg_orders, dim_product, dim_seller, dim_date
    )

    load_data_to_iceberg_table(df=fct_order_items, table_name="fct_order_items")

    logger.info("ETL process completed for fct_order_items")


if __name__ == "__main__":
    run_etl()
