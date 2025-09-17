from pyspark.sql import functions as F
from utils import (
    get_or_create_spark_session,
    read_from_iceberg_table,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("fct_orders.etl")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import F.col, sum as spark_sum, min as spark_min, max as
# spark_max, count, monotonically_increasing_id


def transform_orders_data(stg_order_items, stg_orders, dim_customer, dim_date):
    logger.info("Transforming orders data for fact table")
    order_item_aggregate = stg_order_items.groupBy("order_id").agg(
        F.sum("item_value").alias("total_order_value"),
        F.sum("freight_value").alias("total_freight_value"),
        F.min("shipping_limit_date").alias("min_shipping_limit_date"),
        F.max("shipping_limit_date").alias("max_shipping_limit_date"),
        F.count("order_item_id").alias("item_count"),
    )

    fct_orders = (
        stg_orders.alias("so")
        .join(
            dim_customer.alias("dc"),
            F.col("so.customer_id") == F.col("dc.customer_id"),
            how="left",
        )
        .join(
            order_item_aggregate.alias("oib"),
            F.col("so.order_id") == F.col("oib.order_id"),
            how="left",
        )
        .join(
            dim_date.alias("odd"),
            F.col("so.order_date") == F.col("odd.date"),
            how="left",
        )
        .join(
            dim_date.alias("padd"),
            F.col("so.order_approved_date") == F.col("padd.date"),
            how="left",
        )
        .join(
            dim_date.alias("dtcrdd"),
            F.col("so.delivered_to_carrier_date") == F.col("dtcrdd.date"),
            how="left",
        )
        .join(
            dim_date.alias("dtcsdd"),
            F.col("so.delivered_to_customer_date") == F.col("dtcsdd.date"),
            how="left",
        )
        .join(
            dim_date.alias("edddd"),
            F.col("so.estimated_delivery_date") == F.col("edddd.date"),
            how="left",
        )
        .join(
            dim_date.alias("minsdd"),
            F.col("oib.min_shipping_limit_date") == F.col("minsdd.date"),
            how="left",
        )
        .join(
            dim_date.alias("maxsdd"),
            F.col("oib.max_shipping_limit_date") == F.col("maxsdd.date"),
            how="left",
        )
        .select(
            F.col("so.order_id"),
            F.col("so.order_status"),
            F.col("dc.customer_id"),
            F.col("oib.item_count"),
            F.col("oib.total_order_value"),
            F.col("oib.total_freight_value"),
            F.col("odd.date").alias("order_date_key"),
            F.col("minsdd.date").alias("min_shipping_limit_date_key"),
            F.col("maxsdd.date").alias("max_shipping_limit_date_key"),
            F.col("dtcrdd.date").alias("delivered_to_carrier_date_key"),
            F.col("dtcsdd.date").alias("delivered_to_customer_date_key"),
            F.col("edddd.date").alias("estimated_delivery_date_key"),
            F.col("padd.date").alias("order_approved_date_key"),
        )
    )
    logger.info("Transformed orders data successfully")
    return fct_orders


def run_etl():
    logger.info("Running ETL process for fct_orders")
    sc = get_or_create_spark_session()

    stg_order_items = read_from_iceberg_table(
        spark_context=sc, table_name="stg_order_items"
    )
    dim_customer = read_from_iceberg_table(spark_context=sc, table_name="dim_customer")
    dim_date = read_from_iceberg_table(spark_context=sc, table_name="dim_date")
    stg_orders = read_from_iceberg_table(spark_context=sc, table_name="stg_orders")

    fct_orders = transform_orders_data(
        stg_order_items, stg_orders, dim_customer, dim_date
    )

    load_data_to_iceberg_table(df=fct_orders, table_name="fct_orders")

    logger.info("Finished ETL process for fct_orders")


if __name__ == "__main__":
    run_etl()
