from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import get_orders_schema
from utils import (
    get_or_create_spark_session,
    # extract_raw_data,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
    read_csv,
)

logger = create_logger("stg_orders.etl")


def create_stg_orders_table(spark):
    df = read_csv(
        spark_context=spark, object_name="orders.csv", schema=get_orders_schema()
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
    load_data_to_iceberg_table(df, table_name="stg_orders", mode="overwrite")


def fct_order_summary_table(spark):
    dim_date = read_from_iceberg_table(spark, "dim_date")
    stg_orders = read_from_iceberg_table(spark, "stg_orders")
    stg_order_items = read_from_iceberg_table(spark, "stg_order_items")
    dim_customer = read_from_iceberg_table(spark, "dim_customer")

    order_item_aggregate = stg_order_items.groupBy("order_id").agg(
        F.sum("item_value").alias("total_order_value"),
        F.sum("freight_value").alias("total_freight_value"),
        F.min("shipping_limit_date").alias("min_shipping_limit_date"),
        F.max("shipping_limit_date").alias("max_shipping_limit_date"),
        F.count("order_item_id").alias("item_count"),
    )

    df = (
        stg_orders.alias("so")
        .join(order_item_aggregate.alias("soi"), on="order_id", how="left")
        .join(
            dim_date.alias("od"),
            on=F.col("so.order_date") == F.col("od.date"),
            how="left",
        )
        .join(
            dim_date.alias("dtcad"),
            on=F.col("so.delivered_to_carrier_date") == F.col("dtcad.date"),
            how="left",
        )
        .join(
            dim_date.alias("dtcud"),
            on=F.col("so.delivered_to_customer_date") == F.col("dtcud.date"),
            how="left",
        )
        .join(
            dim_date.alias("edd"),
            on=F.col("so.estimated_delivery_date") == F.col("edd.date"),
            how="left",
        )
        .join(
            dim_date.alias("mnsld"),
            on=F.col("soi.min_shipping_limit_date") == F.col("mnsld.date"),
            how="left",
        )
        .join(
            dim_date.alias("mxsld"),
            on=F.col("soi.max_shipping_limit_date") == F.col("mxsld.date"),
            how="left",
        )
        .join(
            dim_customer.alias("dc"),
            on=[
                F.col("so.customer_id") == F.col("dc.customer_id"),
                F.col("so.order_date") >= F.col("dc.effective_from"),
                (F.col("so.order_date") < F.col("dc.effective_to"))
                | (F.col("dc.is_current") == F.lit(True)),
            ],
            how="left",
        )
    )

    df = df.select(
        F.monotonically_increasing_id().alias("order_summary_sk"),
        F.col("dc.customer_sk").alias("customer_sk"),
        F.col("so.order_id").alias("order_id"),
        F.col("so.order_status").alias("order_status"),
        F.col("soi.total_order_value").alias("total_order_value"),
        F.col("soi.total_freight_value").alias("total_freight_value"),
        F.col("soi.item_count").alias("item_count"),
        F.col("od.date_key").alias("order_date_key"),
        F.col("dtcad.date_key").alias("delivered_to_carrier_date_key"),
        F.col("dtcud.date_key").alias("delivered_to_customer_date_key"),
        F.col("edd.date_key").alias("estimated_delivery_date_key"),
        F.col("od.date_key").alias("order_approved_date_key"),
        F.col("mnsld.date_key").alias("min_shipping_limit_date_key"),
        F.col("mxsld.date_key").alias("max_shipping_limit_date_key"),
    )

    load_data_to_iceberg_table(df, table_name="fct_order_summary", mode="overwrite")


def run_etl():
    logger.info("Starting ETL process for stg_orders")
    sc = get_or_create_spark_session()
    sc.sql(
        """
        CREATE TABLE IF NOT EXISTS fct_order_summary (
            order_summary_sk STRING NOT NULL,
            customer_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_status STRING NOT NULL,
            total_order_value DECIMAL(10, 2) NOT NULL,
            total_freight_value DECIMAL(10, 2) NOT NULL,
            item_count INT NOT NULL,
            order_date_key DATE NOT NULL,
            delivered_to_carrier_date_key DATE NOT NULL,
            delivered_to_customer_date_key DATE NOT NULL,
            estimated_delivery_date_key DATE NOT NULL,
            order_approved_date_key DATE NOT NULL,
            min_shipping_limit_date_key DATE NOT NULL,
            max_shipping_limit_date_key DATE NOT NULL
        ) USING ICEBERG
        """
    )
    create_stg_orders_table(sc)
    fct_order_summary_table(sc)
    logger.info("Finished ETL process for stg_orders")


if __name__ == "__main__":
    run_etl()
