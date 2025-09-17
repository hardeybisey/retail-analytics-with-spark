from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import get_orders_schema
from utils import (
    get_or_create_spark_session,
    extract_raw_data,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("stg_orders.etl")


def transform_orders_data(df):
    logger.info("Transforming orders data for staging table")
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
    logger.info("Transformed orders data successfully")
    return df


def run_etl():
    logger.info("Starting ETL process for stg_orders")
    sc = get_or_create_spark_session()

    df = extract_raw_data(
        spark_context=sc, object_name="orders.csv", schema=get_orders_schema()
    )

    stg_orders = transform_orders_data(df)
    load_data_to_iceberg_table(df=stg_orders, table_name="stg_orders")
    logger.info("Finished ETL process for stg_orders")


if __name__ == "__main__":
    run_etl()
