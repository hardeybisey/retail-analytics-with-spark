from schema import get_order_items_schema
from utils import (
    get_or_create_spark_session,
    extract_raw_data,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("stg_order_items.etl")


def transform_order_items_data(df):
    logger.info("Transforming order items data for staging table")
    df = (
        df.withColumnsRenamed(
            {
                "price": "item_value",
            }
        )
        .dropDuplicates(["order_id", "order_item_id"])
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
    logger.info("Transformed order items data successfully")
    return df


def run_etl():
    logger.info("Starting ETL process for stg_order_items")
    sc = get_or_create_spark_session()

    df = extract_raw_data(
        spark_context=sc,
        object_name="order_items.csv",
        schema=get_order_items_schema(),
    )
    stg_order_items = transform_order_items_data(df)

    load_data_to_iceberg_table(df=stg_order_items, table_name="stg_order_items")
    logger.info("Finished ETL process for stg_order_items")


if __name__ == "__main__":
    run_etl()
