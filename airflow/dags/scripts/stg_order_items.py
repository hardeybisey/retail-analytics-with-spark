import logging

from schema import get_order_items_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")


def create_stg_order_items_table():
    logger.info("Starting create_stg_order_items_table")
    _spark = create_spark_session(caller="stg_order_items")

    df = extract_raw_data(
        _spark, "csv-input", "order_items.csv", get_order_items_schema()
    )
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
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "stg_order_items")


if __name__ == "__main__":
    logger.info("Starting ETL process for stg_order_items")
    create_stg_order_items_table()
    logger.info("ETL process completed for stg_order_items")
