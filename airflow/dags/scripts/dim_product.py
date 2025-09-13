import logging

from schema import get_products_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")


def create_product_dim_table():
    logger.info("Starting create_product_dim_table")
    _spark = create_spark_session(caller="dim_product")

    df = extract_raw_data(_spark, "csv-input", "products.csv", get_products_schema())
    df = (
        df.withColumnsRenamed(
            {
                "product_category_id": "category_id",
                "product_size_label": "size_label",
                "product_price": "price",
                "product_length_cm": "length_cm",
                "product_height_cm": "height_cm",
                "product_width_cm": "width_cm",
            }
        )
        .dropDuplicates(["product_id"])
        .select(
            "product_id",
            "product_name",
            "category_id",
            "price",
            "size_label",
            "length_cm",
            "height_cm",
            "width_cm",
        )
    )
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "dim_product")


if __name__ == "__main__":
    logger.info("Starting ETL process for dim_product")
    create_product_dim_table()
    logger.info("ETL process completed for dim_product")
