import logging

from schema import get_product_category_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")


def create_product_category_dim_table():
    logger.info("Starting create_product_category_dim_table")
    _spark = create_spark_session(caller="dim_product_category")

    df = extract_raw_data(
        _spark, "csv-input", "product_category.csv", get_product_category_schema()
    )

    df = (
        df.withColumnsRenamed(
            {
                "product_category_id": "category_id",
                "product_category": "category_name",
                "product_sub_category": "sub_category",
            }
        )
        .dropDuplicates(["category_id"])
        .select(
            "category_id",
            "category_name",
            "sub_category",
        )
    )
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "dim_product_category")


if __name__ == "__main__":
    logger.info("Starting ETL process for dim_product_category")
    create_product_category_dim_table()
    logger.info("ETL process completed for dim_product_category")
