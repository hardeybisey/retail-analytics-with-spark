import logging

from pyspark.sql.functions import col
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from schema import get_seller_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")


def create_seller_dim_table():
    logger.info("Starting create_seller_dim_table")
    _spark = create_spark_session(caller="dim_seller")

    df = extract_raw_data(_spark, "csv-input", "sellers.csv", get_seller_schema())

    window_spec = Window.partitionBy("seller_id").orderBy("updated_date")

    df = (
        df.withColumnsRenamed(
            {
                "seller_address": "address",
                "seller_state": "state",
                "seller_zip_code": "zip_code_prefix",
                "seller_created_date": "created_date",
                "seller_updated_date": "updated_date",
            }
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .select(
            "seller_id",
            "address",
            "state",
            "zip_code_prefix",
            "created_date",
            "updated_date",
        )
        .drop("row_num")
    )
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "dim_seller")


if __name__ == "__main__":
    logger.info("Starting ETL process for dim_seller")
    create_seller_dim_table()
    logger.info("ETL process completed for dim_seller")
