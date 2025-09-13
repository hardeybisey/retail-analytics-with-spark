import logging

from pyspark.sql.functions import col
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from schema import get_customer_schema
from utils import create_spark_session
from utils import extract_raw_data
from utils import load_data_to_iceberg_table

logger = logging.getLogger("airflow.task")


def create_customer_dim_table():
    logger.info("Starting create_customer_dim_table")
    _spark = create_spark_session(caller="dim_customer")

    df = extract_raw_data(_spark, "csv-input", "customers.csv", get_customer_schema())

    window_spec = Window.partitionBy("customer_id").orderBy("updated_date")

    df = (
        df.withColumnsRenamed(
            {
                "customer_address": "address",
                "customer_state": "state",
                "customer_zip_code": "zip_code_prefix",
                "customer_created_date": "created_date",
                "customer_updated_date": "updated_date",
            }
        )
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .select(
            "customer_id",
            "address",
            "state",
            "zip_code_prefix",
            "created_date",
            "updated_date",
        )
        .drop("row_num")
    )
    logger.info("Finished processing DataFrame, now loading to Iceberg table")

    load_data_to_iceberg_table(df, "dim_customer")


if __name__ == "__main__":
    logger.info("Starting ETL process for dim_customer")
    create_customer_dim_table()
    logger.info("ETL process completed for dim_customer")
