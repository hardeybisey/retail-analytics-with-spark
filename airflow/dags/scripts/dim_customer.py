from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import get_customer_schema
from utils import (
    get_or_create_spark_session,
    extract_raw_data,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("dim_customer.etl")


def transform_customers_data(df):
    logger.info("Transforming customers data for dimensional modelling")
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
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy("updated_date")
            ),
        )
        .filter(F.col("row_num") == 1)
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
    logger.info("Transformed customers data successfully")
    return df


def run_etl():
    logger.info("Running ETL process for dim_customer")
    sc = get_or_create_spark_session()
    df = extract_raw_data(
        spark_context=sc, object_name="customers.csv", schema=get_customer_schema()
    )
    dim_customer = transform_customers_data(df)
    load_data_to_iceberg_table(df=dim_customer, table_name="dim_customer")
    logger.info("Finished ETL process for dim_customer")


if __name__ == "__main__":
    run_etl()
