from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import get_seller_schema
from utils import (
    get_or_create_spark_session,
    extract_raw_data,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("dim_seller.etl")


def transform_sellers_data(df):
    logger.info("Transforming sellers data for dimensional modelling")
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
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("seller_id").orderBy("updated_date")
            ),
        )
        .filter(F.col("row_num") == 1)
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
    logger.info("Transformed sellers data successfully")
    return df


def run_etl():
    logger.info("Starting ETL process for dim_seller")
    sc = get_or_create_spark_session()
    df = extract_raw_data(
        spark_context=sc, object_name="sellers.csv", schema=get_seller_schema()
    )
    dim_seller = transform_sellers_data(df)
    load_data_to_iceberg_table(df=dim_seller, table_name="dim_seller")
    logger.info("ETL process completed for dim_seller")


if __name__ == "__main__":
    run_etl()
