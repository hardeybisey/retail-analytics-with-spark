from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    read_parquet,
    read_from_iceberg_table,
    create_logger,
    load_data_to_iceberg_table,
)

logger = create_logger("dim_seller")


def create_stg_seller_table(spark: SparkSession) -> None:
    """Create the staging table for seller data

    This function creates a `stg_seller` table by
    reading from a Parquet file, renaming columns and deduplicating the data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    df = read_parquet(spark_context=spark, object_name="sellers.parquet")
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
    )
    load_data_to_iceberg_table(df, table_name="stg_seller", mode="overwrite")


def create_seller_scd2(spark: SparkSession) -> None:
    """Create SCD2 records for seller dimension table

    This function creates a `tmp_dim_seller` table with Slowly Changing Dimension Type 2
    (SCD2) records for the seller dimension table with changes from `stg_seller` table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """

    stg_seller_table = read_from_iceberg_table(spark, "stg_seller")
    dim_seller_table = read_from_iceberg_table(spark, "dim_seller")

    active_records = stg_seller_table.alias("ss").join(
        dim_seller_table.alias("dc"),
        on=[
            F.col("ss.seller_id") == F.col("dc.seller_id"),
            F.col("dc.is_current") == F.lit(True),
        ],
        how="fullouter",
    )

    common_filter = (
        (F.col("dc.address") != F.col("ss.address"))
        | (F.col("dc.state") != F.col("ss.state"))
        | (F.col("dc.zip_code_prefix") != F.col("ss.zip_code_prefix"))
    )

    new_records = (
        active_records.filter(F.col("dc.address").isNull() | common_filter)
        .select(
            F.col("ss.seller_id"),
            F.col("ss.address"),
            F.col("ss.state"),
            F.col("ss.zip_code_prefix"),
            F.col("ss.created_date"),
        )
        .withColumns(
            {
                "effective_from": F.col("created_date"),
                "effective_to": F.lit(None),
                "is_current": F.lit(True),
            }
        )
        .withColumn(
            "seller_sk",
            F.sha2(F.concat_ws("||", F.col("seller_id"), F.col("effective_from")), 256),
        )
        .drop("created_date")
    )

    expired_records = (
        active_records.filter(F.col("dc.address").isNotNull() & common_filter)
        .select(
            F.col("dc.seller_sk"),
            F.col("dc.seller_id"),
            F.col("dc.address"),
            F.col("dc.state"),
            F.col("dc.zip_code_prefix"),
            F.col("dc.effective_from"),
        )
        .withColumns(
            {
                "effective_to": F.col("effective_from") - F.expr("INTERVAL 1 DAY"),
                "is_current": F.lit(False),
            }
        )
    )

    df = new_records.unionByName(expired_records)

    load_data_to_iceberg_table(df, table_name="tmp_dim_seller", mode="overwrite")


def create_seller_dim_table(spark):
    """Merge SCD2 records into the seller dimension table

    This function merges the SCD2 records from the `tmp_dim_seller` staging
    table into `dim_seller` table.


    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    spark.sql("""
        MERGE INTO dim_seller AS target
        USING tmp_dim_seller AS src
            ON target.seller_sk = src.seller_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl():
    logger.info("Running ETL process for stg_seller")
    sc = get_or_create_spark_session()
    create_stg_seller_table(sc)
    create_seller_scd2(sc)
    create_seller_dim_table(sc)
    logger.info("Finished ETL process for stg_seller")


if __name__ == "__main__":
    run_etl()
