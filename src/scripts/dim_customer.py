from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    load_data_to_iceberg_table,
    read_parquet,
    create_logger,
    read_from_iceberg_table,
)

logger = create_logger("dim_customer")


def create_stg_customer_table(spark: SparkSession) -> None:
    """Create the staging table for customer data

    This function creates a `stg_customer` table by
    reading from a Parquet file, renaming columns and deduplicating the data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """

    df = read_parquet(spark_context=spark, object_name="customers.parquet")
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
    )
    load_data_to_iceberg_table(df, table_name="stg_customer", mode="overwrite")


def create_customer_scd2(spark: SparkSession) -> None:
    """Create SCD2 records for customer dimension table

    This function creates a `tmp_dim_customer` table with Slowly Changing Dimension Type 2
    (SCD2) records for the customer dimension table with changes from `stg_customer` table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_customer_table = read_from_iceberg_table(spark, "stg_customer")
    dim_customer_table = read_from_iceberg_table(spark, "dim_customer")

    active_records = stg_customer_table.alias("sc").join(
        dim_customer_table.alias("dc"),
        on=[
            F.col("sc.customer_id") == F.col("dc.customer_id"),
            F.col("dc.is_current") == F.lit(True),
        ],
        how="fullouter",
    )

    common_filter = (
        (F.col("dc.address") != F.col("sc.address"))
        | (F.col("dc.state") != F.col("sc.state"))
        | (F.col("dc.zip_code_prefix") != F.col("sc.zip_code_prefix"))
    )

    new_records = (
        active_records.filter(F.col("dc.address").isNull() | common_filter)
        .select(
            F.col("sc.customer_id"),
            F.col("sc.address"),
            F.col("sc.state"),
            F.col("sc.zip_code_prefix"),
            F.col("sc.created_date"),
        )
        .withColumns(
            {
                "effective_from": F.col("created_date"),
                "effective_to": F.lit(None),
                "is_current": F.lit(True),
            }
        )
        .withColumn(
            "customer_sk",
            F.sha2(
                F.concat_ws("||", F.col("customer_id"), F.col("effective_from")), 256
            ),
        )
        .drop("created_date")
    )

    expired_records = (
        active_records.filter(F.col("dc.address").isNotNull() & common_filter)
        .select(
            F.col("dc.customer_sk"),
            F.col("dc.customer_id"),
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

    load_data_to_iceberg_table(df, table_name="tmp_dim_customer", mode="overwrite")


def create_customer_dim_table(spark: SparkSession) -> None:
    """Merge SCD2 records into the customer dimension table

    This function merges the SCD2 records from the `tmp_dim_customer` staging
    table into `dim_customer` table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    spark.sql("""
        MERGE INTO dim_customer AS target
        USING tmp_dim_customer AS src
            ON target.customer_sk = src.customer_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl():
    logger.info("Running ETL process for stg_customer")
    sc = get_or_create_spark_session()
    create_stg_customer_table(sc)
    create_customer_scd2(sc)
    create_customer_dim_table(sc)
    logger.info("Finished ETL process for stg_customer")


if __name__ == "__main__":
    run_etl()
