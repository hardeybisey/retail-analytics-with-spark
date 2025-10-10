from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    read_parquet,
    write_parquet,
    create_logger,
    read_from_iceberg_table,
    get_last_updated_date,
)
import os

S3_INPUTS_BUCKET = os.environ["S3_INPUTS_BUCKET"]
S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]


logger = create_logger("dim_customer")


def create_stg_customer_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw customer data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    last_updated_date = get_last_updated_date(
        spark_context=spark, table_name="dim_customer"
    )
    df = read_parquet(
        spark_context=spark, file_name="customers.parquet", s3_bucket=S3_INPUTS_BUCKET
    )
    df = (
        df.withColumn(
            "customer_updated_date",
            F.coalesce(
                F.to_date(F.col("customer_updated_date").cast("string"), "yyyy-MM-dd"),
                F.col("customer_created_date"),
            ),
        )
        .filter(F.col("customer_updated_date") > F.lit(last_updated_date))
        .withColumnsRenamed(
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
    write_parquet(
        data_frame=df, file_name="stg_customer.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_customer_scd2(spark: SparkSession) -> None:
    """Generate SCD2 records for the customer dimension.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_customer_table = read_parquet(
        spark_context=spark,
        file_name="stg_customer.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    dim_customer_table = read_from_iceberg_table(
        spark_context=spark, table_name="dim_customer"
    )

    active_records = stg_customer_table.alias("s").join(
        dim_customer_table.alias("d"),
        on=[
            F.col("s.customer_id") == F.col("d.customer_id"),
            F.col("d.is_current") == F.lit(True),
        ],
        how="fullouter",
    )

    common_filter = (
        (F.col("d.address") != F.col("s.address"))
        | (F.col("d.state") != F.col("s.state"))
        | (F.col("d.zip_code_prefix") != F.col("s.zip_code_prefix"))
    )

    new_records = (
        active_records.filter(F.col("d.address").isNull() | common_filter)
        .select(
            F.col("s.customer_id"),
            F.col("s.address"),
            F.col("s.state"),
            F.col("s.zip_code_prefix"),
            F.col("s.updated_date"),
        )
        .withColumns(
            {
                "effective_from": F.col("updated_date"),
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
        .drop("updated_date")
    )

    expired_records = (
        active_records.filter(F.col("d.address").isNotNull() & common_filter)
        .select(
            F.col("d.customer_sk"),
            F.col("d.customer_id"),
            F.col("d.address"),
            F.col("d.state"),
            F.col("d.zip_code_prefix"),
            F.col("d.effective_from"),
        )
        .withColumns(
            {
                "effective_to": F.expr("current_date() - INTERVAL 1 DAY"),
                "is_current": F.lit(False),
            }
        )
    )

    df = new_records.unionByName(expired_records)
    write_parquet(
        data_frame=df, file_name="tmp_dim_customer.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_customer_dim_table(spark: SparkSession) -> None:
    """Merge SCD2 records into the customer dimension table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    tmp_dim_customer = read_parquet(
        spark_context=spark,
        file_name="tmp_dim_customer.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    tmp_dim_customer.createOrReplaceTempView("tmp_dim_customer_view")
    spark.sql("""
        MERGE INTO dim_customer AS target
        USING tmp_dim_customer_view AS src
            ON target.customer_sk = src.customer_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl() -> None:
    """Run the full ETL pipeline for the customer dimension."""
    logger.info("Running ETL process for dim_customer")
    sc = get_or_create_spark_session()
    create_stg_customer_table(sc)
    create_customer_scd2(sc)
    create_customer_dim_table(sc)
    logger.info("Finished ETL process for dim_customer")


if __name__ == "__main__":
    run_etl()
