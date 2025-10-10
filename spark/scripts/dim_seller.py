from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    read_parquet,
    read_from_iceberg_table,
    create_logger,
    write_parquet,
    get_last_updated_date,
)
import os

S3_INPUTS_BUCKET = os.environ["S3_INPUTS_BUCKET"]
S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]

logger = create_logger("dim_seller")


def create_stg_seller_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw seller data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    last_updated_date = get_last_updated_date(
        spark_context=spark, table_name="dim_seller"
    )
    df = read_parquet(
        spark_context=spark, file_name="sellers.parquet", s3_bucket=S3_INPUTS_BUCKET
    )
    df = (
        df.withColumn(
            "seller_updated_date",
            F.coalesce(
                F.to_date(F.col("seller_updated_date").cast("string"), "yyyy-MM-dd"),
                F.col("seller_created_date"),
            ),
        )
        .filter(F.col("seller_updated_date") > F.lit(last_updated_date))
        .withColumnsRenamed(
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
    write_parquet(
        data_frame=df, file_name="stg_seller.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_seller_scd2(spark: SparkSession) -> None:
    """Generate SCD2 records for the seller dimension.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_seller_table = read_parquet(
        spark_context=spark,
        file_name="stg_seller.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    dim_seller_table = read_from_iceberg_table(
        spark_context=spark, table_name="dim_seller"
    )

    active_records = stg_seller_table.alias("s").join(
        dim_seller_table.alias("d"),
        on=[
            F.col("s.seller_id") == F.col("d.seller_id"),
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
            F.col("s.seller_id"),
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
            "seller_sk",
            F.sha2(F.concat_ws("||", F.col("seller_id"), F.col("effective_from")), 256),
        )
        .drop("updated_date")
    )

    expired_records = (
        active_records.filter(F.col("d.address").isNotNull() & common_filter)
        .select(
            F.col("d.seller_sk"),
            F.col("d.seller_id"),
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
        data_frame=df, file_name="tmp_dim_seller.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_seller_dim_table(spark) -> None:
    """Merge SCD2 records into the seller dimension table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    tmp_dim_seller = read_parquet(
        spark_context=spark,
        file_name="tmp_dim_seller.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    tmp_dim_seller.createOrReplaceTempView("tmp_dim_seller_view")

    spark.sql("""
        MERGE INTO dim_seller AS target
        USING tmp_dim_seller_view AS src
            ON target.seller_sk = src.seller_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl():
    """Run the full ETL pipeline for the seller dimension."""
    logger.info("Running ETL process for stg_seller")
    sc = get_or_create_spark_session()
    create_stg_seller_table(sc)
    create_seller_scd2(sc)
    create_seller_dim_table(sc)
    logger.info("Finished ETL process for stg_seller")


if __name__ == "__main__":
    run_etl()
