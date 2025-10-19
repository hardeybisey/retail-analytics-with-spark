from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    read_parquet,
    write_parquet,
    read_from_iceberg,
    create_logger,
    get_last_updated_date,
)
import os

S3_INPUTS_BUCKET = os.environ["S3_INPUTS_BUCKET"]
S3_STG_BUCKET = os.environ["S3_STG_BUCKET"]

logger = create_logger("dim_product")


def create_stg_product_table(spark: SparkSession) -> None:
    """Create a deduplicated staging table from raw product data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    last_updated_date = get_last_updated_date(
        spark_session=spark, table_name="dim_product"
    )
    df = read_parquet(
        spark_session=spark, file_name="products.parquet", s3_bucket=S3_INPUTS_BUCKET
    )

    df = (
        df.withColumn(
            "product_updated_date",
            F.coalesce(
                F.to_date(F.col("product_updated_date").cast("string"), "yyyy-MM-dd"),
                F.col("product_created_date"),
            ),
        )
        .filter(F.col("product_updated_date") > F.lit(last_updated_date))
        .coalesce(1)
        .dropDuplicates(["product_id"])
    )

    df = df.withColumnsRenamed(
        {
            "product_category_id": "category_id",
            "product_category": "category_name",
            "product_sub_category": "sub_category",
            "product_size_label": "size_label",
            "product_price": "price",
            "product_length_cm": "length_cm",
            "product_height_cm": "height_cm",
            "product_width_cm": "width_cm",
            "product_created_date": "created_date",
            "product_updated_date": "updated_date",
        }
    ).select(
        "product_id",
        "product_name",
        "category_name",
        "sub_category",
        "price",
        "size_label",
        "length_cm",
        "height_cm",
        "width_cm",
        "created_date",
        "updated_date",
    )
    write_parquet(
        data_frame=df, file_name="stg_product.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_product_scd2(spark: SparkSession) -> None:
    """Generate SCD2 records for the product dimension.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_product_table = read_parquet(
        spark_session=spark,
        file_name="stg_product.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    dim_product_table = read_from_iceberg(spark_session=spark, table_name="dim_product")

    active_records = stg_product_table.alias("s").join(
        dim_product_table.alias("d"),
        on=[
            F.col("s.product_id") == F.col("d.product_id"),
            F.col("d.is_current") == F.lit(True),
        ],
        how="fullouter",
    )

    common_filter = (
        (F.col("d.price") != F.col("s.price"))
        | (F.col("d.category_name") != F.col("s.category_name"))
        | (F.col("d.sub_category") != F.col("s.sub_category"))
    )

    new_records = (
        active_records.filter(F.col("d.price").isNull() | common_filter)
        .select(
            F.col("s.product_id"),
            F.col("s.product_name"),
            F.col("s.price"),
            F.col("s.category_name"),
            F.col("s.sub_category"),
            F.col("s.size_label"),
            F.col("s.length_cm"),
            F.col("s.height_cm"),
            F.col("s.width_cm"),
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
            "product_sk",
            F.sha2(
                F.concat_ws("||", F.col("product_id"), F.col("effective_from")), 256
            ),
        )
        .drop("updated_date")
    )

    expired_records = (
        active_records.filter(F.col("d.price").isNotNull() & common_filter)
        .select(
            F.col("d.product_sk").alias("product_sk"),
            F.col("d.product_id").alias("product_id"),
            F.col("d.product_name").alias("product_name"),
            F.col("d.category_name").alias("category_name"),
            F.col("d.sub_category").alias("sub_category"),
            F.col("d.price").alias("price"),
            F.col("d.size_label").alias("size_label"),
            F.col("d.length_cm").alias("length_cm"),
            F.col("d.height_cm").alias("height_cm"),
            F.col("d.width_cm").alias("width_cm"),
            F.col("d.effective_from").alias("effective_from"),
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
        data_frame=df, file_name="tmp_dim_product.parquet", s3_bucket=S3_STG_BUCKET
    )


def create_product_dim_table(spark: SparkSession) -> None:
    """Merge SCD2 records into the product dimension table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    tmp_dim_product = read_parquet(
        spark_session=spark,
        file_name="tmp_dim_product.parquet",
        s3_bucket=S3_STG_BUCKET,
    )
    tmp_dim_product.createOrReplaceTempView("tmp_dim_product_view")
    spark.sql("""
        MERGE INTO dim_product AS target
        USING tmp_dim_product_view AS src
            ON target.product_sk = src.product_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl():
    """Run the full ETL pipeline for the product dimension."""
    logger.info("Starting ETL process for dim_product")
    sc = get_or_create_spark_session()
    create_stg_product_table(spark=sc)
    create_product_scd2(spark=sc)
    create_product_dim_table(spark=sc)
    logger.info("ETL process completed for dim_product")


if __name__ == "__main__":
    run_etl()
