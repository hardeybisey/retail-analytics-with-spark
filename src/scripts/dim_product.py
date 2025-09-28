from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils import (
    get_or_create_spark_session,
    read_parquet,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
)

logger = create_logger("dim_product")


def create_stg_product_table(spark: SparkSession) -> None:
    """Create the staging table for product data

    This function creates a `stg_product` table from both product and product category data read from Parquet files, renaming columns and deduplicating the data.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    df_product = read_parquet(
        spark_context=spark,
        object_name="products.parquet",
    )
    df_product_category = read_parquet(
        spark_context=spark,
        object_name="product_category.parquet",
    )

    df = df_product.join(
        df_product_category,
        on="product_category_id",
        how="left",
    ).dropDuplicates(["product_id"])

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

    load_data_to_iceberg_table(df, table_name="stg_product", mode="overwrite")


def create_product_scd2(spark: SparkSession) -> None:
    """Create SCD2 records for product dimension table

    This function creates a `tmp_dim_product` table with Slowly Changing Dimension Type 2
    (SCD2) records for the product dimension table with changes from `stg_product` table.

    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    stg_product_table = read_from_iceberg_table(spark, "stg_product")
    dim_product_table = read_from_iceberg_table(spark, "dim_product")

    active_records = stg_product_table.alias("sc").join(
        dim_product_table.alias("dc"),
        on=[
            F.col("sc.product_id") == F.col("dc.product_id"),
            F.col("dc.is_current") == F.lit(True),
        ],
        how="fullouter",
    )

    common_filter = (
        (F.col("dc.price") != F.col("sc.price"))
        | (F.col("dc.category_name") != F.col("sc.category_name"))
        | (F.col("dc.sub_category") != F.col("sc.sub_category"))
    )

    new_records = (
        active_records.filter(F.col("dc.price").isNull() | common_filter)
        .select(
            F.col("sc.product_id"),
            F.col("sc.product_name"),
            F.col("sc.price"),
            F.col("sc.category_name"),
            F.col("sc.sub_category"),
            F.col("sc.size_label"),
            F.col("sc.length_cm"),
            F.col("sc.height_cm"),
            F.col("sc.width_cm"),
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
            "product_sk",
            F.sha2(
                F.concat_ws("||", F.col("product_id"), F.col("effective_from")), 256
            ),
        )
        .drop("created_date")
    )

    expired_records = (
        active_records.filter(F.col("dc.price").isNotNull() & common_filter)
        .select(
            F.col("dc.product_sk").alias("product_sk"),
            F.col("dc.product_id").alias("product_id"),
            F.col("dc.product_name").alias("product_name"),
            F.col("dc.category_name").alias("category_name"),
            F.col("dc.sub_category").alias("sub_category"),
            F.col("dc.price").alias("price"),
            F.col("dc.size_label").alias("size_label"),
            F.col("dc.length_cm").alias("length_cm"),
            F.col("dc.height_cm").alias("height_cm"),
            F.col("dc.width_cm").alias("width_cm"),
            F.col("dc.effective_from").alias("effective_from"),
        )
        .withColumns(
            {
                "effective_to": F.col("effective_from") - F.expr("INTERVAL 1 DAY"),
                "is_current": F.lit(False),
            }
        )
    )

    df = new_records.unionByName(expired_records)

    load_data_to_iceberg_table(df, table_name="tmp_dim_product", mode="overwrite")


def create_product_dim_table(spark: SparkSession) -> None:
    """Merge SCD2 records into product dimension table

    This function merges the SCD2 records from `tmp_dim_product` into the
    `dim_product` table.


    Parameters
    ----------
    spark : SparkSession
        Spark session for the ETL process
    """
    spark.sql("""
        MERGE INTO dim_product AS target
        USING tmp_dim_product AS src
            ON target.product_sk = src.product_sk AND target.is_current = true
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_to = src.effective_to,
                target.is_current = src.is_current
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def run_etl():
    logger.info("Starting ETL process for dim_product")
    sc = get_or_create_spark_session()
    create_stg_product_table(spark=sc)
    create_product_scd2(spark=sc)
    create_product_dim_table(spark=sc)
    logger.info("ETL process completed for dim_product")


if __name__ == "__main__":
    run_etl()
