from pyspark.sql import functions as F
from schema import get_products_schema, get_product_category_schema
from utils import (
    get_or_create_spark_session,
    read_csv,
    load_data_to_iceberg_table,
    read_from_iceberg_table,
    create_logger,
)

logger = create_logger("dim_product.etl")


def create_stg_product(spark):
    """Create the staging table for product data

    Parameters:
        spark: SparkSession
    Returns:
        None
    Steps:
        1. Read the raw product and category data from the CSV file in S3
        2. Rename columns to match the target schema
        3. Deduplicate records based on product_id
        4. Select relevant columns for the staging table
        5. Load the data into the Iceberg staging table 'stg_product'
    """
    df_product = read_csv(
        spark_context=spark, object_name="products.csv", schema=get_products_schema()
    )
    df_product_category = read_csv(
        spark_context=spark,
        object_name="product_category.csv",
        schema=get_product_category_schema(),
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
            # "product_created_date": "created_date",
            # "product_updated_date": "updated_date",
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
    )

    load_data_to_iceberg_table(df, table_name="stg_product", mode="overwrite")


def create_product_scd2(spark):
    """Create the dim_product table with SCD Type 2 implementation

    Parameters:
        spark: SparkSession

    Returns:
        None

    Steps:
        1. Read data from the staging table 'stg_product' and dimension table 'dim_product'
        2. Generate surrogate keys and set SCD2 fields
        3. Load the data into a temporary Iceberg table 'tmp_dim_product'
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
            F.col("sc.product_id").alias("product_id"),
            F.col("sc.product_name").alias("product_name"),
            F.col("sc.price").alias("price"),
            F.col("sc.category_name").alias("category_name"),
            F.col("sc.sub_category").alias("sub_category"),
            F.col("sc.size_label").alias("size_label"),
            F.col("sc.length_cm").alias("length_cm"),
            F.col("sc.height_cm").alias("height_cm"),
            F.col("sc.width_cm").alias("width_cm"),
        )
        .withColumns(
            {
                "effective_from": F.current_date(),
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


def create_product_dim(spark):
    """Merge SCD2 records into the dimension table

    Parameters:
        spark: SparkSession

    Returns:
        None
    Steps:
        1. Merge new and updated records from 'tmp_dim_product' into 'dim_product'
        2. Insert new records into the dimension table
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
    sc.sql(
        """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_sk STRING NOT NULL,
            product_id STRING NOT NULL,
            product_name STRING NOT NULL,
            category_name STRING NOT NULL,
            sub_category STRING NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            size_label STRING NOT NULL,
            length_cm FLOAT NOT NULL,
            height_cm FLOAT NOT NULL,
            width_cm FLOAT NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        ) USING ICEBERG
        """
    )
    create_stg_product(spark=sc)
    create_product_scd2(spark=sc)
    create_product_dim(spark=sc)
    logger.info("ETL process completed for dim_product")


if __name__ == "__main__":
    run_etl()
