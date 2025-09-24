from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema import get_seller_schema
from utils import (
    get_or_create_spark_session,
    read_csv,
    read_from_iceberg_table,
    create_logger,
    load_data_to_iceberg_table,
)

logger = create_logger("dim_seller.etl")


def create_stg_seller(spark):
    """Create the staging table for seller data

    Parameters:
        spark: SparkSession

    Returns:
        None

    Steps:
        1. Read the raw seller data from the CSV file in S3
        2. Rename columns to match the target schema
        3. Deduplicate records based on seller_id, keeping the latest updated_date
        4. Select relevant columns for the staging table
        5. Load the data into the Iceberg staging table 'stg_seller'
    """
    df = read_csv(
        spark_context=spark, object_name="sellers.csv", schema=get_seller_schema()
    )
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


def create_seller_scd2(spark):
    """Create SCD2 records for seller dimension table

    Parameters:
        spark: SparkSession

    Returns:
        None

    Steps:
        1. Read the staging table 'stg_seller' and dimension table 'dim_seller'
        2. Perform a full outer join on seller_id and is_current flag
        3. Identify new and changed records based on address, state, and zip_code_prefix
        4. Create new records with a new surrogate key, effective_from date, and is_current flag
        5. Create expired records by updating the effective_to date and is_current flag
        6. Union new and expired records
        7. Load the data into a temporary Iceberg table 'tmp_dim_seller'
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


def create_seller_dim(spark):
    """Merge SCD2 records into the dimension table

    Parameters:
        spark: SparkSession

    Returns:
        None

    Steps:
        1. Read the temporary table 'tmp_dim_seller' and dimension table 'dim_seller'
        2. Perform a merge operation based on seller_sk and is_current flag
        3. Update existing records with new effective_to date and is_current flag
        4. Insert new records into the dimension table
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
            INSERT (seller_sk, seller_id, address, state, zip_code_prefix, effective_from, effective_to, is_current)
            VALUES (src.seller_sk, src.seller_id, src.address, src.state, src.zip_code_prefix, src.effective_from, src.effective_to, src.is_current)
    """)


def run_etl():
    logger.info("Running ETL process for stg_seller")
    sc = get_or_create_spark_session()
    sc.sql(
        """
        CREATE OR REPLACE TABLE dim_seller (
            seller_sk STRING NOT NULL,
            seller_id STRING NOT NULL,
            address STRING NOT NULL,
            state STRING NOT NULL,
            zip_code_prefix STRING NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        ) USING ICEBERG
        """
    )
    create_stg_seller(sc)
    create_seller_scd2(sc)
    create_seller_dim(sc)
    logger.info("Finished ETL process for stg_seller")


if __name__ == "__main__":
    run_etl()
