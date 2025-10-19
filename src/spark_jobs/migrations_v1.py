"""
This script creates dimension and fact tables using Apache Iceberg in a Spark environment.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    BooleanType,
)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from utils import get_or_create_spark_session, create_logger, load_to_iceberg


def create_namespace(spark: SparkSession) -> None:
    "Create the retail_analytics namespace"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.retail_analytics")


def dim_customer(spark: SparkSession) -> None:
    "Create the dim_customer table with SCD Type 2 implementation"
    logger.info("Creating dim_customer table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_sk STRING NOT NULL,
            customer_id STRING NOT NULL,
            address STRING NOT NULL,
            state STRING NOT NULL,
            zip_code_prefix STRING NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        ) TBLPROPERTIES(
            "write.delete.mode"="copy-on-write",
            "write.update.mode"="merge-on-read",
            "write.merge.mode"="merge-on-read",
            "write.metadata.metrics.column.customer_id"="full",
            "write.metadata.metrics.column.address"="none"
        )
        """
    )


def dim_seller(spark) -> None:
    "Create the dim_seller table with SCD Type 2 implementation"
    logger.info("Creating dim_seller table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS dim_seller (
            seller_sk STRING NOT NULL,
            seller_id STRING NOT NULL,
            address STRING NOT NULL,
            state STRING NOT NULL,
            zip_code_prefix STRING NOT NULL,
            effective_from DATE NOT NULL,
            effective_to DATE,
            is_current BOOLEAN NOT NULL
        )
        """
    )


def dim_product(spark: SparkSession) -> None:
    "Create the dim_product table with SCD Type 2 implementation"
    logger.info("Creating dim_product table")
    spark.sql(
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
        )
        """
    )


def fact_order_summary(spark: SparkSession) -> None:
    "Create the fct_order_summary fact table"
    logger.info("Creating fact_order_summary table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_order_summary (
            customer_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_status STRING NOT NULL,
            total_order_value DECIMAL(10, 2) NOT NULL,
            total_freight_value DECIMAL(10, 2) NOT NULL,
            item_count INT NOT NULL,
            order_date_key INT NOT NULL,
            delivered_to_carrier_date_key INT NOT NULL,
            delivered_to_customer_date_key INT NOT NULL,
            estimated_delivery_date_key INT NOT NULL,
            order_approved_date_key INT NOT NULL,
            min_shipping_limit_date_key INT NOT NULL,
            max_shipping_limit_date_key INT NOT NULL
        )
        """
    )


def fact_order_items(spark: SparkSession) -> None:
    "Create the fct_order_items fact table"
    logger.info("Creating fact_order_items table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_order_items (
            seller_sk STRING NOT NULL,
            product_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_item_id STRING NOT NULL,
            item_value DECIMAL(10, 2) NOT NULL,
            freight_value DECIMAL(10, 2) NOT NULL,
            order_date_key INT NOT NULL,
            shipping_limit_date_key INT NOT NULL
        )
        """
    )


def dim_date(spark: SparkSession) -> None:
    """Create a  date dimension table."""
    logger.info("Creating date dimension table")
    start_date, end_date = "2020-01-01", "2025-12-31"
    schema = StructType(
        [
            StructField("date_key", IntegerType()),
            StructField("date", DateType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("day", IntegerType()),
            StructField("day_of_week", IntegerType()),
            StructField("week_of_year", IntegerType()),
            StructField("quarter", IntegerType()),
            StructField("day_of_year", IntegerType()),
            StructField("is_month_end", BooleanType()),
            StructField("is_month_start", BooleanType()),
            StructField("is_weekend", BooleanType()),
        ]
    )

    null_rows = spark.createDataFrame(
        [(0, None, None, None, None, None, None, None, None, None, None, None)], schema
    )
    df = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
    date_df = df.select(
        F.explode(
            F.sequence(
                F.col("start_date").cast("date"),
                F.col("end_date").cast("date"),
                F.expr("interval 1 day"),
            )
        ).alias("date")
    )
    date_dim = (
        date_df.coalesce(1)
        .withColumns(
            {
                "date_key": F.rank().over(
                    Window.orderBy("date"),
                ),
                "year": F.year(F.col("date")),
                "month": F.month(F.col("date")),
                "day": F.dayofmonth(F.col("date")),
                "day_of_week": F.dayofweek(F.col("date")),
                "week_of_year": F.weekofyear(F.col("date")),
                "quarter": F.quarter(F.col("date")),
                "day_of_year": F.dayofyear(F.col("date")),
                "is_month_end": F.last_day(F.col("date")) == F.col("date"),
                "is_month_start": F.dayofmonth(F.col("date")) == 1,
                "is_weekend": F.when(F.col("day_of_week").isin(1, 7), True).otherwise(
                    False
                ),
            }
        )
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "day",
            "day_of_week",
            "week_of_year",
            "quarter",
            "day_of_year",
            "is_month_end",
            "is_month_start",
            "is_weekend",
        )
    )
    df = null_rows.union(date_dim)

    load_to_iceberg(data_frame=df, table_name="dim_date")


if __name__ == "__main__":
    logger = create_logger("migrations")
    spark = get_or_create_spark_session()
    tables = [
        create_namespace,
        dim_customer,
        dim_seller,
        dim_product,
        fact_order_summary,
        fact_order_items,
        dim_date,
    ]
    for table in tables:
        table(spark)
