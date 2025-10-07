"""
This script creates dimension and fact tables using Apache Iceberg in a Spark environment.
"""

# from pyspark.sql import SparkSession
# from utils import get_or_create_spark_session, create_logger

import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def dim_customer(spark: SparkSession):
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


def dim_seller(spark):
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


def dim_product(spark: SparkSession):
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


def fct_order_summary(spark: SparkSession):
    "Create the fct_order_summary fact table"
    logger.info("Creating fct_order_summary table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fct_order_summary (
            order_summary_sk STRING NOT NULL,
            customer_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_status STRING NOT NULL,
            total_order_value DECIMAL(10, 2) NOT NULL,
            total_freight_value DECIMAL(10, 2) NOT NULL,
            item_count INT NOT NULL,
            order_date_key DATE NOT NULL,
            delivered_to_carrier_date_key DATE NOT NULL,
            delivered_to_customer_date_key DATE NOT NULL,
            estimated_delivery_date_key DATE NOT NULL,
            order_approved_date_key DATE NOT NULL,
            min_shipping_limit_date_key DATE NOT NULL,
            max_shipping_limit_date_key DATE NOT NULL
        )
        """
    )


def fct_order_items(spark: SparkSession):
    "Create the fct_order_items fact table"
    logger.info("Creating fct_order_items table")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fct_order_items (
            order_item_sk STRING NOT NULL,
            seller_sk STRING NOT NULL,
            product_sk STRING NOT NULL,
            order_id STRING NOT NULL,
            order_item_id STRING NOT NULL,
            item_value DECIMAL(10, 2) NOT NULL,
            freight_value DECIMAL(10, 2) NOT NULL,
            order_date_key DATE NOT NULL,
            shipping_limit_date_key DATE NOT NULL
        )
        """
    )


if __name__ == "__main__":
    # logger = create_logger("migrations")
    # spark = get_or_create_spark_session()
    spark = SparkSession.builder.appName("Iceberg Migrations").getOrCreate()
    tables = [dim_customer, dim_seller, dim_product, fct_order_summary, fct_order_items]
    for table in tables:
        table(spark)
