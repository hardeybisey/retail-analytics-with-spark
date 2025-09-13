from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType


def get_customer_schema() -> StructType:
    "Schema for the raw customers data"
    customer_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("customer_state", StringType(), True),
            StructField("customer_zip_code", StringType(), True),
            StructField("customer_created_date", TimestampType(), True),
            StructField("customer_updated_date", TimestampType(), True),
        ]
    )

    return customer_schema


def get_seller_schema() -> StructType:
    "Schema for the raw sellers data"
    sellers_schema = StructType(
        [
            StructField("seller_id", StringType(), True),
            StructField("seller_address", StringType(), True),
            StructField("seller_zip_code", StringType(), True),
            StructField("seller_state", StringType(), True),
            StructField("seller_created_date", TimestampType(), True),
            StructField("seller_updated_date", TimestampType(), True),
        ]
    )
    return sellers_schema


def get_orders_schema() -> StructType:
    "Schema for the raw orders data"
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_date", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
        ]
    )

    return orders_schema


def get_order_items_schema() -> StructType:
    "Schema for the raw order items data"
    order_items_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
        ]
    )

    return order_items_schema


def get_products_schema() -> StructType:
    "Schema for the raw products data"
    products_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_category_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_size_label", StringType(), True),
            StructField("product_width_cm", FloatType(), True),
            StructField("product_length_cm", FloatType(), True),
            StructField("product_height_cm", FloatType(), True),
            StructField("product_price", FloatType(), True),
        ]
    )

    return products_schema


def get_product_category_schema() -> StructType:
    "Schema for the raw product category data"
    product_category_schema = StructType(
        [
            StructField("product_category_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_sub_category", StringType(), True),
        ]
    )
    return product_category_schema
