from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_customer_schema() -> StructType:
    "Schema for the raw customers data"
    customer_schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("customer_address", StringType(), False),
            StructField("customer_state", StringType(), False),
            StructField("customer_zip_code", StringType(), False),
            StructField("customer_created_date", TimestampType(), False),
            StructField("customer_updated_date", TimestampType(), False),
        ]
    )

    return customer_schema


def get_seller_schema() -> StructType:
    "Schema for the raw sellers data"
    sellers_schema = StructType(
        [
            StructField("seller_id", StringType(), False),
            StructField("seller_address", StringType(), False),
            StructField("seller_zip_code", StringType(), False),
            StructField("seller_state", StringType(), False),
            StructField("seller_created_date", TimestampType(), False),
            StructField("seller_updated_date", TimestampType(), False),
        ]
    )
    return sellers_schema


def get_orders_schema() -> StructType:
    "Schema for the raw orders data"
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("order_purchase_date", TimestampType(), False),
            StructField("order_approved_at", TimestampType(), False),
            StructField("order_delivered_carrier_date", TimestampType(), False),
            StructField("order_delivered_customer_date", TimestampType(), False),
            StructField("order_estimated_delivery_date", TimestampType(), False),
        ]
    )

    return orders_schema


def get_order_items_schema() -> StructType:
    "Schema for the raw order items data"
    order_items_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("order_item_id", IntegerType(), False),
            StructField("product_id", StringType(), False),
            StructField("seller_id", StringType(), False),
            StructField("shipping_limit_date", TimestampType(), False),
            StructField("price", DoubleType(), False),
            StructField("freight_value", DoubleType(), False),
        ]
    )

    return order_items_schema


def get_products_schema() -> StructType:
    "Schema for the raw products data"
    products_schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_category_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("product_size_label", StringType(), False),
            StructField("product_width_cm", FloatType(), False),
            StructField("product_length_cm", FloatType(), False),
            StructField("product_height_cm", FloatType(), False),
            StructField("product_price", FloatType(), False),
        ]
    )

    return products_schema


def get_product_category_schema() -> StructType:
    "Schema for the raw product category data"
    product_category_schema = StructType(
        [
            StructField("product_category_id", StringType(), False),
            StructField("product_category", StringType(), False),
            StructField("product_sub_category", StringType(), False),
        ]
    )
    return product_category_schema
