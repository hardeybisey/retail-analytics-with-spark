from schema import get_products_schema, get_product_category_schema
from utils import (
    get_or_create_spark_session,
    extract_raw_data,
    load_data_to_iceberg_table,
    create_logger,
)

logger = create_logger("dim_product.etl")


def transform_product_data(df_product_category, df_product):
    logger.info("Transforming product/category data for dimensional modelling")

    df_product_category = df_product_category.withColumnsRenamed(
        {
            "product_category_id": "category_id",
            "product_category": "category_name",
            "product_sub_category": "sub_category",
        }
    ).dropDuplicates(["category_id"])

    df_product = df_product.withColumnsRenamed(
        {
            "product_category_id": "category_id",
            "product_size_label": "size_label",
            "product_price": "price",
            "product_length_cm": "length_cm",
            "product_height_cm": "height_cm",
            "product_width_cm": "width_cm",
        }
    ).dropDuplicates(["product_id"])

    dim_product = df_product.join(
        df_product_category,
        on="category_id",
        how="left",
    ).select(
        "product_id",
        "category_id",
        "product_name",
        "category_name",
        "sub_category",
        "price",
        "size_label",
        "length_cm",
        "height_cm",
        "width_cm",
    )
    logger.info("Transformed product/category data successfully")
    return dim_product


def run_etl():
    logger.info("Starting ETL process for dim_product")
    sc = get_or_create_spark_session()

    df_product_category = extract_raw_data(
        spark_context=sc,
        object_name="product_category.csv",
        schema=get_product_category_schema(),
    )
    df_product = extract_raw_data(
        spark_context=sc, object_name="products.csv", schema=get_products_schema()
    )

    dim_product = transform_product_data(df_product_category, df_product)

    load_data_to_iceberg_table(df=dim_product, table_name="dim_product")

    logger.info("ETL process completed for dim_product")


if __name__ == "__main__":
    run_etl()
