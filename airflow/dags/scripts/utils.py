import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = create_logger("retail-analytics.utils")


def get_or_create_spark_session(
    app_name: str = "Retail Analytics with Spark", config: dict = {}
) -> SparkSession:
    logger.info("Creating Spark session")

    sc = SparkSession.builder.appName(app_name).getOrCreate()
    for key, value in config.items():
        sc.conf.set(key, value)

    return sc


def read_csv(
    spark_context: SparkSession,
    object_name: str,
    schema: StructType,
    s3_bucket: str = "csv-input",
) -> SparkDataFrame:
    "Extract the raw data from the CSV file"
    logger.info(f"Extracting raw data from s3a://{s3_bucket}/{object_name}")
    data = spark_context.read.csv(
        f"s3a://{s3_bucket}/{object_name}", header=True, schema=schema
    )
    return data


def load_data_to_iceberg_table(
    df: SparkDataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    "Load the data into the Iceberg table"
    logger.info(f"Writing data to Iceberg table {table_name}")
    df.write.mode(mode).saveAsTable(table_name)


def read_from_iceberg_table(
    spark_context: SparkSession, table_name: str
) -> SparkDataFrame:
    "Read the data from the Iceberg table"
    logger.info(f"Reading data from Iceberg table {table_name}")
    df = spark_context.read.table(table_name)
    return df


# class IcebergSparkSession:
#     def __init__(self, app_name: str, config: dict):
#         self.app_name = app_name
#         self.config = config
#         self.warehouse_path = config.get("warehouse_path")
#         self.s3_endpoint = config.get("s3_endpoint")
#         self.s3_access_key = config.get("s3_access_key")
#         self.s3_secret_key = config.get("s3_secret_key")
#         self.spark = self.create_spark_session()
#         self.configure_s3()

#     def create_spark_session(self):
#         sc = SparkSession.builder.appName(self.app_name).getOrCreate()

#         for key, value in self.config.items():
#             sc.conf.set(key, value)

#         return sc

#         # self.spark = sc

#         # packages = [
#         #     "hadoop-aws-3.3.4",
#         #     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12-1.5.2",
#         #     "aws-java-sdk-bundle-1.12.262",
#         # ]

#         # builder = (
#         #     SparkSession.builder.appName(self.app_name)
#         #     .config("spark.jars.packages", ",".join(packages))
#         #     .config(
#         #         "spark.sql.extensions",
#         #         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#         #     )
#         #     .config(
#         #         "spark.sql.catalog.spark_catalog",
#         #         "org.apache.iceberg.spark.SparkCatalog",
#         #     )
#         #     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#         #     .config("spark.sql.catalog.local.type", "hadoop")
#         #     .config("spark.sql.catalog.local.warehouse", self.warehouse_path)
#         #     .config("spark.ui.port", "4050")
#         # )

#         # return builder.getOrCreate()

#     def configure_s3(self):
#         sc = self.spark.sparkContext
#         sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
#         sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
#         sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.s3_endpoint)
#         sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
#         sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
#         sc._jsc.hadoopConfiguration().set(
#             "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
#         )
