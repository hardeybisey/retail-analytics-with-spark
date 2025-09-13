import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

t_log = logging.getLogger("airflow.task")


def create_spark_session(caller: str = "") -> SparkSession:
    t_log.info(f"Creating Spark session for {caller}")
    spark = SparkSession.builder.appName(
        f"Retail Analytics with Spark - {caller}"
    ).getOrCreate()
    # .config("spark.hadoop.fs.s3a.access.key", "admin") \
    # .config("spark.hadoop.fs.s3a.secret.key", "password") \
    # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000").getOrCreate()
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.466")\

    return spark


def extract_raw_data(
    spark: SparkSession, s3_bucket: str, object_name: str, schema: StructType
) -> SparkDataFrame:
    "Extract the raw data from the CSV file"
    t_log.info(f"Extracting raw data from s3a://{s3_bucket}/{object_name}")
    data = spark.read.csv(
        f"s3a://{s3_bucket}/{object_name}", header=True, schema=schema
    )
    return data


def load_data_to_iceberg_table(
    df: SparkDataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    "Load the data into the Iceberg table"
    t_log.info(f"Loading data into Iceberg table {table_name}")
    df.write.mode(mode).saveAsTable(table_name)


class IcebergSparkSession:
    def __init__(
        self, app_name, warehouse_path, s3_endpoint, s3_access_key, s3_secret_key
    ):
        self.app_name = app_name
        self.warehouse_path = warehouse_path
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.spark = self.create_spark_session()
        self.configure_s3()

    def create_spark_session(self):
        packages = [
            "hadoop-aws-3.3.4",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12-1.5.2",
            "aws-java-sdk-bundle-1.12.262",
        ]

        builder = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars.packages", ",".join(packages))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", self.warehouse_path)
            .config("spark.ui.port", "4050")
        )

        return builder.getOrCreate()

    def configure_s3(self):
        sc = self.spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.s3_endpoint)
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
