import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


def create_logger(name: str) -> logging.Logger:
    """Create custom logger for the ETL process"""
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


logger = create_logger("retail-analytics-with-spark")


def get_or_create_spark_session(
    app_name: str = "Retail Analytics with Spark", config: dict = {}
) -> SparkSession:
    """Get or create a Spark session

    Parameters
    ----------
    app_name : str, optional
        Application name for the Spark session, by default "Retail Analytics with Spark"
    config : dict, optional
        Configuration settings for the Spark session, by default {}

    Returns
    -------
    SparkSession
        The created or existing Spark session
    """
    logger.info("Creating Spark session")
    sc = SparkSession.builder.appName(app_name)
    for key, value in config.items():
        sc.config(key, value)

    return sc.getOrCreate()


def read_parquet(
    spark_context: SparkSession,
    file_name: str,
    s3_bucket: str = "demo-input",
) -> SparkDataFrame:
    """Read a parquet file from S3.

    Parameters
    ----------
    spark_context : SparkSession
        Spark session for the ETL process
    file_name : str
        Name of the Parquet file to read
    s3_bucket : str, optional
        Name of the S3 bucket, by default "demo-input"

    Returns
    -------
    SparkDataFrame
        The Spark DataFrame containing the data from the Parquet file
    """
    logger.info(f"Extracting raw data from s3a://{s3_bucket}/{file_name}")
    data = spark_context.read.parquet(f"s3a://{s3_bucket}/{file_name}")
    return data


def write_parquet(
    data_frame: SparkDataFrame,
    file_name: str,
    mode: str = "overwrite",
    s3_bucket: str = "demo-input",
) -> None:
    """Write the data_frame as a parquet file to S3.

    Parameters
    ----------
    data_frame : SparkDataFrame
        Dataframe to write to parquet
    file_name : str
        Name of the Parquet file to write to.
    mode : str, optional
        The write mode for the data load, by default "overwrite"
    s3_bucket : str, optional
        Name of the S3 bucket, by default "demo-input"
    """
    logger.info(f"Writing data to s3a://{s3_bucket}/{file_name}")
    data_frame.write.mode(mode).parquet(f"s3a://{s3_bucket}/{file_name}")


def load_data_to_iceberg_table(
    data_frame: SparkDataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """Load the given data_frame into an Iceberg table.

    Parameters
    ----------
    data_frame : SparkDataFrame
        The Spark DataFrame containing the data to load
    table_name : str
        The name of the Iceberg table to load data into
    mode : str, optional
        The write mode for the data load, by default "overwrite"
    """
    logger.info(f"Writing data to Iceberg table {table_name}")
    data_frame.write.mode(mode).saveAsTable(table_name)


def read_from_iceberg_table(
    spark_context: SparkSession, table_name: str
) -> SparkDataFrame:
    """Read a table from iceberg.

    Parameters
    ----------
    spark_context : SparkSession
        Spark session for the ETL process
    table_name : str
        Name of the Iceberg table to read data from

    Returns
    -------
    SparkDataFrame
        The Spark DataFrame containing the data from the Iceberg table
    """
    logger.info(f"Reading data from Iceberg table {table_name}")
    df = spark_context.read.table(table_name)
    return df
