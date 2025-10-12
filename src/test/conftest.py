import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def test_spark_session():
    spark = (
        SparkSession.builder.appName("unit-tests")
        .master("local[*]")
        .appName("PyTest")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
