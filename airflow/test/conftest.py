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


# def test_process_data(spark_session):
#     data = [("Alpha", "2000-01-01"), ("Beta", "1980-05-12")]
#     schema = StructType(
#         [
#             StructField("name", StringType(), True),
#             StructField("birthDate", StringType(), True),
#         ]
#     )
#     df = spark_session.createDataFrame(data, schema)
#     result_df = process_data(df)

#     assert result_df.count() == 1
