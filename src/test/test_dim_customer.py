from unittest.mock import patch, MagicMock
from pyspark.sql import Row
import datetime
from spark.dim_customer import (
    create_customer_scd2,
    create_stg_customer_table,
    create_customer_dim_table,
)


@patch("dim_customer.get_last_updated_date")
@patch("dim_customer.read_parquet")
@patch("dim_customer.write_parquet")
def test_create_stg_customer_table(
    mock_write, mock_read, mock_last_date, test_spark_session
):
    mock_last_date.return_value = datetime.date(2024, 1, 1)

    data = [
        Row(
            customer_id="1",
            customer_address="Addr1",
            customer_state="State1",
            customer_zip_code="12345",
            customer_created_date=datetime.date(2024, 1, 1),
            customer_updated_date=None,
        ),
        Row(
            customer_id="2",
            customer_address="Addr2",
            customer_state="State2",
            customer_zip_code="12345",
            customer_created_date=datetime.date(2024, 1, 1),
            customer_updated_date=datetime.date(2024, 3, 1),
        ),
    ]
    mock_read.return_value = test_spark_session.createDataFrame(data)

    create_stg_customer_table(test_spark_session)

    # mock_write.assert_called_once()
    # written_df = mock_write.call_args.kwargs["data_frame"]
    # result = [r.asDict() for r in written_df.collect()]

    # assert len(result) == 1
    # assert result[0]["address"] == "Addr2"
    # assert "row_num" not in result[0]


@patch("dim_customer.read_parquet")
@patch("dim_customer.load_to_iceberg")
@patch("dim_customer.write_parquet")
def test_create_customer_scd2(
    mock_write, mock_read_iceberg, mock_read_stg, test_spark_session
):
    stg_data = [
        Row(
            customer_id="1",
            address="A1",
            state="S1",
            zip_code_prefix="Z1",
            updated_date=datetime.date(2024, 3, 1),
        ),
        Row(
            customer_id="2",
            address="A2",
            state="S2",
            zip_code_prefix="Z2",
            updated_date=datetime.date(2024, 3, 2),
        ),
    ]
    dim_data = [
        Row(
            customer_sk="SK1",
            customer_id="C1",
            address="A0",
            state="S0",
            zip_code_prefix="Z0",
            effective_from=datetime.date(2024, 1, 1),
            effective_to=None,
            is_current=True,
        ),
        Row(
            customer_sk="SK2",
            customer_id="C2",
            address="A2",
            state="S2",
            zip_code_prefix="Z2",
            effective_from=datetime.date(2024, 2, 1),
            effective_to=None,
            is_current=True,
        ),
    ]

    mock_read_stg.return_value = test_spark_session.createDataFrame(stg_data)
    mock_read_iceberg.return_value = test_spark_session.createDataFrame(dim_data)

    create_customer_scd2(test_spark_session)

    # mock_write.assert_called_once()
    # written_df = mock_write.call_args.kwargs["data_frame"]
    # rows = written_df.collect()

    # # C1 should appear as new (address changed)
    # # C2 remains same, so one expired (is_current=False) + one new (is_current=True)
    # assert any(r.customer_id == "C1" and r.is_current for r in rows)
    # assert all("effective_from" in r.asDict() for r in rows)


@patch("dim_customer.read_parquet")
def test_create_customer_dim_table(mock_read, spark):
    tmp_data = [
        Row(
            customer_sk="SK1",
            customer_id="C1",
            address="A1",
            state="S1",
            zip_code_prefix="Z1",
            effective_from=datetime.date(2024, 3, 1),
            effective_to=None,
            is_current=True,
        )
    ]
    mock_read.return_value = spark.createDataFrame(tmp_data)

    # Mock SQL execution
    spark.sql = MagicMock()

    create_customer_dim_table(spark)
    # spark.sql.assert_called_once()
    # sql_query = spark.sql.call_args[0][0]
    # assert "MERGE INTO dim_customer" in sql_query


# -----------------------------------------------------------
# Test run_etl
# -----------------------------------------------------------


# @patch("dim_customer.create_customer_dim_table")
# @patch("dim_customer.create_customer_scd2")
# @patch("dim_customer.create_stg_customer_table")
# @patch("dim_customer.get_or_create_spark_session")
# def test_run_etl(mock_get_spark, mock_stg, mock_scd2, mock_dim):
#     mock_spark = MagicMock()
#     mock_get_spark.return_value = mock_spark

#     dim_customer.run_etl()

#     mock_stg.assert_called_once_with(mock_spark)
#     mock_scd2.assert_called_once_with(mock_spark)
#     mock_dim.assert_called_once_with(mock_spark)


# def test_dataframe_equality(spark_session):
#     df1 = spark_session.createDataFrame([("Alpha", 20)], ["name", "age"])
#     df2 = spark_session.createDataFrame([("Alpha", 20)], ["name", "age"])

#     assertDataFrameEqual(df1, df2)
