from pyspark.sql import functions as F

from utils import get_or_create_spark_session, create_logger, load_data_to_iceberg_table

logger = create_logger("dim_date.etl")

start_date = "2020-01-01"
end_date = "2030-12-31"


def run_etl() -> None:
    """Create a  date dimension table."""
    logger.info("Creating date dimension table")
    sc = get_or_create_spark_session()

    df = sc.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
    date_df = df.select(
        F.explode(
            F.sequence(
                F.col("start_date").cast("date"),
                F.col("end_date").cast("date"),
                F.expr("interval 1 day"),
            )
        ).alias("date")
    )
    date_dim = date_df.withColumns(
        {
            "date_key": F.monotonically_increasing_id(),
            "year": F.year(F.col("date")),
            "month": F.month(F.col("date")),
            "day": F.dayofmonth(F.col("date")),
            "day_of_week": F.dayofweek(F.col("date")),
            "week_of_year": F.weekofyear(F.col("date")),
            "quarter": F.quarter(F.col("date")),
            "day_of_year": F.dayofyear(F.col("date")),
            "is_month_end": F.last_day(F.col("date")) == F.col("date"),
            "is_month_start": F.dayofmonth(F.col("date")) == 1,
            "is_weekend": F.when(F.col("day_of_week").isin(1, 7), True).otherwise(
                False
            ),
        }
    ).select(
        "date_key",
        "date",
        "year",
        "month",
        "day",
        "day_of_week",
        "week_of_year",
        "quarter",
        "day_of_year",
        "is_month_end",
        "is_month_start",
        "is_weekend",
    )

    load_data_to_iceberg_table(data_frame=date_dim, table_name="dim_date")
    logger.info("Finished creating date dimension table")


if __name__ == "__main__":
    run_etl()
