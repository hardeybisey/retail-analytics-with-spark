from pyspark.sql import functions as F

from utils import get_or_create_spark_session, create_logger, load_data_to_iceberg_table

logger = create_logger("dim_date.etl")

start_date = "2020-01-01"
end_date = "2030-12-31"


def run_etl():
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
    date_dim = (
        date_df.withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("day", F.dayofmonth(F.col("date")))
        .withColumn("day_of_week", F.dayofweek(F.col("date")))
        .withColumn("week_of_year", F.weekofyear(F.col("date")))
        .withColumn("quarter", F.quarter(F.col("date")))
        .withColumn("day_of_year", F.dayofyear(F.col("date")))
        .withColumn(
            "is_weekend", F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False)
        )
    )

    load_data_to_iceberg_table(df=date_dim, table_name="dim_date")
    logger.info("Finished creating date dimension table")


if __name__ == "__main__":
    run_etl()
