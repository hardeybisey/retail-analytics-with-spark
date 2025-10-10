from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# print(spark.range(5).show())
print(spark.sql("select * from dim_customer order by customer_id limit 5").show())
