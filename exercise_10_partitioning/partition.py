from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercise10_Partitioning") \
    .getOrCreate()

df = spark.read.csv(
    "exercise_10_partitioning/data/employees.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Write partitioned parquet files
df.write \
    .partitionBy("department") \
    .mode("overwrite") \
    .parquet("exercise_10_partitioning/output")

print("Partitioned parquet files written successfully")

spark.stop()