from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("FileFormatConversion") \
    .getOrCreate()

# Read CSV dataset
df = spark.read.csv(
    "excercise_1_spark_pipeline/data/employee.csv",
    header=True,
    inferSchema=True
)

print("Original CSV Dataset")
df.show()

# Write JSON format
df.write.mode("overwrite").json(
    "exercise_3_file_formats/output_json"
)

print("JSON file written successfully")

# Write Parquet format
df.write.mode("overwrite").parquet(
    "exercise_3_file_formats/output_parquet"
)

print("Parquet file written successfully")

spark.stop()