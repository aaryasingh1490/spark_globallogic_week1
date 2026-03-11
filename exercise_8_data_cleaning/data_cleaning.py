from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim

spark = SparkSession.builder \
    .appName("Exercise8_DataCleaning") \
    .getOrCreate()

df = spark.read.csv(
    "exercise_8_data_cleaning/data/employees_dirty.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Clean department names
df_clean = df.withColumn(
    "department",
    upper(trim(col("department")))
)

print("Cleaned Dataset")
df_clean.show()

spark.stop()