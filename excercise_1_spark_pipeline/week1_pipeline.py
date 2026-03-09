from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create Spark session
spark = SparkSession.builder.appName("Week1Pipeline").getOrCreate()

# Read CSV
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

print("\n================ ORIGINAL DATASET ================\n")
df.show()

# Filter dataset
filtered_df = df.filter(df.age > 22)

print("\n================ FILTERED DATASET (Age > 22) ================\n")
filtered_df.show()

# Aggregation
agg_df = filtered_df.groupBy("department").agg(avg("salary"))

print("\n================ AVERAGE SALARY BY DEPARTMENT ================\n")
agg_df.show()

# Write to Parquet
agg_df.write.mode("overwrite").parquet("output/department_salary")

print("\n================ PARQUET FILE WRITTEN SUCCESSFULLY ================\n")

spark.stop()