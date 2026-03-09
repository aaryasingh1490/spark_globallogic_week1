from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

# Create Spark session
spark = SparkSession.builder \
    .appName("AggregationsExample") \
    .getOrCreate()

# Read CSV dataset
df = spark.read.csv(
    "excercise_1_spark_pipeline/data/employee.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Average salary by department
print("Average Salary by Department")
df.groupBy("department").agg(
    avg("salary").alias("avg_salary")
).show()

# Maximum salary
print("Maximum Salary")
df.select(
    max("salary").alias("max_salary")
).show()

# Minimum salary
print("Minimum Salary")
df.select(
    min("salary").alias("min_salary")
).show()

spark.stop()