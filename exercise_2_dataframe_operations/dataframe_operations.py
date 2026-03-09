from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("DataFrameOperations") \
    .getOrCreate()

# Read CSV dataset
df = spark.read.csv(
    "excercise_1_spark_pipeline/data/employee.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Select specific columns
print("Selected Columns (Name, Department, Salary)")
df.select("name", "department", "salary").show()

# Filter rows
print("Employees with salary greater than 55000")
df.filter(col("salary") > 55000).show()

# Sort dataset
print("Employees sorted by salary (descending)")
df.orderBy(col("salary").desc()).show()

spark.stop()