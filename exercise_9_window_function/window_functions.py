from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

spark = SparkSession.builder \
    .appName("Exercise9_WindowFunctions") \
    .getOrCreate()

df = spark.read.csv(
    "exercise_9_window_function/data/employees.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Window specification
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Rank employees within each department
df_ranked = df.withColumn(
    "salary_rank",
    rank().over(window_spec)
)

print("Employees Ranked by Salary within Department")
df_ranked.show()

spark.stop()