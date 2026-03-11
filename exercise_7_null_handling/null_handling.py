from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Exercise7_NullHandling") \
    .getOrCreate()

df = spark.read.csv(
    "exercise_7_null_handling/data/employees_nulls.csv",
    header=True,
    inferSchema=True
)

print("Original Dataset")
df.show()

# Step 1: Remove rows with null salary
df_clean = df.dropna(subset=["salary"])

# Step 2: Fill null age with 0
df_clean = df_clean.fillna({"age": 0})

print("Final Cleaned Dataset")
df_clean.show()

spark.stop()