from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercise6_Joins") \
    .getOrCreate()

# Read employees
employees = spark.read.csv(
    "exercise_6_join_operations/data/employees.csv",
    header=True,
    inferSchema=True
)

# Read departments
departments = spark.read.csv(
    "exercise_6_join_operations/data/departments.csv",
    header=True,
    inferSchema=True
)

print("Employees Table")
employees.show()

print("Departments Table")
departments.show()

# Inner Join
joined_df = employees.join(
    departments,
    employees.department_id == departments.department_id,
    "inner"
)

print("Joined Dataset")
joined_df.show()

spark.stop()