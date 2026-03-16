from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, expr, avg, rank
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("EmployeeDataPipeline") \
    .getOrCreate()

# ---------------------------------------------------
# Read datasets
# ---------------------------------------------------

employees = spark.read.csv(
    "excercise_11/data/raw_employee.csv",
    header=True,
    inferSchema=True
)

departments = spark.read.csv(
    "excercise_11/data/departments.csv",
    header=True,
    inferSchema=True
)

departments = departments.withColumn(
    "department_id",
    expr("try_cast(department_id as int)")
)

print("RAW EMPLOYEE DATA")
employees.show()

# ---------------------------------------------------
# DATA CLEANING
# ---------------------------------------------------

# remove duplicates
employees = employees.dropDuplicates()

employees = employees.withColumn(
    "department_id",
    expr("try_cast(department_id as int)")
)

# fill missing names
employees = employees.fillna({"name": "Unknown"})

# standardize department id column
employees = employees.withColumn(
    "department_id",
    trim(col("department_id"))
)

# fix salary schema
employees = employees.withColumn(
    "salary",
    expr("try_cast(salary as int)")
)

# remove negative salary
employees = employees.withColumn(
    "salary",
    expr("CASE WHEN salary < 0 THEN NULL ELSE salary END")
)

# handle nulls
employees = employees.fillna({
    "salary": 0,
    "age": 0
})

print("CLEANED DATA")
employees.show()


# ---------------------------------------------------
# JOIN WITH DEPARTMENT TABLE
# ---------------------------------------------------

employee_full = employees.join(
    departments,
    "department_id",
    "left"
)

print("JOINED DATA")
employee_full.show()

# ---------------------------------------------------
# AGGREGATION
# ---------------------------------------------------

dept_salary = employee_full.groupBy("department_name") \
    .agg(avg("salary").alias("avg_salary"))

print("AVERAGE SALARY BY DEPARTMENT")
dept_salary.show()

# ---------------------------------------------------
# WINDOW FUNCTION (RANK SALARIES)
# ---------------------------------------------------

windowSpec = Window.partitionBy("department_name").orderBy(col("salary").desc())

ranked = employee_full.withColumn(
    "salary_rank",
    rank().over(windowSpec)
)

print("SALARY RANKING")
ranked.show()

# ---------------------------------------------------
# WRITE OUTPUT
# ---------------------------------------------------

ranked.write \
    .mode("overwrite") \
    .partitionBy("department_name") \
    .parquet("excercise_11/output/employee_parquet")

print("Pipeline completed successfully")

spark.stop()