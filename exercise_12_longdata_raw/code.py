from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, expr, avg, rank, current_date
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("EndToEndDataPipeline") \
    .getOrCreate()


# STEP 1: READ RAW DATA

raw_df = spark.read.csv(
    "exercise_12_longdata_raw/data/raw_employee.csv",
    header=True,
    inferSchema=False   # important for raw ingestion
)

departments = spark.read.csv(
    "exercise_12_longdata_raw/data/departments.csv",
    header=True,
    inferSchema=True
)

print("RAW DATA")
raw_df.show()


# STEP 2: BRONZE LAYER (SAFE INGESTION)

bronze_df = raw_df \
    .withColumn("department_id", expr("try_cast(department_id as int)")) \
    .withColumn("salary", expr("try_cast(salary as int)")) \
    .withColumn("age", expr("try_cast(age as int)")) \
    .withColumn("joining_date", expr("try_cast(joining_date as date)"))

print("BRONZE DATA")
bronze_df.show()

# write bronze
bronze_df.write \
    .mode("overwrite") \
    .partitionBy("department_id") \
    .parquet("exercise_12_longdata_raw/output/bronze")


# STEP 3: SILVER LAYER (CLEANING)

silver_df = bronze_df

# remove duplicates
silver_df = silver_df.dropDuplicates()

# trim names
silver_df = silver_df.withColumn("name", trim(col("name")))

# fill missing names
silver_df = silver_df.fillna({"name": "Unknown"})

# fix salary (remove negative)
silver_df = silver_df.withColumn(
    "salary",
    expr("CASE WHEN salary < 0 THEN NULL ELSE salary END")
)

# handle nulls
silver_df = silver_df.fillna({
    "salary": 0,
    "age": 0
})

# fix invalid dates
silver_df = silver_df.withColumn(
    "joining_date",
    expr("CASE WHEN joining_date IS NULL THEN current_date() ELSE joining_date END")
)

print("SILVER DATA")
silver_df.show()

# write silver
silver_df.write \
    .mode("overwrite") \
    .partitionBy("department_id") \
    .parquet("exercise_12_longdata_raw/output/silver")


# STEP 4: PREPARE DIMENSION TABLE (DEPARTMENTS)

departments = departments.withColumn(
    "department_id",
    expr("try_cast(department_id as int)")
)


# STEP 5: JOIN (FACT + DIMENSION)

final_df = silver_df.join(
    departments,
    "department_id",
    "left"
)

print("JOINED DATA")
final_df.show()


# STEP 6: ANALYTICS


# 1. Average salary by department
avg_salary = final_df.groupBy("department_name") \
    .agg(avg("salary").alias("avg_salary"))

print("AVERAGE SALARY")
avg_salary.show()

# 2. Salary ranking within department
windowSpec = Window.partitionBy("department_name").orderBy(col("salary").desc())

ranked_df = final_df.withColumn(
    "salary_rank",
    rank().over(windowSpec)
)

print("RANKED DATA")
ranked_df.show()


# STEP 7: WRITE FINAL OUTPUT

ranked_df.write \
    .mode("overwrite") \
    .partitionBy("department_name") \
    .parquet("exercise_12_longdata_raw/output/gold")

print("PIPELINE COMPLETED SUCCESSFULLY")

spark.stop()