# Employee Data Processing Pipeline (PySpark)
# Overview

This exercise implements a mini data engineering pipeline using PySpark to process employee data with multiple data quality issues. The goal was to simulate a real-world scenario where raw datasets often contain inconsistencies, invalid values, duplicates, and schema mismatches.

The pipeline reads raw CSV datasets, performs data cleaning and transformations, joins with a reference dataset, computes analytical metrics, and writes the final results to partitioned Parquet files.

This exercise helped reinforce core Spark concepts such as:

- DataFrame transformations
- Data cleaning
- Schema handling
- Joins
- Aggregations
- Window functions
- Partitioned data writing

# Dataset Description

Two datasets were used in this exercise.
Employee Dataset (Raw Data)
Contains employee-level information.

Columns:

id – Employee ID
name – Employee name
department_id – Department identifier
salary – Employee salary
age – Employee age

The dataset intentionally contained multiple data quality issues such as:
Duplicate records
Null values
Invalid salary values (unknown, not available)
Negative salary values
Missing names
Missing age values
Department IDs stored as decimals (101.0)
Leading/trailing spaces in names

Department Dataset
Contains department reference information.

Columns:
department_id
department_name

This dataset is used to enrich employee records using a join operation.

Pipeline Architecture

The pipeline follows a simplified ETL workflow:

Raw CSV Data

Data Cleaning & Validation

Schema Fixing

Join with Department Table

Aggregation & Analytics

Window Function Ranking

Partitioned Parquet Output

# Step 1: Spark Session Initialization
spark = SparkSession.builder \
    .appName("EmployeeDataPipeline") \
    .getOrCreate()
Why this is required

The SparkSession is the entry point for all Spark operations including reading data, transformations, and writing output.

# Step 2: Reading Raw Data
employees = spark.read.csv(
    "data/raw_employee.csv",
    header=True,
    inferSchema=True
)
Why we used inferSchema

inferSchema=True allows Spark to automatically detect column types rather than reading everything as strings.

However, this also introduced an issue:
department_id = 101.0
Spark interpreted department IDs as double values, which later caused a join mismatch.

# Step 3: Data Cleaning
Removing duplicate records
employees = employees.dropDuplicates()
Why this is important

Duplicate records can:
distort analytics
cause double counting
affect aggregation results

# Trimming names
employees = employees.withColumn(
    "name",
    trim(col("name"))
)
Why this is needed

Raw data often contains extra spaces, which can cause issues when performing:
joins
grouping
comparisons

Example:
" Neha "  →  "Neha"

# Filling missing names
employees = employees.fillna({"name": "Unknown"})
Why

Null names can break downstream processing and analytics.
Replacing them ensures data completeness.

# Step 4: Schema Fixing
Converting department_id
employees = employees.withColumn(
    "department_id",
    expr("try_cast(department_id as int)")
)
Why we used try_cast
Some values may fail during casting.

try_cast:
converts valid values
replaces invalid values with NULL
prevents pipeline crashes

# Fixing salary column
employees = employees.withColumn(
    "salary",
    expr("try_cast(salary as int)")
)

This handles invalid values like:
not available
unknown

These values become:
NULL

# Removing negative salary
employees = employees.withColumn(
    "salary",
    expr("CASE WHEN salary < 0 THEN NULL ELSE salary END")
)
Why
Negative salaries are invalid data.
They are replaced with NULL before final handling.

# Handling remaining nulls
employees = employees.fillna({
    "salary": 0,
    "age": 0
})

Final data guarantees:

salary is not NULL
age is not NULL

# Step 5: Joining Department Data
employee_full = employees.join(
    departments,
    "department_id",
    "left"
)
Why we used a LEFT JOIN

A left join ensures that:
All employees remain in the dataset
Department information is added where available

# Step 6: Aggregation
dept_salary = employee_full.groupBy("department_name") \
    .agg(avg("salary").alias("avg_salary"))

Purpose:
Calculate average salary per department.


Why we used window functions
Window functions allow calculations within partitions of data without collapsing rows.
Here we rank employees within each department based on salary.

# Step 8: Writing Partitioned Parquet Output
ranked.write \
    .mode("overwrite") \
    .partitionBy("department_name") \
    .parquet("output/employee_parquet")

Why Parquet
Parquet is a columnar storage format that provides:
better compression
faster query performance
efficient analytics

Why partitioning
Partitioning improves performance when filtering data.



# Issues Faced During Development
## Issue 1: Department names becoming NULL after join

Problem:
department_name = NULL

Cause:
department_id was stored as double (101.0) in employees but integer (101) in departments.
Spark join requires matching data types.

Solution:
Explicitly cast department_id to integer using:

expr("try_cast(department_id as int)")
Issue 2: trim() causing type mismatch

Earlier we attempted:

trim(col("department_id"))

This converted numeric values to strings like:

"101.0"

Casting "101.0" to integer failed and resulted in NULL.

Fix:
Removed the trim operation for numeric columns.

## Issue 2: Invalid Salary Values
Problem:
Some salary values were stored as strings such as:
not available
unknown

These values caused type conversion errors when Spark tried to treat the salary column as numeric.

Solution:

Use try_cast to safely convert values.

employees = employees.withColumn(
    "salary",
    expr("try_cast(salary as int)")
)

Invalid values are automatically converted to NULL.

## issue 3 :Window Function Ranking Confusion
Problem
While reviewing the ranking output, multiple employees received the same rank value.

This initially looked incorrect.

Cause
The rank() window function assigns the same rank to tied values.

Solution

Understanding the difference between ranking functions:
rank() → allows ties and skips numbers
dense_rank() → allows ties but does not skip numbers
row_number() → assigns unique sequential numbers

# Key Concepts Learned

This exercise reinforced several important PySpark concepts:
Data Cleaning

Handling:
duplicates
invalid values
missing values
inconsistent schemas
Schema Management

Understanding when to:
infer schema
explicitly cast columns
Data Transformations

Using:
withColumn
fillna
expr
dropDuplicates
Joins
Combining datasets using shared keys.
Aggregations
Using groupBy and avg() for analytical summaries.
Window Functions
Using rank() to perform calculations within partitions of data.

Partitioned Data Storage

Writing optimized analytics-ready datasets using Parquet with partitioning.

Conclusion

This exercise simulated a real-world data engineering workflow using PySpark. The pipeline successfully handled messy raw data, cleaned and transformed it, enriched it using reference data, and produced optimized analytical outputs.

# Key takeaways:

Raw data almost always contains inconsistencies.
Schema alignment is critical for joins.
Window functions provide powerful analytical capabilities.
Partitioned Parquet files improve performance for large datasets.
