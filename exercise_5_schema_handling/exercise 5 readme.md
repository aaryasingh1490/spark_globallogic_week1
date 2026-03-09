# objective
In this exercise we define a schema while reading data into a Spark DataFrame. Instead of relying on automatic schema inference, we manually define the data types for each column.

# steps to run

1. Run the code in the root directory
spark-submit exercise_5_schema/schema_example.py

# Learnings

- Understanding Spark DataFrame schemas
- Difference between schema inference and manual schema definition
- Using StructType and StructField
- Controlling data types when reading CSV files
- Improving performance and reliability by defining schemas explicitly

# Observations
Defining a schema manually ensures that Spark reads the dataset with the correct data types. This avoids issues where Spark incorrectly infers data types and also improves performance in large datasets.