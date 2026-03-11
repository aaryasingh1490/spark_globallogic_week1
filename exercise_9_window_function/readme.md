# Objetive

This exercise demonstrates the use of window functions in PySpark. Window functions allow analytical computations across groups of rows while preserving individual rows in the dataset.

In this example, employees are ranked based on their salary within each department.

## Steps to run

spark-submit exercise_9_window_functions/window_functions.py

# Learnings

- Understanding window functions in Spark
- Using Window.partitionBy() to define logical groups
- Ordering rows within partitions
- Ranking rows using rank()

# Observations

Window functions are widely used in analytical workloads, such as ranking, running totals, and top-N queries. They are especially useful when performing advanced data analysis within grouped data.