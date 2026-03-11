# obs

This exercise demonstrates how to write partitioned datasets using PySpark. Partitioning divides the dataset into multiple folders based on a column value, improving query performance and data organization.

## Steps to run

spark-submit exercise_10_partitioning/partition_write.py

## Key Learnings

- Writing data in Parquet format
- Partitioning datasets using partitionBy()
- Organizing large datasets efficiently
- Improving query performance with partitioned storage

## Observations

Partitioning is widely used in large-scale data pipelines to optimize data access and reduce query execution time. Spark automatically organizes the data into folders based on the partition column.