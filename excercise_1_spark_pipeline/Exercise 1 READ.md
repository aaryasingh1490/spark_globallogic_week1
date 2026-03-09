#objective
The goal of the excercise was to build a Spark pipeline that:
1. Reads a CSV dataset
2. Applies transformations and filtering
3. Performs aggregation
4. Writes the processed output in Parquet format

This exercise helps demonstrate how Spark is commonly used in data pipelines to process raw data and convert it into optimized storage formats(PARQUET).

#data
We created a small dataset Employee.csv. 
Columns in the dataset:
- id
- name
- department
- salary
- age

#operations
then we perfomred the following operations:
1. Read the CSV file using {spark.read.csv()}
2. Filtering using {age>22}
3. Aggregation- average salary by department using {groupBy("department").avg(salary)}
4. converted to parquest form using {df.write.parquest()}

#running the code
1. cd spark-week1  (project folder)
2. spark-submit exercise_1_spark_pipeline/week1_pipeline.py  (running the code)
3. the output will be shown in: exercise_1_spark_pipeline/output/
The output is stored in Parquet format, which is commonly used in data engineering pipelines due to its efficient columnar storage.


#errors faced
-> missing .jar file
-> git repository sync issue:
Issue:
Git push failed due to divergent branches and remote repository conflicts.

Solution:
Resolved by pulling the remote repository with: git pull origin main --allow-unrelated-histories
and resolving merge conflicts before pushing changes again.



