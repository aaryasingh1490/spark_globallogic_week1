#objective
This exercise demonstrates how Spark can read and write data in different file formats.  
The employee dataset is read from CSV and written into JSON and Parquet formats.

#running the code
1. go to the project folder
   cd spark-week1

2. running the code
   spark-submit exercise_3_file_formats/file_format_conversion.py

3. outputs will be created in 2 files:
   exercise_3_file_formats/output_json
   exercise_3_file_formats/output_parquet

#Learnings
- Reading CSV datasets with Spark
- Writing data in JSON format
- Writing data in Parquet format
- Understanding differences between row-based and columnar storage formats

#error faced
Issue: Dataset path mismatch  
Resolution: Updated the correct dataset path in the script.
