from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("SchemaExample") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.csv(
    "excercise_1_spark_pipeline/data/employee.csv",
    header=True,
    schema=schema
)

print("Dataset with Defined Schema")
df.printSchema()

df.show()

spark.stop()