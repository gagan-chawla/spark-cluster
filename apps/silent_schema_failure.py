from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, LongType
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Silent Schema Failure Test").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("./data/numbers.txt")
df = rdd.toDF(schema=StringType())
df.show()

print("StringType")
df.select(col("value").cast(StringType())).show()

print("IntegerType")
df.select(col("value").cast(IntegerType())).show()

print("LongType")
df.select(col("value").cast(LongType())).show()
