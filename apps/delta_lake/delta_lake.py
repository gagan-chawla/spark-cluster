"""
pyspark --packages io.delta:delta-spark_2.12:3.2.0 --packages org.apache.hadoop:hadoop-aws:3.3.4 --packages=com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
"""
import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import expr, col

builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
# builder = pyspark.sql.SparkSession.builder.appName("DeltaApp")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

data = spark.range(1, 6, numPartitions=5)
# data = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ['id'])

data.write.format("delta").save("./apps/delta_lake/delta-table")

# Check what difference modes does
# data = spark.range(5, 10, numPartitions=1)
# data.write.format("delta").mode("overwrite").save("./data/delta-table")
# df = spark.read.format("delta").load("./data/delta-table")
# df.show()
# Only new data is shown because it was overwritten

# Delta Lake provides programmatic APIs to conditional update, delete, and merge (upsert) data into tables.
deltaTable = DeltaTable.forPath(spark, "./apps/delta_lake/delta-table")
deltaTable.toDF().show()

# # Update value 5 to 105
deltaTable.update(
  condition=expr("id == 5"),
  set={
    "id": expr("id + 100")
  }
)

# Delete every even value
deltaTable.delete(condition=expr("id % 9 == 0"))

# Upsert (merge) new data
newData = spark.createDataFrame([(6, 60), (7, 70), (8, 80), (9, 90)], ['id', 'number'])
newData = newData.repartition(2)

deltaTable.alias("oldData").merge(
    newData.alias("newData"),
    "oldData.id = newData.id"
).whenMatchedUpdate(
  set={"id": col("newData.number")}
).whenNotMatchedInsert(
  values={"id": col("newData.id")}
).execute()

deltaTable.toDF().show()

# Read older versions of data using time travel
df = spark.read.format("delta").option("versionAsOf", 0).load("./apps/delta_lake/delta-table")
# df = spark.read.format("delta").load("./data/delta-table@v0")
# deltaLog.getSnapshotAt(0)

# If using timestamp
# df = spark.read.format("delta").option("timestampAsOf", "1492-10-30").load("./data/delta-table")
df.show()
