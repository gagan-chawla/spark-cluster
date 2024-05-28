import time
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField


def print_partition(rows):
    sys.stdout.write("\nPrinting Partition v1\n")
    # print("Printing Partition")
    for row in rows:
        # print(row)
        sys.stdout.write(f"{row}\n")
    sys.stdout.flush()


def print_partition_2(rows):
    sys.stdout.write("\nPrinting Partition v2\n")
    # print("Printing Partition")
    for row in rows:
        # print(row)
        sys.stdout.write(f"{row[0]} --> {row[1].data}\n")
    sys.stdout.flush()


spark = SparkSession.builder.appName(
    "Word Count test"
).config(
    "spark.executor.instances", "3"
).config(
    "spark.executor.cores", "2"
).getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("./data/names.txt", 6)
df = rdd.map(lambda name: (name[0], name)).toDF(
    schema=StructType([
        StructField("letter", StringType(), True),
        StructField("name", StringType(), True),
    ])
)

# So that the df is not recomputed everytime we use foreachPartition for testing purposes
df.persist(storageLevel=StorageLevel.MEMORY_ONLY)
df.foreachPartition(lambda rows: print_partition(rows))

rdd = df.rdd.groupByKey(numPartitions=6)
rdd.cache()

rdd.foreachPartition(lambda rows: print_partition_2(rows))

rdd = rdd.mapValues(lambda names: len(set(names)))
rdd.collect()  # A=2, B=1, C=5, D=2, E=2

# To keep sparkSession alive for just a bit longer in order to check spark UI at 4040 port.
time.sleep(60)

# from operator import add
# rdd = sc.textFile("./data/names.txt", 6)
# df = rdd.map(lambda name: (name[0], 1)).toDF()
# df.rdd.reduceByKey(add).collect()
