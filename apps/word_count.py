import time
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Word Count test"
).config(
    "spark.executor.instances", "3"
).config(
    "spark.executor.cores", "2"
).getOrCreate()
sc = spark.sparkContext

# A=2, B=1, C=5, D=2, E=2
rdd = sc.textFile("./data/names.txt", 6)
df = rdd.map(lambda name: (name[0], name)).toDF()

df.collect()
df.foreachPartition(lambda rows: print_partition(rows))

rdd = df.rdd.groupByKey(numPartitions=6)
rdd.foreachPartition(lambda rows: print_partition_2(rows))

rdd = rdd.mapValues(lambda names: len(set(names)))
rdd.collect()

time.sleep(60)

# from operator import add
# rdd = sc.textFile("./data/names.txt", 6)
# df = rdd.map(lambda name: (name[0], 1)).toDF()
# df.rdd.reduceByKey(add).collect()


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
