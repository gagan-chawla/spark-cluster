import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Word Count test"
).config(
    "spark.executor.instances", "3"
).config(
    "spark.executor.cores", "2"
).getOrCreate()
sc = spark.sparkContext

##
# A - 2
# B - 1
# C - 5
# D - 2
# E - 2


#3
# E, C

#4
# A
# D
# B, C

#5
# D
# E, C

# result
# A
# D
# B, C, E
# Null
# Null

df = sc.textFile("./data/names.txt", 3)
df.collect()
# check workers for shuffle files - should be 0

df.foreachPartition(lambda rows: (print(r) for r in rows))

df = df.map(lambda name: (name[0], name))
# same here. Pipelined

df = df.groupByKey(numPartitions=6)
df.collect()
df.foreachPartition(lambda rows: (print(r) for r in rows))

df = df.mapValues(lambda names: len(set(names)))
df.collect()

time.sleep(60)
