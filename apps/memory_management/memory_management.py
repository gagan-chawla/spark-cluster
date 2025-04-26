import os
import psutil
from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

# spark = SparkSession.builder.appName("Memory Management").config("spark.executor.memory", "6G").config("spark.executor.instances", "1").config("spark.executor.cores", "1").config("spark.task.maxFailures", 1).config("spark.python.worker.memory", "3G").config("spark.driver.memory", "6G").config("spark.driver.maxResultSize", "6G").config("spark.executor.memoryOverhead", "2G").getOrCreate()
# spark = SparkSession.builder.appName("Memory Management").config("spark.executor.memory", "3G").config("spark.executor.pyspark.memory", "2G").config("spark.executor.instances", "1").config("spark.executor.cores", "1").getOrCreate()
spark = SparkSession.builder.appName("Memory Management").config("spark.executor.memory", "1G").config("spark.executor.pyspark.memory", "1G").config("spark.executor.instances", "1").config("spark.executor.cores", "1").getOrCreate()
sc = spark.sparkContext
# check python.worker.memory too via spark submit. Some configs like such dont take effect in regular config like above.


def comprehensive_jvm_memory_check():
    # Java Runtime Memory
    runtime = spark._jvm.java.lang.Runtime.getRuntime()

    print("JVM RUNTIME MEMORY:")
    print(f"Total Memory: {runtime.totalMemory() / (1024 * 1024):.2f} MB")
    print(f"Free Memory: {runtime.freeMemory() / (1024 * 1024):.2f} MB")
    print(f"Max Memory: {runtime.maxMemory() / (1024 * 1024):.2f} MB")

    # Memory Management Bean
    memory_mxbean = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean()

    # Heap Memory
    heap_memory = memory_mxbean.getHeapMemoryUsage()
    print("\nHEAP MEMORY:")
    print(f"Heap Used: {heap_memory.getUsed() / (1024 * 1024):.2f} MB")
    print(f"Heap Committed: {heap_memory.getCommitted() / (1024 * 1024):.2f} MB")
    print(f"Heap Max: {heap_memory.getMax() / (1024 * 1024):.2f} MB")

    # Non-Heap Memory
    non_heap_memory = memory_mxbean.getNonHeapMemoryUsage()
    print("\nNON-HEAP MEMORY:")
    print(f"Non-Heap Used: {non_heap_memory.getUsed() / (1024 * 1024):.2f} MB")
    print(f"Non-Heap Committed: {non_heap_memory.getCommitted() / (1024 * 1024):.2f} MB")
    print(f"Non-Heap Max: {non_heap_memory.getMax() / (1024 * 1024):.2f} MB")


def print_system_wide_memory():
    virtual_memory = psutil.virtual_memory()
    print("\nSystem Memory:")
    print(f"Total: {virtual_memory.total / (1024 * 1024):.2f} MB")
    print(f"Available: {virtual_memory.available / (1024 * 1024):.2f} MB")
    print(f"Used: {virtual_memory.used / (1024 * 1024):.2f} MB")
    print(f"Percentage Used: {virtual_memory.percent}%")


def allocate_memory():
    # Create a list of large strings to consume memory
    memory_hog = []
    while True:
        # Allocate large chunks of memory
        for i in range(10):
            memory_hog.append('x' * 1024 * 1024)  # 1MB chunks

        # Check current memory usage
        process = psutil.Process(os.getpid())
        print(f"Current memory usage: {process.memory_info().rss / (1024 * 1024):.2f} MB")
        # Optional: Add a condition to stop or limit memory allocation
        if len(memory_hog) > 1000:  # Roughly 1GB
            print("Memory reached 1GB limit, stopping allocation.")
            break
    return memory_hog


def process_partition(index: int, rows: Iterable[Row]):
    print_system_wide_memory()

    print(f"Partition: {index}. Starting allocate_memory")
    result = allocate_memory()
    print("Done allocate_memory")

    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    print("\nProcess Memory Details:")
    print(f"RSS (Resident Set Size): {memory_info.rss / (1024 * 1024):.2f} MB")
    print(f"VMS (Virtual Memory Size): {memory_info.vms / (1024 * 1024):.2f} MB")

    print_system_wide_memory()

    print("LENGTH OF RESULT:", len(result))
    for i in result:
        yield {
            "result": i
        }


id_df = spark.range(1, 3, numPartitions=1)

# num_rows = 250 * 1024  # Approximately 1GB
# id_df = spark.range(
#     0, num_rows, numPartitions=1
# ).withColumn("large_string",
#     concat(
#         lit("large_data_"),
#         rand().cast("string")
#     )
# )

result_rdd = id_df.rdd.mapPartitionsWithIndex(process_partition)

print("JVM Memory Configuration:")
print("Executor Memory:", spark.conf.get("spark.executor.memory"))


# def count_rows(index: int, rows: Iterable[Row]):
#     count = 0
#     for _ in rows:
#         count += 1
#     print("Returning count", count)
#     return [
#         {
#             "count": count
#         }
#     ]
# result_rdd = result_rdd.mapPartitionsWithIndex(count_rows)
# result_rdd = result_rdd.toDF().dropDuplicates().rdd
print("----- Collecting RDD", len(result_rdd.collect()), " -----")


# Call the function
comprehensive_jvm_memory_check()
