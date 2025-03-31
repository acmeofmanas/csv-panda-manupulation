from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveMetastoreTest") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a managed table in Hive
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("USE test_db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS test_table (
        id INT,
        name STRING,
        value DOUBLE
    ) STORED AS PARQUET
""")

# Load metadata timing
import time
start_time = time.time()
spark.sql("MSCK REPAIR TABLE test_table")  # Load metadata
end_time = time.time()

print(f"Metadata Load Time: {end_time - start_time:.2f} seconds")

spark.stop()

##
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveQueryPerformance") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE test_db")

# Measure query execution time
import time
start_time = time.time()
spark.sql("SELECT COUNT(*) FROM test_table").show()
end_time = time.time()

print(f"Query Execution Time: {end_time - start_time:.2f} seconds")

spark.stop()
##
from pyspark.sql import SparkSession
import random

spark = SparkSession.builder \
    .appName("HDFSPerformanceTest") \
    .getOrCreate()

# Generate large dataset
data = [(i, random.randint(1, 1000), random.uniform(0, 100)) for i in range(10000000)]
df = spark.createDataFrame(data, ["id", "category", "value"])

hdfs_path = "hdfs:///user/data/performance_test.parquet"

# Write to HDFS and measure time
import time
start_time = time.time()
df.write.mode("overwrite").parquet(hdfs_path)
end_time = time.time()

print(f"HDFS Write Time: {end_time - start_time:.2f} seconds")

# Read from HDFS and measure time
start_time = time.time()
df_read = spark.read.parquet(hdfs_path)
df_read.count()  # Trigger read action
end_time = time.time()

print(f"HDFS Read Time: {end_time - start_time:.2f} seconds")

spark.stop()
##
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PartitionPruningTest") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE test_db")

# Create a partitioned table
spark.sql("""
    CREATE TABLE IF NOT EXISTS partitioned_table (
        id INT,
        value DOUBLE
    ) PARTITIONED BY (category INT)
    STORED AS PARQUET
""")

# Insert sample data
spark.sql("INSERT INTO partitioned_table PARTITION (category=1) VALUES (1, 100.0)")
spark.sql("INSERT INTO partitioned_table PARTITION (category=2) VALUES (2, 200.0)")

# Test partition pruning
import time
start_time = time.time()
spark.sql("SELECT * FROM partitioned_table WHERE category=1").show()
end_time = time.time()

print(f"Partition Pruning Query Time: {end_time - start_time:.2f} seconds")

spark.stop()
##
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SmallFilesTest") \
    .getOrCreate()

hdfs_base_path = "hdfs:///user/data/small_files/"

for i in range(1000):
    df = spark.range(100).toDF("id")
    df.write.mode("overwrite").parquet(f"{hdfs_base_path}file_{i}.parquet")

spark.stop()
##
