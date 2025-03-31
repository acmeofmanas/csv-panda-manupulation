from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, row_number, avg, sum
from pyspark.sql.window import Window
import random

spark = SparkSession.builder \
    .appName("HeavyComputeHiveHDFS") \
    .enableHiveSupport() \
    .getOrCreate()

hdfs_base_path = "hdfs:///user/data/performance_test/"
hive_db = "test_db"
parquet_path = f"{hdfs_base_path}data.parquet"
orc_path = f"{hdfs_base_path}data.orc"
csv_path = f"{hdfs_base_path}data.csv"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
spark.sql(f"USE {hive_db}")

num_rows = 100000000  # 100 million rows
num_partitions = 500  # High partition count for distributed processing

rdd = spark.sparkContext.parallelize(range(num_rows), num_partitions)
data_df = rdd.map(lambda x: (x, random.randint(1, 10000), random.uniform(0, 1000), chr(65 + (x % 26)))) \
    .toDF(["id", "category", "value", "group"])

# Repartition to force shuffle and increase compute load
data_df = data_df.repartition(200)
data_df.cache()

# Write data in multiple formats
data_df.write.mode("overwrite").option("header", "true").parquet(parquet_path)
data_df.write.mode("overwrite").orc(orc_path)
data_df.write.mode("overwrite").option("header", "true").csv(csv_path)

# Read data back and apply transformations
parquet_df = spark.read.parquet(parquet_path)
orc_df = spark.read.orc(orc_path)

# Complex aggregation & window function
window_spec = Window.partitionBy("group").orderBy(col("value").desc())
transformed_df = parquet_df.withColumn("rank", row_number().over(window_spec)) \
    .groupBy("group").agg(avg("value").alias("avg_value"), sum("value").alias("total_value"))

# Write transformed results
transformed_df.write.mode("overwrite").parquet(f"{hdfs_base_path}output_grouped.parquet")

print("Heavy Compute Job Completed Successfully")
spark.stop()
