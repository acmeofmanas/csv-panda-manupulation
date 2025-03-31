from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import random

# Initialize Spark session for cluster mode
spark = SparkSession.builder \
    .appName("HeavyIOJob") \
    .getOrCreate()

# HDFS Path
hdfs_path_csv = "hdfs:///user/data/synthetic_data.csv"
hdfs_path_parquet = "hdfs:///user/data/synthetic_data.parquet"

# Generate Large Synthetic Data
def generate_data(num_rows=10000000, num_partitions=100):
    rdd = spark.sparkContext.parallelize(range(num_rows), num_partitions)
    df = rdd.map(lambda x: (x, random.randint(1, 1000), random.uniform(0, 100), chr(65 + (x % 26)))) \
        .toDF(["id", "category", "value", "group"])
    return df

# Create DataFrame
data_df = generate_data()

# Write Data to HDFS (CSV & Parquet)
data_df.write.mode("overwrite").option("header", "true").csv(hdfs_path_csv)
data_df.write.mode("overwrite").parquet(hdfs_path_parquet)

# Read the Data Back (I/O Intensive Operation)
csv_df = spark.read.option("header", "true").csv(hdfs_path_csv, inferSchema=True)
parquet_df = spark.read.parquet(hdfs_path_parquet)

# Perform Heavy Aggregation
grouped_df = parquet_df.groupBy("group").sum("value")

# Trigger an expensive action
grouped_df.write.mode("overwrite").parquet("hdfs:///user/data/output_grouped.parquet")

print("Job Completed Successfully")

# Stop Spark Session
spark.stop()
