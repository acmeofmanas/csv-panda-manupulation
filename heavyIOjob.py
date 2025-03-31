#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when, col
import time

# Initialize Spark Session for cluster mode
spark = SparkSession.builder \
    .appName("HeavyIOJob") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# HDFS paths
hdfs_namenode = "hdfs://namenode:9000"
input_path = f"{hdfs_namenode}/data/input"
output_path = f"{hdfs_namenode}/data/output"
temp_path = f"{hdfs_namenode}/data/temp"

def generate_large_data():
    """Generate large dataset and write to HDFS"""
    start_time = time.time()
    
    # Create DataFrame with 100 million rows
    df = spark.range(0, 100000000, numPartitions=1000)
    
    # Add multiple columns with random data
    df = df.withColumn("rand1", rand() * 100) \
           .withColumn("rand2", rand() * 1000) \
           .withColumn("category", when(rand() > 0.5, "A").otherwise("B")) \
           .repartition(200)
    
    # Write to HDFS in multiple formats
    df.write.mode("overwrite").parquet(input_path)
    df.write.mode("overwrite").csv(temp_path)
    
    print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
    return df.count()

def process_data():
    """Process data with multiple IO operations"""
    start_time = time.time()
    
    # Read from different sources
    parquet_df = spark.read.parquet(input_path)
    csv_df = spark.read.csv(temp_path)
    
    # Complex transformation 1: Aggregations
    agg_df = parquet_df.groupBy("category") \
        .agg({"rand1": "avg", "rand2": "max"}) \
        .withColumnRenamed("avg(rand1)", "avg_rand1") \
        .withColumnRenamed("max(rand2)", "max_rand2")
    
    # Complex transformation 2: Joins
    joined_df = parquet_df.join(csv_df, "id", "inner")
    
    # Write intermediate results
    joined_df.write.mode("overwrite").parquet(output_path + "/joined")
    agg_df.write.mode("overwrite").parquet(output_path + "/aggregated")
    
    # Trigger multiple write operations
    for i in range(3):
        joined_df.write.mode("overwrite").csv(output_path + f"/joined_csv_{i}")
    
    print(f"Data processing completed in {time.time() - start_time:.2f} seconds")
    return joined_df.count()

def main():
    try:
        # Generate initial data
        gen_count = generate_large_data()
        print(f"Generated {gen_count} records")
        
        # Process data
        proc_count = process_data()
        print(f"Processed {proc_count} records")
        
    finally:
        # Cleanup temporary data
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        ).delete(spark._jvm.org.apache.hadoop.fs.Path(temp_path), True)
        
        spark.stop()

if __name__ == "__main__":
    main()
