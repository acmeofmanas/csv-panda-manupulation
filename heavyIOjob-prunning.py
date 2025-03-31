#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when, col, expr
import time

# Initialize Spark Session with optimized configurations
spark = SparkSession.builder \
    .appName("HeavyIOJobWithPartitionPruning") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.optimizer.metadataOnly", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# HDFS paths
hdfs_namenode = "hdfs://namenode:9000"
input_path = f"{hdfs_namenode}/data/input"
output_path = f"{hdfs_namenode}/data/output"
temp_path = f"{hdfs_namenode}/data/temp"

def generate_partitioned_data():
    """Generate partitioned dataset with multiple columns"""
    start_time = time.time()
    
    # Create DataFrame with 100 million rows and multiple partitions
    df = spark.range(0, 100000000, numPartitions=1000)
    
    # Add partitioned column and data columns
    df = df.withColumn("category", when(rand() > 0.5, "A").otherwise("B")) \
           .withColumn("date", expr("date_add(current_date(), cast(rand()*365 as int))")) \
           .withColumn("value1", rand() * 100) \
           .withColumn("value2", rand() * 1000) \
           .repartition(200, "category", "date")
    
    # Write partitioned data in Parquet format
    df.write.partitionBy("category", "date") \
       .mode("overwrite") \
       .parquet(input_path)
    
    print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
    return df.count()

def process_with_pruning():
    """Process data with partition pruning and predicate pushdown"""
    start_time = time.time()
    
    # Read partitioned data with partition filters
    partitioned_df = spark.read.parquet(input_path) \
        .filter((col("category") == "A") & 
                (col("date") > "2024-01-01"))
    
    # Perform aggregation on filtered partitions
    agg_results = partitioned_df.groupBy("date") \
        .agg({"value1": "avg", "value2": "max"}) \
        .withColumnRenamed("avg(value1)", "daily_avg") \
        .withColumnRenamed("max(value2)", "daily_max")
    
    # Write results with dynamic partition overwrite
    agg_results.write.mode("overwrite") \
        .partitionBy("date") \
        .parquet(output_path + "/daily_metrics")
    
    # Create temporary view for SQL-based pruning
    partitioned_df.createOrReplaceTempView("partitioned_data")
    
    # Execute SQL query with partition pruning
    sql_result = spark.sql("""
        SELECT date, COUNT(*) AS total_transactions
        FROM partitioned_data
        WHERE category = 'B' 
          AND date BETWEEN '2024-01-01' AND '2024-01-31'
        GROUP BY date
    """)
    
    # Write SQL results
    sql_result.write.mode("overwrite") \
        .parquet(output_path + "/monthly_summary")
    
    print(f"Processing with pruning completed in {time.time() - start_time:.2f} seconds")
    return sql_result.count()

def main():
    try:
        # Generate partitioned data
        gen_count = generate_partitioned_data()
        print(f"Generated {gen_count} records")
        
        # Process with partition pruning
        proc_count = process_with_pruning()
        print(f"Processed {proc_count} records")
        
    finally:
        # Cleanup temporary data
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(temp_path), True)
        
        spark.stop()

if __name__ == "__main__":
    main()
