
"""Spark job to read sales.csv, drop duplicates, and load to MySQL"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("clean_sales").getOrCreate()
df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
deduped = df.dropDuplicates()
(
    deduped.write
    .format("jdbc")
    .option("url", f"jdbc:mysql://{os.getenv('DB_HOST', '127.0.0.1')}:3306/{os.getenv('DB_NAME', 'retail')}")
    .option("dbtable", "stg_sales_clean")
    .option("user", os.getenv('DB_USER', 'root'))
    .option("password", os.getenv('DB_PASSWORD', ''))
    .mode("overwrite")
    .save()
)
spark.stop()
