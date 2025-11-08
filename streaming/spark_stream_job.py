
"""Spark Structured Streaming Job: Read events from Kafka and write to MySQL"""
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, DecimalType

load_dotenv()

spark = SparkSession.builder \
    .appName("stream_sales") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.0.33") \
    .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("store_id", IntegerType()),
    StructField("dept_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("payment_method", StringType()),
    StructField("promotion_applied", BooleanType())
])

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    .option("subscribe", os.getenv("KAFKA_TOPIC", "sales_events"))
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("event_time").cast(TimestampType()))
)

def write_to_mysql(df, epoch_id):
    print(f"Writing batch {epoch_id} with {df.count()} records")
    
    # Remove duplicates within the current batch based on event_id
    df_deduped = df.dropDuplicates(["event_id"])
    deduped_count = df_deduped.count()
    print(f"After deduplication: {deduped_count} records (removed {df.count() - deduped_count} duplicates)")
    
    try:
        df_deduped.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{os.getenv('DB_HOST', '127.0.0.1')}:3306/{os.getenv('DB_NAME', 'retail_analytics')}") \
            .option("dbtable", "stream_sales_events") \
            .option("user", os.getenv("DB_USER", "root")) \
            .option("password", os.getenv("DB_PASSWORD", "")) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {epoch_id}")
    except Exception as e:
        if "Duplicate entry" in str(e):
            print(f"Batch {epoch_id}: Skipped duplicates that already exist in database")
        else:
            raise e

(
    parsed.writeStream
    .foreachBatch(write_to_mysql)
    .option("checkpointLocation", "/tmp/spark_checkpoint/stream_sales")
    .start()
    .awaitTermination()
)
