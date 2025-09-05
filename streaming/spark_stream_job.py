
"""Spark Structured Streaming Job: Read events from Kafka and write to MySQL"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, DecimalType

spark = SparkSession.builder.appName("stream_sales").getOrCreate()

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
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales_events")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("event_time").cast(TimestampType()))
)

def write_to_mysql(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://127.0.0.1:3306/retail_analytics") \
        .option("dbtable", "stream_sales_events") \
        .option("user", "root") \
        .option("password", "Alex@12345") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

(
    parsed.writeStream
    .foreachBatch(write_to_mysql)
    .option("checkpointLocation", "/tmp/spark_checkpoint/stream_sales")
    .start()
    .awaitTermination()
)
