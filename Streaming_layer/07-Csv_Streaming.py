import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, current_date, date_format
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField

# === Spark Session ===
spark = SparkSession.builder \
    .appName("RetailDataHub_Kafka_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

# === Kafka Configuration (use environment variables for security) ===
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pkc-56d1g.eastus.azure.confluent.cloud:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "Diaa_topic")
kafka_username = os.getenv("KAFKA_USERNAME")
kafka_password = os.getenv("KAFKA_PASSWORD")

# === Define schema for incoming JSON ===
schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metadata", StructType([
        StructField("category", StringType(), True),
        StructField("source", StringType(), True)
    ]), True),
    StructField("quantity", IntegerType(), True),
    StructField("totalAmount", FloatType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("recommendedProductId", StringType(), True),
    StructField("algorithm", StringType(), True)
])

# === Read streaming data from Kafka ===
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";'
    ) \
    .load()

# === Parse JSON and flatten schema ===
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# === Transform data ===
transformed_df = json_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("metadata_category", col("metadata.category").cast(StringType())) \
    .withColumn("metadata_source", col("metadata.source").cast(StringType())) \
    .drop("metadata") \
    .filter((col("quantity") >= 0) & (col("totalAmount") >= 0)) \
    .withColumn("event_date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .withColumn("event_hour", date_format(col("timestamp"), "HH"))

# === Write streaming data to HDFS Delta (partitioned by date/hour) ===
query = transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "/project/log_Events_delta") \
    .option("checkpointLocation", "/project/streaming_checkpoint_delta/") \
    .partitionBy("event_date", "event_hour") \
    .start()

# === Continuous streaming (production-ready) ===
query.awaitTermination()
