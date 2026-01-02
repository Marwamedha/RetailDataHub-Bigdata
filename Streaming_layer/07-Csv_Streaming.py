from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "Diaa_topic"
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Define schema for the incoming JSON data
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

# Read data from Kafka topic as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Parse the JSON data and apply schema
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Handle potentially missing or empty metadata fields
transformed_df = json_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("metadata_category", col("metadata.category").cast(StringType())) \
    .withColumn("metadata_source", col("metadata.source").cast(StringType())) \
    .drop("metadata")

# Write the transformed data to CSV files
query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/project/log_Events") \
    .option("checkpointLocation", "/project/streaming_checkpoint/") \
    .start()

query.awaitTermination(20)
spark.stop()