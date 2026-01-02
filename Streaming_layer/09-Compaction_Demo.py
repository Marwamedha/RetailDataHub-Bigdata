from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("Compaction") \
    .getOrCreate()

path = "hdfs:///logv2/playground/log_Events/"
temp_output_path = "hdfs:///logv2/playground/temp_log_Events_combined/"

log_events_schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("totalAmount", FloatType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("recommendedProductId", StringType(), True),
    StructField("algorithm", StringType(), True),
    StructField("category", StringType(), True),
    StructField("source", StringType(), True)
])

df = spark.read.csv(path + "*.csv", schema=log_events_schema, header=True)
df = df.coalesce(1)

# write the combined data as a single CSV file to temp dir
df.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(temp_output_path)


sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

# move the combined CSV file to the original dir

temp_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(temp_output_path))
temp_file_path = [file.getPath().toString() for file in temp_files if file.getPath().getName().startswith("part-")][0]
fs.rename(sc._jvm.org.apache.hadoop.fs.Path(temp_file_path), sc._jvm.org.apache.hadoop.fs.Path(path + "combined.csv"))


original_files = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(path))
for file in original_files:
    if file.getPath().getName().endswith(".csv") and file.getPath().getName() != "combined.csv":
        fs.delete(file.getPath(), True)

# Rename combined.csv to part-00000.csv
fs.rename(sc._jvm.org.apache.hadoop.fs.Path(path + "combined.csv"), sc._jvm.org.apache.hadoop.fs.Path(path + "part-00000.csv"))

# clean up the temp dir
fs.delete(sc._jvm.org.apache.hadoop.fs.Path(temp_output_path), True)

spark.stop()