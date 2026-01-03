#!/usr/bin/env python3

import re
import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError


PROJECT_RAW_PATH = "/project/raw_data"
HIVE_DB = "retail_dw"
DATE_DIM_BOOTSTRAP_PATH = "file:///data/RetailDataHub-Bigdata/batch_layer/dimensions/date_dim.csv"


def create_spark():
    return SparkSession.builder \
        .appName("Retail_Batch_Layer") \
        .enableHiveSupport() \
        .getOrCreate()


def main(day=None, hour=None):

    spark = create_spark()
    spark.sql(f"USE {HIVE_DB}")

    if day and hour:
        full_date, hour = day, hour
    else:
        full_date = datetime.now().strftime('%Y_%m_%d')
        hour = datetime.now().strftime('%H')

    raw_path = f"{PROJECT_RAW_PATH}/{full_date}/{hour}"

    # Validate HDFS path
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    try:
        fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(raw_path))
    except Py4JJavaError:
        print(f"[ERROR] Raw data not found: {raw_path}")
        sys.exit(1)

    # Load Date Dimension once
    if not spark.catalog.tableExists("date_dim"):
        spark.read.csv(DATE_DIM_BOOTSTRAP_PATH, header=True, inferSchema=True) \
            .write.mode("overwrite").format("orc").saveAsTable("date_dim")

    # === Schemas ===
    sales_schema = StructType([
        StructField("transaction_date", DateType()),
        StructField("transaction_id", StringType()),
        StructField("customer_id", IntegerType()),
        StructField("customer_fname", StringType()),
        StructField("cusomter_lname", StringType()),
        StructField("cusomter_email", StringType()),
        StructField("sales_agent_id", IntegerType()),
        StructField("branch_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("product_category", StringType()),
        StructField("offer_1", BooleanType()),
        StructField("offer_2", BooleanType()),
        StructField("offer_3", BooleanType()),
        StructField("offer_4", BooleanType()),
        StructField("offer_5", BooleanType()),
        StructField("units", IntegerType()),
        StructField("unit_price", FloatType()),
        StructField("is_online", StringType()),
        StructField("payment_method", StringType()),
        StructField("shipping_address", StringType())
    ])

    sales_df = spark.read.csv(f"{raw_path}/sales_transactions*.csv",
                              header=True, schema=sales_schema)

    # === Transformations ===
    sales_df = sales_df.withColumn(
        "discount",
        greatest(
            when(col("offer_1"), 5),
            when(col("offer_2"), 10),
            when(col("offer_3"), 15),
            when(col("offer_4"), 20),
            when(col("offer_5"), 25),
            lit(0)
        )
    ).withColumn(
        "total_price",
        col("units") * col("unit_price") * (1 - col("discount") / 100)
    )

    sales_df = sales_df.withColumn("addr", split(col("shipping_address"), "/")) \
        .withColumn("street", col("addr")[0]) \
        .withColumn("city", col("addr")[1]) \
        .withColumn("state", col("addr")[2]) \
        .withColumn("postal_code", col("addr")[3]) \
        .drop("addr")

    sales_df.write.mode("append").format("orc").insertInto("sales_fact")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day")
    parser.add_argument("--hour")
    args = parser.parse_args()

    if (args.day is None) ^ (args.hour is None):
        print("Both --day and --hour must be provided together")
        sys.exit(1)

    main(args.day, args.hour)
