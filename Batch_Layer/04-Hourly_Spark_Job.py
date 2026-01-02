#!/usr/bin/env python3

import re
import argparse
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from py4j.protocol import Py4JJavaError

def main(args):
    spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("ProcessingData")\
        .config("spark.eventLog.logBlockUpdates.enabled", True)\
        .enableHiveSupport()\
        .getOrCreate()

    sc = spark.sparkContext

    _ = spark.sql("USE qcompany")

    if(args.day):
        full_date = args.day
        hour = args.hour
    else:
        full_date = datetime.now().strftime('%Y_%m_%d')
        hour = datetime.now().strftime('%H')

    if(full_date == "2024_07_12" and hour == "01"):
        date_dim = spark.read.csv("file:///home/itversity/date_dim.csv", header=True, inferSchema=True)
        date_dim.write \
        .mode("overwrite") \
        .format("orc") \
        .saveAsTable("date_dim")
    else:
        date_dim = spark.table("date_dim").withColumn("full_date", to_date(col("full_date")))

    raw_path = "/project/raw_data/{}/{}".format(full_date, hour)

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(raw_path)
    try:
        file_status = hdfs.listStatus(path)
    except Py4JJavaError as e:
        if "FileNotFoundException" in str(e):
            print("File not found: {}".format(raw_path))
            sys.exit(1)

    file_list = ["{}/{}".format(raw_path, hdfs_object.getPath().getName()) for hdfs_object in file_status]

    sales_transactions_schema = StructType([
        StructField("transaction_date", DateType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("customer_fname", StringType(), True),
        StructField("cusomter_lname", StringType(), True),
        StructField("cusomter_email", StringType(), True),
        StructField("sales_agent_id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("offer_1", BooleanType(), True),
        StructField("offer_2", BooleanType(), True),
        StructField("offer_3", BooleanType(), True),
        StructField("offer_4", BooleanType(), True),
        StructField("offer_5", BooleanType(), True),
        StructField("units", IntegerType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("is_online", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True)
    ])

    sales_agents_schema = StructType([
        StructField("sales_person_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("hire_date", DateType(), True)
    ])

    branch_schema = StructType([
        StructField("branch_id", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("establish_date", DateType(), True),
        StructField("class", StringType(), True),
    ])

    for file in file_list:
        if "branches" in file:
            branches_df = spark.read.csv(file, header=True, schema=branch_schema)
        elif "sales_agents" in file:
            sales_agents_df = spark.read.csv(file, header=True, schema=sales_agents_schema)
        elif "sales_transactions" in file:
            sales_transactions_df = spark.read.csv(file, header=True, schema=sales_transactions_schema)

    def fixEmail(email):
        match_re = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.com", email)
        if match_re:
            return match_re.group()
        return None

    fixEmailUDF = udf(fixEmail, StringType())

    customer_df = sales_transactions_df.select(['customer_id', 
                                                concat(col('customer_fname'), lit(' '), col('cusomter_lname')).alias("customer_full_name"),
                                                fixEmailUDF(col('cusomter_email')).alias('email')]).distinct()

    product_df = sales_transactions_df.select("product_id", "product_name", "product_category").distinct()\
      .withColumnRenamed("product_name", "name")\
      .withColumnRenamed("product_category", "category")

    branches_df.limit(3)

    branches_df.printSchema()

    sales_agents_df = sales_agents_df.withColumnRenamed("sales_person_id", "agent_id")\
      .withColumnRenamed("name", "name")\
      .withColumnRenamed("hire_date", "hire_date")

    sales_agents_df.printSchema()

    def LookUp(df, df_lookup, source_col, target_col, surr_name, name_after, drop=False):
        joined_df = df.alias("l").join(df_lookup.alias("r"), col("l.{}".format(source_col)) == col("r.{}".format(target_col)), "left")
        selected_df = joined_df.select([col("l.{}".format(col_name)) for col_name in df.columns] + [col("r.{}".format(surr_name)).alias(name_after)])
        if drop:
            return selected_df.drop(source_col)
        else:
            return selected_df

    def UpdateDimension(dim_name, prim_key, sur_key, cols):
        max_sk = spark.sql("SELECT COALESCE(MAX({}), 0) AS max_id FROM {}".format(sur_key, dim_name)).collect()[0].max_id
        new_data = spark.sql("""
        SELECT stg.*
        FROM {0}_staging stg
        LEFT JOIN {0} dim ON stg.{1} = dim.{1}
        WHERE dim.{1} IS NULL
        """.format(dim_name, prim_key))
        window_spec = Window.orderBy(prim_key)
        new_data = new_data.withColumn(sur_key, row_number().over(window_spec) + max_sk)
        
        if new_data.count() > 0:
            new_data.select(*cols).write.mode("append").insertInto(dim_name)

    branches_df = LookUp(branches_df, date_dim, "establish_date", "full_date", "date_key", "established_date_id", True)
    branches_df.registerTempTable("branch_dim_staging")

    branch_cols = ["branch_key", "branch_id", "location", "established_date_id", "class"]
    UpdateDimension("branch_dim", "branch_id", "branch_key", branch_cols)

    sales_agents_df = LookUp(sales_agents_df, date_dim, "hire_date", "full_date", "date_key", "hire_date_id", True)
    sales_agents_df.registerTempTable("sales_agent_dim_staging")

    sales_agent_cols = ["agent_key", "agent_id", "name", "hire_date_id"]
    UpdateDimension("sales_agent_dim", "agent_id", "agent_key", sales_agent_cols)

    customer_df.registerTempTable("customer_dim_staging")

    customer_cols = ["customer_key", "customer_id", "customer_full_name", "email"]
    UpdateDimension("customer_dim", "customer_id", "customer_key", customer_cols)

    product_df.registerTempTable("product_dim_staging")

    product_cols = ["product_key", "product_id", "name", "category"]
    UpdateDimension("product_dim", "product_id", "product_key", product_cols)

    def UpdateFactTable(df, fact_name):
        new_fact_data = df.registerTempTable("temp_{}".format(fact_name))
        final = spark.sql(""" SELECT * FROM {0} UNION SELECT * FROM temp_{0}""".format(fact_name))
        final.coalesce(1).write \
        .mode("overwrite") \
        .format("orc") \
        .insertInto(fact_name)

    branches_df = spark.table("branch_dim")
    customer_df = spark.table("customer_dim")
    product_df = spark.table("product_dim")
    sales_agents_df = spark.table("sales_agent_dim")

    select_columns = ['transaction_date','transaction_id','customer_id','sales_agent_id', 'branch_id',
                      'product_id','offer_1','offer_2','offer_3','offer_4','offer_5',
                      'units','unit_price','is_online','payment_method','shipping_address']

    filtered_df = sales_transactions_df.select(*select_columns)\
        .withColumn(
            "discount",
            coalesce(
                when(col("offer_1"), 5),
                when(col("offer_2"), 10),
                when(col("offer_3"), 15),
                when(col("offer_4"), 20),
                when(col("offer_5"), 25),
                lit(0)
            )
        )\
        .withColumn(
            "Total_Price", col("Units") * col("Unit_price") * (1 - col("discount") / 100)
        )\
        .drop("offer_1", "offer_2", "offer_3", "offer_4", "offer_5")\
        .withColumn("shipping_address_split", split(col("shipping_address"), "/"))\
        .withColumn("Street", col("shipping_address_split").getItem(0))\
        .withColumn("City", col("shipping_address_split").getItem(1))\
        .withColumn("State", col("shipping_address_split").getItem(2))\
        .withColumn("Postal_Code", col("shipping_address_split").getItem(3))\
        .drop("shipping_address_split")

    filtered_df = LookUp(filtered_df,date_dim , "transaction_date" ,"full_date","date_key","transaction_date_key" , True)
    filtered_df = LookUp(filtered_df,customer_df , "customer_id" ,"customer_id","customer_key","customer_key" , True)
    filtered_df = LookUp(filtered_df,product_df , "product_id" ,"product_id","product_key","product_key" , True)

    _ = spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    online_filtered_df = filtered_df.filter(col("is_online") == "yes")
    online_filtered_df = online_filtered_df.select(
        "transaction_id",
        "customer_key",
        "product_key",
        "units",
        "unit_price",
        "discount",
        "total_price",
        "payment_method",
        "street",
        "city",
        "state",
        "postal_code",
        "transaction_date_key"
    )

    UpdateFactTable(online_filtered_df , "online_sales_fact")

    branch_filtered_df = filtered_df.filter(col("is_online") == "no")

    branch_filtered_df = LookUp(branch_filtered_df,branches_df , "branch_id" ,"branch_id","branch_key","branch_key" , True)
    branch_filtered_df = LookUp(branch_filtered_df,sales_agents_df , "sales_agent_id" ,"agent_id","agent_key","sales_agent_key" , True)

    branch_filtered_df = branch_filtered_df.select(
       "transaction_id",
        "customer_key",
        "branch_key",
        "sales_agent_key",
        "product_key",
        "units",
        "unit_price",
        "discount",
        "total_price",
        "payment_method",
        "transaction_date_key"
    )

    UpdateFactTable(branch_filtered_df , "branch_sales_fact")

    spark.stop()

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Process Certain Date')
    parser.add_argument('--day', type=str)
    parser.add_argument('--hour', type=str)

    args = parser.parse_args()

    if (args.day is None) != (args.hour is None):
        print("You must provide both --day and --hour")
        sys.exit(1)

    
    main(args)