import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime , timedelta

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("B2B_Job")\
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport()\
    .getOrCreate()

sc = spark.sparkContext

_ = spark.sql("USE Qcompany")

previous_day_date = datetime.now() - timedelta(days=1)
previous_day_date = str(previous_day_date.strftime('%Y-%m-%d'))

daily_report = spark.sql("""
    SELECT sd.agent_key, sd.name, pr.name AS product_name, SUM(fct.units) AS total_sales
    FROM branch_sales_fact fct
    JOIN product_dim pr ON fct.product_key = pr.product_key
    JOIN sales_agent_dim sd ON fct.sales_agent_key = sd.agent_key
    WHERE transaction_date_key = (SELECT date_key FROM date_dim WHERE full_date = '{}')
    GROUP BY sd.agent_key, sd.name, pr.name
    ORDER BY sd.agent_key, product_name
""".format(previous_day_date))

os.makedirs("/home/itversity/B2B_Team/", exist_ok=True)
daily_report.coalesce(1).write.csv("file:///home/itversity/B2B_Team/{}".format(previous_day_date), header=True)

