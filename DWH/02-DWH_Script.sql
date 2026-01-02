set hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS Qcompany;
USE Qcompany;

-- Customer Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS customer_dim (
    customer_key INT,
    customer_id INT,
    customer_full_name STRING,
    email STRING
)
STORED AS ORC;

-- Product Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS product_dim (
    product_key INT,
    product_id INT,
    name STRING,
    category STRING
)
STORED AS ORC;

-- Branch Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS branch_dim (
    branch_key INT,
    branch_id INT,
    location STRING,
    established_date_id INT,
    class STRING
)
STORED AS ORC;

-- Sales Agent Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS sales_agent_dim (
    agent_key INT,
    agent_id INT,
    name STRING,
    hire_date_id INT
)
STORED AS ORC;

-- Date Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS date_dim (
    date_key INT,
    full_date STRING,
    year INT,
    quarter_number INT,
    month_name STRING,
    month_number INT,
    day_name STRING,
    day_of_month INT
)
STORED AS ORC;

-- Create external table for online sales
CREATE EXTERNAL TABLE online_sales_fact (
  transaction_id String,
  customer_key INT,
  product_key INT,
  units INT,
  unit_price DECIMAL(10,2),
  discount DECIMAL(10,2),
  total_price DECIMAL(10,2),
  payment_method STRING,
  street STRING,
  city STRING,
  state STRING,
  postal_code STRING
)
PARTITIONED BY (transaction_date_key INT)
STORED AS ORC;

-- Create external table for branch sales
CREATE EXTERNAL TABLE branch_sales_fact (
  transaction_id STRING,
  customer_key INT,
  branch_key INT,
  sales_agent_key INT,
  product_key INT,
  units INT,
  unit_price DECIMAL(10,2),
  discount DECIMAL(10,2),
  total_price DECIMAL(10,2),
  payment_method STRING
)
PARTITIONED BY (transaction_date_key INT )
STORED AS ORC;



-- Streaming Table 

CREATE EXTERNAL TABLE IF NOT EXISTS log_Events (
    eventType STRING,
    customerId STRING,
    productId STRING,
    `timestamp` STRING,
    quantity INT,
    totalAmount FLOAT,
    paymentMethod STRING,
    recommendedProductId STRING,
    algorithm STRING,
    category STRING,
    source STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/log_Events';

