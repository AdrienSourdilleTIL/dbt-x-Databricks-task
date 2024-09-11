# Databricks notebook source
# Read raw (bronze) data from the volume
Orders = spark.read.format("csv").option("header", "true").load("/Volumes/adrien_workspace/raw/raw_volume/Orders.csv")
Returns = spark.read.format("csv").option("header", "true").load("/Volumes/adrien_workspace/raw/raw_volume/Returns.csv")



# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

# Step 1: Filter the Returns DataFrame to only include rows where Returned is 'yes'
Returns = Returns.filter(Returns.Returned == 'Yes')

# Select relevant columns from Orders
Orders = Orders.select(
    col("Order ID").alias("order_id"),
    col("Customer Name").alias("customer_name"),
    col("Sales").alias("sales")
)

# Step 2: Join the Returns DataFrame with the Orders DataFrame
returned_orders = Returns.join(Orders, Returns["Order ID"] == Orders["order_id"])

# Step 3: Group by Customer Name and sum the Sales
returned_sales = returned_orders.groupBy("customer_name").agg(_sum("sales").alias("total_returned_sales"))

# Step 4: Create a view for the silver model
returned_sales.createOrReplaceTempView("silver_returned_sales")

# Step 5: Create a table for the gold model
spark.sql("CREATE TABLE IF NOT EXISTS gold.returned_sales AS SELECT * FROM silver_returned_sales")

# Display the final gold table
display(spark.table("gold.returned_sales"))
