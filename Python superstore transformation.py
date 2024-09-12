# Databricks notebook source
# creating schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# COMMAND ----------

# Read raw (bronze) data from the volume
Orders = spark.read.format("csv").option("header", "true").load("/Volumes/adrien_workspace/raw/raw_volume/Orders.csv")
Returns = spark.read.format("csv").option("header", "true").load("/Volumes/adrien_workspace/raw/raw_volume/Returns.csv")

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

# Step 1: Filter the Returns DataFrame to only include rows where Returned is 'yes'
# Ensure you rename the columns with invalid characters (spaces in column names)
Returns = Returns.select(
    col("Order ID").alias("order_id"),
    col("Returned").alias("returned")
).filter(col("returned") == 'Yes')

# Step 2: Select relevant columns from Orders and rename the "Order ID" column
Orders = Orders.select(
    col("Order ID").alias("order_id"),
    col("Customer Name").alias("customer_name"),
    col("Sales").alias("sales")
)

# Step 3: Write the DataFrames to Delta tables in the silver schema

# Writing the Returns DataFrame to the silver schema as a table
Returns.write.mode("overwrite").saveAsTable("silver.returns")

# Writing the Orders DataFrame to the silver schema as a table
Orders.write.mode("overwrite").saveAsTable("silver.orders")


# COMMAND ----------

# Step 3: Join the Returns DataFrame with the Orders DataFrame
returned_orders = spark.sql("""
    SELECT o.order_id, o.customer_name, o.sales
    FROM silver.orders o
    JOIN silver.returns r ON o.order_id = r.order_id
""")

# Step 4: Group by Customer Name and sum the Sales
returned_sales = returned_orders.groupBy("customer_name").agg(_sum("sales").alias("total_returned_sales"))

# Step 5: Create a table for the gold model
returned_sales.createOrReplaceTempView("temp_gold_returned_sales")
spark.sql("CREATE TABLE IF NOT EXISTS gold.returned_sales AS SELECT * FROM temp_gold_returned_sales")

# Display the final gold table
display(spark.table("gold.returned_sales"))
