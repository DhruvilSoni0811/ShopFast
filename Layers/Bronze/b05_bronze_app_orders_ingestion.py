# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Mobile App Orders (MongoDB)
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_app_orders`  
# MAGIC **Description**: Change streams from MongoDB Atlas  
# MAGIC **Generated**: 2025-10-30 15:00:35

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data

# COMMAND ----------

# TODO: Implement source data reading
# Example:
# df = spark.read.format("...").load("...")

source_table = "db_uci_data_team_dev_wkspc.mongo_shopfastmobileapp.app_orders"
nested_app_order_df = spark.read.format("delta").table(source_table)

# COMMAND ----------

nested_app_order_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# Schema Definition

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

json_app_order_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_transaction_id", StringType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("payment_status", StringType(), True),
    StructField("shipping_street", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("shipping_zip", StringType(), True),
    StructField("shipping_method", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("promo_code", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

app_order_df_parsed = nested_app_order_df.withColumn("parsed_data", from_json(col("data"), json_app_order_schema))

# COMMAND ----------

app_order_df_parsed.display()

# COMMAND ----------

app_order_df_flattened = app_order_df_parsed.select(
    col("_id").alias("fivetran_id"),
    col("_fivetran_synced"),
    col("_fivetran_deleted"),
    col("parsed_data._id").alias("order_record_id"),
    col("parsed_data.order_id"),
    col("parsed_data.customer_id"),
    col("parsed_data.customer_name"),
    col("parsed_data.customer_email"),
    col("parsed_data.customer_phone"),
    col("parsed_data.app_version"),
    col("parsed_data.device_type"),
    to_timestamp(col("parsed_data.order_date")).alias("order_date"),
    col("parsed_data.status"),
    col("parsed_data.payment_method"),
    col("parsed_data.payment_transaction_id"),
    col("parsed_data.payment_amount"),
    col("parsed_data.payment_status"),
    col("parsed_data.shipping_city"),
    col("parsed_data.shipping_state"),
    col("parsed_data.shipping_zip"),
    col("parsed_data.shipping_method"),
    col("parsed_data.tracking_number"),
    col("parsed_data.session_id"),
    col("parsed_data.promo_code"),
    to_timestamp(col("parsed_data.created_at")).alias("created_at"),
    to_timestamp(col("parsed_data.updated_at")).alias("updated_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_app_orders")

target_app_order_df = "db_uci_data_team_dev_wkspc.shopfast.app_order_flattened"
app_order_df_flattened.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_app_order_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db_uci_data_team_dev_wkspc.shopfast.app_order_flattened

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
