# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Cart Events
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_app_cart_events`  
# MAGIC **Description**: Add/remove events for inventory holds  
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

source_table = "db_uci_data_team_dev_wkspc.mongo_shopfastmobileapp.app_cart_events"

nested_app_cart_events_df = spark.read.format("delta").table(source_table)

# COMMAND ----------

nested_app_cart_events_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

json_app_cart_events_df = StructType([
    StructField("_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("inventory_reserved", BooleanType(), True),
    StructField("expires_at", TimestampType(), True)
])

app_cart_events_df_parsed = nested_app_cart_events_df.withColumn("parsed_data", from_json(col("data"), json_app_cart_events_df))

# COMMAND ----------

app_cart_events_df_flattened = app_cart_events_df_parsed.select(
    col("_id").alias("fivetran_id"),
    col("_fivetran_synced"),
    col("_fivetran_deleted"),
    col("parsed_data._id").alias("cart_id"),
    col("parsed_data.event_type"),
    col("parsed_data.customer_id"),
    col("parsed_data.session_id"),
    col("parsed_data.sku"),
    col("parsed_data.quantity"),
    col("parsed_data.timestamp"),
    col("parsed_data.inventory_reserved"),
    col("parsed_data.expires_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_app_inventory_sync")

target_app_cart_df = "db_uci_data_team_dev_wkspc.shopfast.bronze_app_cart_events"
app_cart_events_df_flattened.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable(target_app_cart_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db_uci_data_team_dev_wkspc.shopfast.bronze_app_cart_events

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
