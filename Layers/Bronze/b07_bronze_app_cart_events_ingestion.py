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

source_table = "db_uci_data_team_dev_wkspc.mongo_shopfastmobileapp.app_inventory_sync"

nested_app_inventory_df = spark.read.format("delta").table(source_table)

# COMMAND ----------

nested_app_inventory_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

json_app_inventory_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("available_quantity", IntegerType(), True),
    StructField("display_status", StringType(), True),
    StructField("last_synced", TimestampType(), True),
    StructField("source_warehouse", StringType(), True),
    StructField("estimated_delivery", TimestampType(), True)
])

app_inventory_df_parsed = nested_app_inventory_df.withColumn("parsed_data", from_json(col("data"), json_app_inventory_schema))

# COMMAND ----------

app_inventory_df_flattened = app_inventory_df_parsed.select(
  col("_id").alias("fivetran_id"),
  col("_fivetran_synced"),
  col("_fivetran_deleted"),
  col("parsed_data._id").alias("order_inventory_id"),
  col("parsed_data.sku"),
  col("parsed_data.available_quantity"),
  col("parsed_data.display_status"),
  col("parsed_data.last_synced"),
  col("parsed_data.source_warehouse"),
  col("parsed_data.estimated_delivery")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_app_cart_events")

target_app_inventory_df = "db_uci_data_team_dev_wkspc.shopfast.app_inventory_flattened"
app_inventory_df_flattened.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable(target_app_inventory_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db_uci_data_team_dev_wkspc.shopfast.app_inventory_flattened

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
