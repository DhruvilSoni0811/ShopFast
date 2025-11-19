# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest App Inventory
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_app_inventory_sync`  
# MAGIC **Description**: 30-min sync cycle  
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

app_inventory_df_flattened = (app_inventory_df_parsed.select(
  col("_id"),
  col("parsed_data.sku"),
  col("parsed_data.available_quantity"),
  col("parsed_data.display_status"),
  col("parsed_data.last_synced"),
  col("parsed_data.source_warehouse"),
  col("parsed_data.estimated_delivery"),
  col("_fivetran_deleted")
)
.withColumn("_source_system", lit("mobile_application"))
.withColumn("_ingestion_timestamp", current_timestamp())
.withColumn("_mongodb_operation", when(col("_fivetran_deleted") == True, lit("delete"))
.otherwise(lit("upsert"))                              
)
.drop("_fivetran_deleted")
)

# COMMAND ----------

app_inventory_df_flattened.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_app_cart_events")

target_app_inventory_df = "db_uci_data_team_dev_wkspc.shopfast.bronze_app_inventory"
app_inventory_df_flattened.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable(target_app_inventory_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db_uci_data_team_dev_wkspc.shopfast.bronze_app_inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
