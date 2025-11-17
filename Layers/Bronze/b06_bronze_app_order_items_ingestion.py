# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Mobile Order Items
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_app_order_items`  
# MAGIC **Description**: Flattened array items  
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

source_table = "db_uci_data_team_dev_wkspc.mongo_shopfastmobileapp.app_order_items"

nested_app_order_items_df = spark.read.format("delta").table(source_table)

# COMMAND ----------

nested_app_order_items_df.printSchema()
# nested_app_order_items_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

json_app_order_items_schema = StructType([
    StructField("_id",StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("size", StringType(), True),
    StructField("color", StringType(), True),

])

app_order_items_df_parsed = nested_app_order_items_df.withColumn("parsed_data", from_json(col("data"), json_app_order_items_schema))

# COMMAND ----------

app_order_items_df_parsed.display()

# COMMAND ----------

app_order_items_df_flattened = app_order_items_df_parsed.select(
    col("_id").alias("fivetran_id"),
    col("_fivetran_synced"),
    col("_fivetran_deleted"),
    col("parsed_data._id").alias("order_items_record_id"),
    col("parsed_data.order_id"),
    col("parsed_data.sku"),
    col("parsed_data.product_name"),
    col("parsed_data.quantity"),
    col("parsed_data.unit_price"),
    col("parsed_data.size"),
    col("parsed_data.color")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_app_order_items")

target_app_order_items_df = "db_uci_data_team_dev_wkspc.shopfast.app_order_items_flattened"
app_order_items_df_flattened.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_app_order_items_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from db_uci_data_team_dev_wkspc.shopfast.app_order_items_flattened

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
