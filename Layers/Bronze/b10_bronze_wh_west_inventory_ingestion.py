# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest West Warehouse CSV
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_wh_west_inventory`  
# MAGIC **Description**: Pipe-delimited, schema-on-read  
# MAGIC **Generated**: 2025-10-30 15:00:35

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pathlib import Path

env_path = Path("/Workspace/Users/dhruvil@uciny.com/ShopFast/.env.development.local")
load_dotenv(env_path, override=True)
import os

storage_account = "stgaccshopfastcindres"
container_name = "shopfast-raw-data"
source_folder = "warehouses/west"
checkpoint_path = "checkpoints/csv_schema/west"
schema_location = "schemas/west_wh_inventory_schema"
target_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_wh_west_inventory"

source_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{source_folder}/"
checkpoint_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{checkpoint_path}/"
schema_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{schema_location}"

ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")

print("Loaded key:", ADLS_ACCOUNT_KEY)
print("CWD:", os.getcwd())
print("Env file exists:", os.path.isfile("../.env.development.local"))

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    ADLS_ACCOUNT_KEY)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data

# COMMAND ----------

# TODO: Implement source data reading
# Example:
# df = spark.read.format("...").load("...")

csv_schema = StructType([
    StructField("item_code", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("on_hand_qty", IntegerType(), True),
    StructField("allocated_qty", IntegerType(), True),
    StructField("free_qty", IntegerType(), True),
    StructField("zone", StringType(), True),
    StructField("last_physical_count", StringType(), True),
    StructField("lot_number", StringType(), True),
    StructField("warehouse_code", StringType(), True),
    StructField("file_generated_at", StringType(), True)

])


# COMMAND ----------

west_wh_inventory_raw_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        
        .option("cloudFiles.schemaLocation", schema_full_path)
        .option("cloudFiles.inferColumnTypes", "true")

        .option("header", "true")
        .option("delimiter", "|")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "false")
        .option("encoding", "UTF-8")

        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .option("cloudFiles.maxBytesPerTrigger", "10g")

        .schema(csv_schema)
        .load(source_path)
)

# COMMAND ----------

west_wh_inventory_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

west_wh_inventory_transformed_df = (
    west_wh_inventory_raw_df
    .withColumn("item_code", F.col("item_code").cast("string"))
    .withColumn("item_name", F.col("item_name").cast("string"))
    .withColumn("on_hand_qty", F.col("on_hand_qty").cast("integer"))
    .withColumn("allocated_qty", F.col("allocated_qty").cast("integer"))
    .withColumn("free_qty", F.col("free_qty").cast("integer"))
    .withColumn("zone", F.col("zone").cast("string"))
    .withColumn("last_physical_count", F.col("last_physical_count").cast("string"))
    .withColumn("lot_number", F.col("lot_number").cast("string"))
    .withColumn("warehouse_code", F.col("warehouse_code").cast("string"))
    .withColumn("file_generated_at", F.col("file_generated_at").cast("string"))
    .withColumn("_source_system", lit("west_warehouse"))
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_file_name", F.col("_metadata.file_name"))
    .withColumn("_file_path", F.col("_metadata.file_path"))
)

# COMMAND ----------

west_wh_inventory_transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_wh_west_inventory")

query = (west_wh_inventory_transformed_df.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_full_path)
         .option("mergeSchema", "true")
         
         .trigger(availableNow=True)
         .toTable(target_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
