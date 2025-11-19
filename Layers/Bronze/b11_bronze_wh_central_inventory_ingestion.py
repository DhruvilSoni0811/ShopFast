# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Central Warehouse CSV
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_wh_central_inventory`  
# MAGIC **Description**: Detailed export with damaged & in-transit  
# MAGIC **Generated**: 2025-10-30 15:00:35

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path("/Workspace/Users/dhruvil@uciny.com/ShopFast/.env.development.local")
load_dotenv(env_path, override=True)

storage_account = "stgaccshopfastcindres"
container_name = "shopfast-raw-data"
source_folder = "warehouses/central"
checkpoint_path = "checkpoints/csv_schema/central"
schema_location = "schemas/central_wh_inventory_schema"
target_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_wh_central_inventory"

source_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{source_folder}/"
checkpoint_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{checkpoint_path}/"
schema_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{schema_location}/"


ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")

print("Loaded key:", ADLS_ACCOUNT_KEY)
print("CWD:", os.getcwd())
print("Env file exists:", os.path.isfile("../.env/development.local"))


spark.conf.set(
  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
  ADLS_ACCOUNT_KEY
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data

# COMMAND ----------

# TODO: Implement source data reading
# Example:
# df = spark.read.format("...").load("...")

csv_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("product_description", StringType(), True),
    StructField("physical_inventory", IntegerType(), True),
    StructField("committed_inventory", IntegerType(), True),
    StructField("available_inventory", IntegerType(), True),
    StructField("in_transit_from_supplier", IntegerType(), True),
    StructField("damaged_qty", IntegerType(), True),
    StructField("storage_location", StringType(), True),
    StructField("last_cycle_count", TimestampType(), True),
    StructField("lot_batch", StringType(), True),
    StructField("cost_per_unit", DecimalType(10,2), True),
    StructField("warehouse_id", StringType(), True),
    StructField("snapshot_timestamp", TimestampType(), True)
])

# COMMAND ----------

central_wh_inventory_raw_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")

        .option("cloudFiles.schemaLocation", schema_full_path)
        .option("cloudFiles.inferColumnTypes", "true")

        .option("header", "true")
        .option("delimiter", ",")
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

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

central_wh_inventory_transformed_df = (
    central_wh_inventory_raw_df
    .withColumn("sku", F.col("sku").cast("string"))
    .withColumn("product_description", F.col("product_description").cast("string"))
    .withColumn("physical_inventory", F.col("physical_inventory").cast("integer"))
    .withColumn("committed_inventory", F.col("committed_inventory").cast("integer"))
    .withColumn("available_inventory", F.col("available_inventory").cast("integer"))
    .withColumn("in_transit_from_supplier", F.col("in_transit_from_supplier").cast("integer"))
    .withColumn("damaged_qty", F.col("damaged_qty").cast("integer"))
    .withColumn("storage_location", F.col("storage_location").cast("string"))
    .withColumn("last_cycle_count", F.col("last_cycle_count").cast("timestamp"))
    .withColumn("lot_batch", F.col("lot_batch").cast("string"))
    .withColumn("cost_per_unit", F.col("cost_per_unit").cast("decimal"))
    .withColumn("warehouse_id", F.col("warehouse_id").cast("string"))
    .withColumn("snapshot_timestamp", F.col("snapshot_timestamp").cast("timestamp"))
    .withColumn("_source_system", lit("central_warehouse"))
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_file_name", F.col("_metadata.file_name"))
    .withColumn("_file_path", F.col("_metadata.file_path"))
)

# COMMAND ----------

central_wh_inventory_transformed_df.display(
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_wh_central_inventory")

query = (central_wh_inventory_transformed_df.writeStream
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
