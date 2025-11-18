# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest East Warehouse CSV
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_wh_east_inventory`  
# MAGIC **Description**: Daily CSV via AutoLoader  
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

env_path = Path("/Workspace/Users/dhruvil@uciny.com/ShopFast/.env.development.local")
load_dotenv(env_path, override=True)
import os

storage_account = "stgaccshopfastcindres"
container_name = "shopfast-raw-data"
source_folder = "warehouses/east"
checkpoint_path = "checkpoints/csv_schema/east"
schema_location = "schemas/east_wh_inventory_schema"
target_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_wh_east_inventory"

source_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{source_folder}/"
checkpoint_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{checkpoint_path}/"
schema_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{schema_location}"

ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")

print("Loaded key:", ADLS_ACCOUNT_KEY)
print("CWD:", os.getcwd())
print("Env file exists:", os.path.isfile("../.env.development.local"))


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
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("last_counter_date", DateType(), True),
    StructField("batch_number", StringType(), True),
    StructField("expiry_date", DateType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("export_timestamp", TimestampType(), True)
])


# COMMAND ----------

east_wh_inventory_raw_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        
        .option("cloudFiles.schemaLocation", schema_full_path)
        .option("cloudFiles.inferColumnTypes", "true")
      #  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")

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

display(east_wh_inventory_raw_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

east_wh_inventory_transformed_df = (
    east_wh_inventory_raw_df
        .withColumn("export_timestamp", F.col("export_timestamp").cast("timestamp"))
        .withColumn("sku", F.col("sku").cast("string"))
        .withColumn("product_description", F.col("product_description").cast("string"))
        .withColumn("quantity_on_hand", F.col("quantity_on_hand").cast("integer"))
        .withColumn("quantity_reserved", F.col("quantity_reserved").cast("integer"))
        .withColumn("quantity_available", F.col("quantity_available").cast("integer"))
        .withColumn("location", F.col("location").cast("string"))
        .withColumn("last_counter_date", F.col("last_counter_date").cast("date"))
        .withColumn("batch_number", F.col("batch_number").cast("string"))
        .withColumn("expiry_date", F.col("expiry_date").cast("date"))
        .withColumn("warehouse_id", F.col("warehouse_id").cast("string")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_wh_east_inventory")

query = (east_wh_inventory_transformed_df.writeStream
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
