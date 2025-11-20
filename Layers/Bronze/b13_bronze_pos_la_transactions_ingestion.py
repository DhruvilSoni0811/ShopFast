# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest LA POS Transactions (Kafka)
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_pos_la_transactions`  
# MAGIC **Description**: Real-time Kafka stream  
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
source_folder = "pos/la"
checkpoint_path = "checkpoints/csv_schema/la/transactions"

schema_location = "schemas/pos_la_transaction_schema"
target_main_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_pos_la_transactions"

source_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{source_folder}/"
checkpoints_full_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{checkpoint_path}/"
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

la_transactions_json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_amount", DecimalType(10,2), True),
    StructField("tender_type", StringType(), True),
    StructField("cashier_id", StringType(), True),
    StructField("register_id", StringType(), True)
])

# COMMAND ----------

pos_la_transactions_inventory_raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoints_full_path)
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("cloudFiles.maxBytesPerTrigger", "10g")
    .option("cloudFiles.includePattern", "transactions_\\d+\\.json")
    .option("cloudFiles.validateOptions", "false")
    .schema(la_transactions_json_schema)
    .load(source_path)
)

# COMMAND ----------

pos_la_transactions_inventory_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

pos_la_transactions_inventory_clean_df = (pos_la_transactions_inventory_raw_df.select(
    col("event_id"),
    col("event_type"),
    col("store_id"),
    col("store_name"),
    col("transaction_id"),
    col("timestamp").alias("event_timestamp"),
    col("payment_method"),
    col("payment_amount"),
    col("tender_type"),
    col("cashier_id"),
    col("register_id")
)
.withColumn("_source_system", lit("pos_la_transactions_api"))
.withColumn("_ingestion_timestamp", current_timestamp())
.withColumn("_file_path", col("_metadata.file_path"))
.withColumn("_file_name", col("_metadata.file_name"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_pos_la_transactions")

query = (pos_la_transactions_inventory_clean_df.writeStream
         .format("delta") \
         .option("mergeSchema", "true")
         .option("checkpointLocation", checkpoints_full_path + "/pos_la_transactions")
         .trigger(availableNow=True)
         .toTable(target_main_table)
         )

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
