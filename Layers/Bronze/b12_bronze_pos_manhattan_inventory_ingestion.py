# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Manhattan POS Inventory (API)
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_pos_manhattan_inventory`  
# MAGIC **Description**: REST polling every 5 min  
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
source_folder = "pos/manhattan"
checkpoint_path = "checkpoints/csv_schema/manhattan"

schema_location = "schemas/pos_manhattan_schema"
target_main_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_main_pos_manhattan_inventory"
target_sub_table = "db_uci_data_team_dev_wkspc.shopfast.bronze_sub_pos_manhattan_inventory"

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

manhattan_main_json_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    
    StructField("location", StructType([
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True),

    StructField("timestamp", TimestampType(), True),
    StructField("last_physical_count", TimestampType(), True),
    
    StructField("inventory_snapshot", ArrayType(StructType([
        StructField("sku", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity_on_floor", IntegerType(), True),
        StructField("quantity_in_backroom", IntegerType(), True),
        StructField("quantity_total", IntegerType(), True),
        StructField("reserved_for_online_pickup", IntegerType(), True),
        StructField("last_sold", TimestampType(), True),
        StructField("price", DecimalType(10,2), True),
        StructField("on_promotion", BooleanType(), True),
        StructField("promo_price", DecimalType(10,2), True)
    ])), True)
])

# COMMAND ----------

pos_manhattan_inventory_raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoints_full_path)
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("cloudFiles.maxBytesPerTrigger", "10g")
    .schema(manhattan_main_json_schema)
    .load(source_path)
)


# COMMAND ----------

pos_manhattan_inventory_raw_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

main_df = (
    pos_manhattan_inventory_raw_df
    .select(
        "store_id",
        "store_name",
        col("location.address").alias("address"),
        col("location.city").alias("city"),
        col("location.state").alias("state"),
        col("location.zip").alias("zip"),
        "timestamp",
        "last_physical_count"
    )
)

# COMMAND ----------

sub_df = (
    pos_manhattan_inventory_raw_df
    .select(
        "store_id",
        explode("inventory_snapshot").alias("item")
    )
    .select(
        "store_id",
        col("item.sku").alias("sku"),
        col("item.product_name").alias("product_name"),
        col("item.quantity_on_floor").alias("quantity_on_floor"),
        col("item.quantity_in_backroom").alias("quantity_in_backroom"),
        col("item.quantity_total").alias("quantity_total"),
        col("item.reserved_for_online_pickup").alias("reserved_for_online_pickup"),
        col("item.last_sold").alias("last_sold"),
        col("item.price").alias("price"),
        col("item.on_promotion").alias("on_promotion"),
        col("item.promo_price").alias("promo_price")
    )
)

# COMMAND ----------

main_df.display()

# COMMAND ----------

sub_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_pos_manhattan_inventory")

def write_to_both_tables(batch_df, batch_id):
    main_df = (
        batch_df
        .select(
            "store_id",
            "store_name",
            col("location.address").alias("address"),
            col("location.city").alias("city"),
            col("location.state").alias("state"),
            col("location.zip").alias("zip"),
            "timestamp",
            "last_physical_count"
        )
    )

    sub_df = (
        batch_df
        .select(
            "store_id",
            explode("inventory_snapshot").alias("item")
        )
        .select(
            "store_id",
            col("item.sku").alias("sku"),
            col("item.product_name").alias("product_name"),
            col("item.quantity_on_floor").alias("quantity_on_floor"),
            col("item.quantity_in_backroom").alias("quantity_in_backroom"),
            col("item.quantity_total").alias("quantity_total"),
            col("item.reserved_for_online_pickup").alias("reserved_for_online_pickup"),
            col("item.last_sold").alias("last_sold"),
            col("item.price").alias("price"),
            col("item.on_promotion").alias("on_promotion"),
            col("item.promo_price").alias("promo_price")
        )
    )

    main_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_main_table)

    sub_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_sub_table)

# COMMAND ----------


query = (
    pos_manhattan_inventory_raw_df.writeStream
    .foreachBatch(write_to_both_tables)
    .option("checkpointLocation", checkpoints_full_path + "/combined")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from
# MAGIC db_uci_data_team_dev_wkspc.shopfast.bronze_main_pos_manhattan_inventory limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
