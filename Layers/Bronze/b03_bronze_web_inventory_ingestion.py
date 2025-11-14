# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Web Inventory Snapshot
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_web_inventory`  
# MAGIC **Description**: Real-time inventory view from website DB  
# MAGIC **Generated**: 2025-10-30 15:00:35

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType# TODO: Implement source data reading

# Example:
# df = spark.read.format("...").load("...")

bronze_web_inventory_schema = StructType([
    StructField("inventory_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("available_qty", IntegerType(), True),
    StructField("reserved_qty", IntegerType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("_fivetran_deleted", BooleanType(), True),
    StructField("_fivetran_synced", TimestampType(), True)
])

bronze_unclean_web_inventory_df = (
    spark.readStream
        .schema(bronze_web_inventory_schema) \
        .format("delta") \
        .table("db_uci_data_team_dev_wkspc.postgres_public.web_inventory")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unclean Data Display

# COMMAND ----------

bronze_unclean_web_inventory_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

bronze_with_metadata_df = bronze_unclean_web_inventory_df \
    .withColumn("_source_system", F.lit("postgres")) \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_cdc_operation", F.when(F.col("_fivetran_deleted") == True, F.lit("DELETE"))
                .otherwise(F.lit("UPSERT")) \
    ) \
    .withColumn("_cdc_timestamp", F.current_timestamp()) \
    .withColumn("_record_hash", F.md5(F.concat_ws("||",
                                                  F.coalesce(F.col("inventory_id").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("sku"), F.lit("")),
                                                  F.coalesce(F.col("available_qty").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("reserved_qty").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("last_updated").cast("string"), F.lit(""))
    ))
)
    
bronze_schemacontrolled_web_inventory_df = bronze_with_metadata_df.select(
    F.col("inventory_id").cast("long"),
    F.col("sku").cast("string"),
    F.col("available_qty").cast("double"),
    F.col("reserved_qty").cast("double"),
    F.col("last_updated").cast("timestamp"),
    F.col("_source_system").cast("string"),
    F.col("_ingestion_timestamp").cast("timestamp"),
    F.col("_cdc_operation").cast("string"),
    F.col("_cdc_timestamp").cast("timestamp"),
    F.col("_record_hash").cast("string")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_web_inventory")

bronze_web_inventory_df = bronze_schemacontrolled_web_inventory_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_web_inventory") \
    .outputMode("append") \
    .option("mergeSchema", "false") \
    .table("db_uci_data_team_dev_wkspc.shopfast.bronze_web_inventory")

bronze_web_inventory_df.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
