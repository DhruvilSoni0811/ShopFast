# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Web Order Items
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_web_order_items`  
# MAGIC **Description**: Line items from web orders  
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

# TODO: Implement source data reading
# Example:
# df = spark.read.format("...").load("...")

bronze_web_order_items_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("discount_applied", DecimalType(10,2), True),
    StructField("fullfillment_status", StringType(), True),
    StructField("warehouse_allocated", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("_fivetran_deleted", BooleanType(), True),
    StructField("_fivetran_synced", TimestampType(), True)
])

bronze_unclean_web_order_items_df = (
    spark.readStream
        .schema(bronze_web_order_items_schema) \
            .format("delta") \
            .table("db_uci_data_team_dev_wkspc.azure_postgres_public.web_order_items")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Unclean Data Display

# COMMAND ----------

bronze_unclean_web_order_items_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

bronze_with_metadata_df = bronze_unclean_web_order_items_df \
    .withColumn("_source_system", F.lit("website_postgresql")) \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_cdc_operation", F.when(F.col("_fivetran_deleted") == True, F.lit("DELETE"))
                .otherwise(F.lit("UPSERT"))
    ) \
    .withColumn("_cdc_timestamp", F.current_timestamp()) \
    .withColumn("_record_hash", F.md5(F.concat_ws("||", 
                                                   F.coalesce(F.col("order_item_id"), F.lit("")),
                                                   F.coalesce(F.col("order_id"), F.lit("")),
                                                   F.coalesce(F.col("sku").cast("string"), F.lit("")),
                                                   F.coalesce(F.col("product_name"), F.lit("")),
                                                   F.coalesce(F.col("quantity").cast("string"), F.lit("")),
                                                   F.coalesce(F.col("unit_price").cast("string"), F.lit("")),
                                                   F.coalesce(F.col("discount_applied").cast("string"), F.lit("")),
                                                   F.coalesce(F.col("fulfillment_status"), F.lit("")),
                                                   F.coalesce(F.col("warehouse_allocated"), F.lit("")),
                                                   F.coalesce(F.col("created_at").cast("string"), F.lit(""))
    ))
)

bronze_schemacontrolled_web_order_items_df = bronze_with_metadata_df.select(
    F.col("order_item_id").cast("string"),
    F.col("order_id").cast("string"),
    F.col("sku").cast("string"),
    F.col("product_name").cast("string"),
    F.col("quantity").cast("integer"),
    F.col("unit_price").cast("decimal"),
    F.col("discount_applied").cast("decimal"),
    F.col("fulfillment_status").cast("string"),
    F.col("warehouse_allocated").cast("string"),
    F.col("created_at").cast("timestamp"),
    F.col("_source_system").cast("string"),
    F.col("_ingestion_timestamp").cast("timestamp"),
    F.col("_cdc_operation").cast("string"),
    F.col("_cdc_timestamp").cast("timestamp"),
    F.col("_record_hash").cast("string")

)

# COMMAND ----------

bronze_schemacontrolled_web_order_items_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_web_order_items")

bronze_web_order_items_df = bronze_schemacontrolled_web_order_items_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_web_order_items") \
      .outputMode("append") \
      .option("mergeSchema", "false") \
      .trigger(availableNow=True) \
      .table("db_uci_data_team_dev_wkspc.shopfast.bronze_web_order_items")

bronze_web_order_items_df.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
