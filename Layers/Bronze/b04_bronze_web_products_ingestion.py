# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Product Catalog
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_web_products`  
# MAGIC **Description**: Master product data with pricing & suppliers  
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

bronze_web_products_schema = StructType([
  StructField("product_id", IntegerType(), True),
  StructField("sku", StringType(), True),
  StructField("product_name", StringType(), True),
  StructField("category", StringType(), True),
  StructField("subcategory", StringType(), True),
  StructField("price", DecimalType(10,2), True),
  StructField("cost", DecimalType(10,2), True),
  StructField("supplier_id", StringType(), True),
  StructField("reorder_point", IntegerType(), True),
  StructField("lead_time_days", IntegerType(), True),
  StructField("is_active", BooleanType(), True),
  StructField("created_at", TimestampType(), True),
  StructField("_fivetran_deleted", BooleanType(), True),
  StructField("_fivetran_synced", TimestampType(), True)
])

bronze_unclean_web_products_df = (
  spark.readStream
    .schema(bronze_web_products_schema) \
      .format("delta") \
      .table("db_uci_data_team_dev_wkspc.postgres_public.web_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unclean Data Display

# COMMAND ----------

bronze_unclean_web_products_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

bronze_with_metadata_df = bronze_unclean_web_products_df \
    .withColumn("_source_system", F.lit("webiste_postgresql")) \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_cdc_operation", F.when(F.col("_fivetran_deleted") == True, F.lit("DELETE"))
                .otherwise(F.lit("UPSERT"))
    ) \
    .withColumn("_cdc_timestamp", F.current_timestamp()) \
    .withColumn("_record_hash", F.md5(F.concat_ws("||",
                                                  F.coalesce(F.col("product_id").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("sku"), F.lit("")),
                                                  F.coalesce(F.col("product_name"), F.lit("")),
                                                  F.coalesce(F.col("category"), F.lit("")),
                                                  F.coalesce(F.col("subcategory"), F.lit("")),
                                                  F.coalesce(F.col("price").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("cost").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("supplier_id"), F.lit("")),
                                                  F.coalesce(F.col("reorder_point").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("lead_time_days").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("is_active").cast("string"), F.lit("")),
                                                  F.coalesce(F.col("created_at").cast("string"), F.lit(""))
                ))
    )

bronze_schemacontrolled_web_products_df = bronze_with_metadata_df \
    .select(
        F.col("product_id").cast("integer"),
        F.col("sku").cast("string"),
        F.col("product_name").cast("string"),
        F.col("category").cast("string"),
        F.col("subcategory").cast("string"),
        F.col("price").cast("decimal"),
        F.col("cost").cast("decimal"),
        F.col("supplier_id").cast("string"),
        F.col("reorder_point").cast("integer"),
        F.col("lead_time_days").cast("integer"),
        F.col("is_active").cast("boolean"),
        F.col("created_at").cast("timestamp"),
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
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_web_products")

bronze_web_products_df = bronze_schemacontrolled_web_products_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_web_products") \
        .outputMode("append") \
        .option("mergeSchema", "false") \
        .table("db_uci_data_team_dev_wkspc.shopfast.bronze_web_products")

bronze_web_products_df.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
