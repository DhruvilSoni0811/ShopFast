# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Web Orders (PostgreSQL CDC)
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Target table(s)**: `bronze_web_orders`  
# MAGIC **Description**: CDC from PostgreSQL using Debezium/Fivetran  
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

bronze_web_orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address_id", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("_fivetran_deleted", BooleanType(), True),
    StructField("_fivetran_synced", TimestampType(), True)
])


bronze_unclean_web_orders_df = (
                        spark.readStream   
                            .schema(bronze_web_orders_schema) \
                            .format("delta") \
                            .table("db_uci_data_team_dev_wkspc.postgres_public.web_orders")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unclean Data Display

# COMMAND ----------

bronze_unclean_web_orders_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality & Validation

# COMMAND ----------

# TODO: Add data quality checks
# Example: Check for nulls, duplicates, schema validation

#Schema Infer aka. Schema Drift Control

bronze_with_metadata_df = bronze_unclean_web_orders_df \
    .withColumn("_source_system", F.lit("website_postgresql")) \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_cdc_operation",
                F.when(F.col("_fivetran_deleted") == True, F.lit("DELETE"))
                .otherwise(F.lit("UPSERT"))
    ) \
    .withColumn("_cdc_timestamp", F.col("_fivetran_synced")) \
    .withColumn("_record_hash",
                F.md5(F.concat_ws("||",
                                  F.coalesce(F.col("order_id"), F.lit("")),
                                  F.coalesce(F.col("customer_id"), F.lit("")),
                                  F.coalesce(F.col("order_date").cast("string"), F.lit("")),
                                  F.coalesce(F.col("order_status"), F.lit("")),
                                  F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
                                  F.coalesce(F.col("payment_method"), F.lit("")),
                                  F.coalesce(F.col("shipping_address_id").cast("string"), F.lit("")),
                                  F.coalesce(F.col("created_at").cast("string"), F.lit("")),
                                  F.coalesce(F.col("updated_at").cast("string"), F.lit(""))
                ))
        )
    
bronze_schemacontrolled_web_orders_df = bronze_with_metadata_df.select(
    F.col("order_id").cast("string"),
    F.col("customer_id").cast("string"),
    F.col("order_date").cast("timestamp"),  # ← timestamp not string
    F.col("order_status").cast("string"),
    F.col("total_amount").cast("double"),
    F.col("payment_method").cast("string"),
    F.col("shipping_address_id").cast("int"),  # ← int not integer
    F.col("created_at").cast("timestamp"),
    F.col("updated_at").cast("timestamp"),
    # Bronze metadata columns
    F.col("_source_system"),
    F.col("_ingestion_timestamp"),
    F.col("_cdc_operation"),
    F.col("_cdc_timestamp"),
    F.col("_record_hash")
)


# COMMAND ----------

bronze_schemacontrolled_web_orders_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Table

# COMMAND ----------

# TODO: Write to Delta table
# Example:
# df.write #   .format("delta") #   .mode("append") #   .option("mergeSchema", "true") #   .saveAsTable("bronze_web_orders")

bronze_web_orders_df = bronze_schemacontrolled_web_orders_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_web_orders") \
    .outputMode("append") \
    .option("mergeSchema", "false") \
    .trigger(availableNow=True) \
    .table("db_uci_data_team_dev_wkspc.shopfast.bronze_web_orders")

bronze_web_orders_df.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Logging & Monitoring

# COMMAND ----------

# TODO: Add logging and alerts
# Record counts, execution time, data quality metrics
