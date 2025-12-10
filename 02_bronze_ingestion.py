# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------


spark.sql("DROP TABLE IF EXISTS health_catalog.bronze.patient_records")


# COMMAND ----------

# DBTITLE 1,Reading Data From S3 bucket
from pyspark.sql import functions as F

# File Path & Table Name
raw_path = "s3://revdemobucketpersistence/raw/heart_disease_clean_raw.csv"
bronze_table = "health_catalog.bronze.patient_records"

# 1️⃣ Read Raw Data from S3 with schema inference
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

# 2️⃣ Add metadata columns (for traceability)
df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit(raw_path))
)

# 3️⃣ Load into Bronze Delta table
df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

# 4️⃣ Display sample output
display(df_bronze.limit(5))
print("Bronze Layer Loaded Successfully!")


# COMMAND ----------

# DBTITLE 1,Displaying 10 rows 
# MAGIC %sql
# MAGIC SELECT * FROM health_catalog.bronze.patient_records LIMIT 10;
# MAGIC