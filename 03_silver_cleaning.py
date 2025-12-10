# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_catalog.silver.patient_cleaned PURGE;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Data Transformation and Standardization
import re
from pyspark.sql.functions import col, trim, upper, current_timestamp, lit
from pyspark.sql.types import IntegerType, DoubleType

# Table Paths
bronze_table = "health_catalog.bronze.patient_records"
silver_table = "health_catalog.silver.patient_cleaned"
error_table = "health_catalog.logs.etl_errors"

# Load Bronze Data
df_silver = spark.read.table(bronze_table)

# 1️ Clean Column Names
clean_cols = [re.sub(r'[ ,;{}()\n\t=]', '_', c) for c in df_silver.columns]
df_silver = df_silver.toDF(*clean_cols)

# Fix column names starting with '_'
for col_name in df_silver.columns:
    if col_name.startswith("_"):
        df_silver = df_silver.withColumnRenamed(col_name, f"col{col_name}")

# 2️ Remove Duplicate Records
df_silver = df_silver.dropDuplicates()

# 3️ Cast Numeric Fields
numeric_cols = ["oldpeak", "age", "trestbps", "chol", "thalch", "ca", "num"]
for col_name in numeric_cols:
    if col_name in df_silver.columns:
        df_silver = df_silver.withColumn(col_name, col(col_name).cast(IntegerType()))

# 4️ Clean String Columns
string_cols = ["sex", "cp", "fbs", "restecg", "thal", "slope", "exang"]
for col_name in string_cols:
    if col_name in df_silver.columns:
        df_silver = df_silver.withColumn(col_name, trim(upper(col(col_name))))

# 5️ Data Quality Check — Log NULL values in critical columns
bad_records = df_silver.filter(
    col("age").isNull() |
    col("trestbps").isNull() |
    col("chol").isNull()
)

if bad_records.count() > 0:
    print("Logging bad records into ETL error table!")
    
    bad_records.withColumn("error_message", lit("Missing important medical values")) \
               .withColumn("error_time", current_timestamp()) \
               .select("error_message", "error_time") \
               .write.format("delta").mode("append").saveAsTable(error_table)

# Remove NULL rows from main dataset
df_silver = df_silver.filter(
    col("age").isNotNull() &
    col("trestbps").isNotNull() &
    col("chol").isNotNull()
)

# 6️ Add Metadata column
df_silver = df_silver.withColumn("silver_load_time", current_timestamp())

# 7️ Write Output Table
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print("Silver Layer processed successfully!")
