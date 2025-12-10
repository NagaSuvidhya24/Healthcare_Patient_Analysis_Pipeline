# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_catalog.gold.patient_gold;
# MAGIC DROP TABLE IF EXISTS health_catalog.gold.patient_summary;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Analytics and Risk Scoring
from pyspark.sql.functions import (
    col, when, avg, count, current_timestamp
)

# Table Names
silver_table = "health_catalog.silver.patient_cleaned"
gold_table = "health_catalog.gold.patient_gold"
gold_summary_table = "health_catalog.gold.patient_summary"

# 1️ Load Silver Data
df_silver = spark.read.table(silver_table)

# 2️ GOLD Transformations + Risk Score
df_gold = (
    df_silver
    .withColumn(
        "bp_category",
        when(col("trestbps") < 120, "NORMAL")
        .when(col("trestbps") < 140, "ELEVATED")
        .when(col("trestbps") < 160, "HIGH STAGE 1")
        .otherwise("HIGH STAGE 2")
    )
    .withColumn(
        "chol_category",
        when(col("chol") < 200, "NORMAL")
        .when(col("chol") < 240, "BORDERLINE HIGH")
        .otherwise("HIGH")
    )
    .withColumn(
        "risk_score",
        (when(col("bp_category") == "HIGH STAGE 2", 2)
        .when(col("bp_category") == "HIGH STAGE 1", 1)
        .otherwise(0)
        +
        when(col("chol_category") == "HIGH", 2)
        .when(col("chol_category") == "BORDERLINE HIGH", 1)
        .otherwise(0))
    )
    .withColumn("gold_load_timestamp", current_timestamp())
)

# 3️ Save GOLD Table (Schema Overwrite Enabled)
df_gold.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(gold_table)

print(" Gold Table Created Successfully!")

# 4️ Summary Insights Table
df_summary = (
    df_gold.groupBy("sex")
    .agg(
        count("*").alias("total_patients"),
        avg("age").alias("avg_age"),
        avg("trestbps").alias("avg_resting_bp"),
        avg("chol").alias("avg_cholesterol"),
        count(when(col("bp_category") == "HIGH STAGE 2", True)).alias("high_bp_patients"),
        count(when(col("chol_category") == "HIGH", True)).alias("high_chol_patients"),
        avg("risk_score").alias("avg_risk_score")
    )
    .withColumn("summary_load_timestamp", current_timestamp())
)

# 5️ Save GOLD Summary Table (Schema Overwrite Enabled)
df_summary.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(gold_summary_table)

print("Gold Summary Table Created Successfully!")


# COMMAND ----------

spark.sql("SELECT * FROM health_catalog.gold.patient_gold LIMIT 10").show()
spark.sql("SELECT * FROM health_catalog.gold.patient_summary LIMIT 10").show()
