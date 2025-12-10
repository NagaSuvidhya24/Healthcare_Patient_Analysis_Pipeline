# Databricks notebook source
# MAGIC %md
# MAGIC ### BP Risk Category Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     bp_category, 
# MAGIC     COUNT(*) AS total_patients
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC GROUP BY bp_category
# MAGIC ORDER BY total_patients DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gender-Wise Heart Risk Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     sex,
# MAGIC     COUNT(*) AS total_patients,
# MAGIC     AVG(risk_score) AS avg_risk_score
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC GROUP BY sex;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Age Group-based Risk Pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN age < 40 THEN 'YOUNG'
# MAGIC         WHEN age BETWEEN 40 AND 60 THEN 'MIDDLE AGED'
# MAGIC         ELSE 'SENIOR'
# MAGIC     END AS age_group,
# MAGIC     COUNT(*) AS total_patients,
# MAGIC     AVG(risk_score) AS avg_risk_score
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC GROUP BY age_group
# MAGIC ORDER BY avg_risk_score DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### BP + Cholesterol + Risk Insights Together

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     bp_category,
# MAGIC     chol_category,
# MAGIC     AVG(risk_score) AS avg_risk_score,
# MAGIC     COUNT(*) AS total_patients
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC GROUP BY bp_category, chol_category
# MAGIC ORDER BY avg_risk_score DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Chest Pain Type vs Average Risk Score

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     cp AS chest_pain_type,
# MAGIC     AVG(risk_score) AS avg_risk_score,
# MAGIC     COUNT(*) AS total_patients
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC GROUP BY cp
# MAGIC ORDER BY avg_risk_score DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 High-Risk Patients (for Clinical Attention)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     id, age, sex, risk_score, bp_category, chol_category
# MAGIC FROM health_catalog.gold.patient_gold
# MAGIC WHERE risk_score >= 3
# MAGIC ORDER BY risk_score DESC, age DESC
# MAGIC LIMIT 50;