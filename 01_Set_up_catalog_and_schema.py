# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS health_catalog;
# MAGIC
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS health_catalog.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS health_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS health_catalog.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS health_catalog.logs;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS health_catalog.bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS health_catalog.silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS health_catalog.gold CASCADE;
# MAGIC