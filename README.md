Health Patient Care Analytics Pipeline

A scalable healthcare data engineering solution using Databricks to ingest, clean, transform, and analyze patient data to support better decision-making and patient care.

Project Architecture

This project follows the Medallion Architecture:

Layer	Purpose:
Bronze	Raw ingestion of patient data
Silver	Cleaned & validated data with business logic
Gold	Aggregated insights for dashboards

Technology Stack
Component	Technology
Data Platform	Databricks
Orchestration	Databricks Workflows
Storage	AWS S3 + Delta Lake
Processing	PySpark / Spark SQL
Governance	Unity Catalog
Visualization	Databricks Dashboard
📂 Repository Structure
📁 src
 ├── 01_setup_catalog_and_schema
 ├── 02_ingest_bronze
 ├── 03_transform_silver
 ├── 04_aggregate_gold
 └── dashboard

Databricks Workflow (Job Orchestration)

The ETL pipeline is automated using Databricks Workflows.
✔ We must manually create a Workflow in Databrick
After importing notebooks, a new Workflow (Job) should be created:
Databricks UI → Workflows → Create Job → Add Tasks in order below

Order	Task Name	Notebook	Purpose
1️.Setup Catalog and Schema	01_setup_catalog_and_schema	Create catalog, schemas & tables
2️.Bronze Layer Ingestion	02_ingest_bronze	Load raw data into Bronze tables
3️.Silver Transformations	03_transform_silver	Cleaning, validations, deduplication
4️.Gold Layer Aggregations	04_aggregate_gold	Build analytics-ready datasets

Each task runs on an all-purpose or job cluster.

Scheduling

✔ Job scheduled daily at 6:00 AM IST
✔ Fully automated execution — no manual trigger needed

Monitoring & Alerts
Feature	Description
Failure Alerts	Email notifications on task failures
Error Logging	Failures stored in health_catalog.logs.etl_errors Delta table
Observability	Job run history stored within Databricks

Dataset Information
Source: Kaggle Heart Disease Dataset
Example fields include:
Patient demographics (age, sex)
Vitals (blood pressure, cholesterol)
Clinical indicators (thalassemia, chest pain type)


Dashboard Insights
Gold layer dataset drives dashboards to analyze:
High-risk patient identification

Age-wise disease distribution

Vital metrics comparisons

Heart disease trends

✨ Key Features

Fully automated data pipeline

Delta Lake ACID capabilities + Time Travel

Data quality validation and error tracking

Scalable Spark-based processing

Secure governance using Unity Catalog

▶️ How to Run

1️. Import notebooks into Databricks
2️. Create external location connected to AWS S3
3️. Create Workflow → Add all tasks → Set schedule
4️. Validate output in Bronze → Silver → Gold tables
5️. View dashboards for final analytics

Future Enhancements

Streaming ingestion using Auto Loader

ML-based patient risk prediction

CI/CD automation using Databricks Repos

Data lineage visualization

