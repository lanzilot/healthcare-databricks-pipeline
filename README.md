# Healthcare Patient Analytics on Databricks

## Overview
End‑to‑data pipeline on Databricks using the medallion architecture (bronze → silver → gold).  
Cleans messy healthcare data, joins patient admissions with charges, and produces KPIs.

## Data Source
- `raw_patient.csv` – patient demographics, admission dates, diagnosis
- `raw_charges.csv` – line‑item charges per admission

## Pipeline Steps
1. **Bronze** – ingest CSVs into Delta tables
2. **Silver** – clean timestamps (handles both `09:15AM` and `12:45 AM`), extract numeric age from strings like `26 y/o 9 mos.`, compute length of stay, flag age mismatches, aggregate charges, join tables
3. **Gold** – create aggregated views:
   - Average length of stay by diagnosis
   - Total revenue by service
   - Monthly admission trend

## How to Run
1. Import notebooks into Databricks (Community Edition or full workspace)
2. Upload the sample CSVs to a Unity Catalog volume or DBFS
3. Run notebooks in order: `1_bronze_ingestion` → `2_silver_cleaning` → `3_gold_aggregation`

## Results
- Silver table: `default.silver_patient_charges` (41,213 admissions)
- Gold tables: KPIs ready for dashboards

## Built With
- Databricks Runtime (serverless or cluster)
- PySpark, Delta Lake, Unity Catalog

## Author
Rolando C. Abucejo 
