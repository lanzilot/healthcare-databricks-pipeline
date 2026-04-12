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
   - Average length of stay 
   - Total revenue by service
   - Monthly admission trend
   - Top Disease per month

Top Diseases per Month – Clinical Analytics
## Top Diseases per Month
Using the silver layer (`silver_patient_charges`), Performed additional diagnosis cleaning:
- Split comma‑separated diagnosis strings into individual rows.
- Standardized synonyms (e.g., `HPT` → `hypertension`).
- Filtered out noise (`10`, `unspecified`, `as 9`, `null` dates).
- Aggregated case counts by `year_month` and ranked the top 5 diagnoses per month.

**Gold table created:** `gold_top_diseases_per_month_cleaned`

**Sample output:**
| year_month | diagnosis_std                                              | case_count |
|------------|------------------------------------------------------------|------------|
| 2017-01    | end stage renal disease secondary to hypertensive ...     | 284        |
| 2017-01    | end stage renal disease secondary to diabetic nephropathy | 192        |
| 2017-01    | acute gastroenteritis with moderate dehydration           | 88         |

**Visualization:** Interactive Bar chart showing top diseases per month.

**Business value:** Helps hospital administration identify seasonal disease trends, allocate resources and manpower.

## How to Run
1. Import notebooks into Databricks 
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
