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

**Business value:** 
Average Length of Stay - helps hospitals optimize bed utilization, control operational costs, and improve patient flow. By analyzing ALOS, management can identify inefficiencies in treatment or discharge processes, ensure better resource allocation, and align hospital operations with financial and clinical performance goals.

Top Disease Per Month - This insight helped management prioritize the procurement of essential medicines and medical supplies, ensuring that frequently used medications were always available and reducing the risk of shortages

Monthly admission trend - This information helped management determine appropriate manpower allocation, ward utilization, and room planning to better accommodate patient demand and improve hospital operations

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
