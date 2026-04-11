# FINAL SILVER LAYER – Handles invalid dates, bad age strings, mixed timestamp formats, and ICD codes as strings
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
from datetime import datetime

# ------------------------------
# 1. UDF: Extract years from age string (e.g., "26 y/o 9 mos." -> 26)
# ------------------------------
def extract_years(age_str):
    if age_str is None:
        return None
    match = re.search(r'(\d+)\s*y/o', str(age_str), re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r'^(\d+)', str(age_str))
    if match:
        return int(match.group(1))
    return None

extract_years_udf = udf(extract_years, IntegerType())

# ------------------------------
# 2. UDF: Parse timestamp with multiple formats (robust)
# ------------------------------
def parse_timestamp_udf(date_str, time_str):
    if date_str is None or time_str is None:
        return None
    datetime_str = f"{date_str} {time_str}"
    formats = [
        "%Y-%m-%d %I:%M%p",      # 2017-01-01 09:15AM
        "%Y-%m-%d %I:%M %p",     # 2017-01-01 12:45 AM
        "%Y-%m-%d %I:%M:%S%p",   # with seconds, no space
        "%Y-%m-%d %I:%M:%S %p",  # with seconds and space
        "%Y-%m-%d %H:%M:%S",     # 24-hour fallback
        "%Y-%m-%d %H:%M"         # 24-hour without seconds
    ]
    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except:
            continue
    return None

parse_timestamp = udf(parse_timestamp_udf, TimestampType())

# ------------------------------
# 3. Read bronze tables
# ------------------------------
df_patient = spark.table("default.bronze_patient")
df_charges = spark.table("default.bronze_charges")

# Force string type for diagnosis and key columns (avoid casting errors)
df_patient = df_patient.withColumn("finaldiag", col("finaldiag").cast("string"))
df_patient = df_patient.withColumn("pin", col("pin").cast("string"))
df_patient = df_patient.withColumn("adm_no", col("adm_no").cast("string"))
df_charges = df_charges.withColumn("pin", col("pin").cast("string"))
df_charges = df_charges.withColumn("adm_no", col("adm_no").cast("string"))

# ------------------------------
# 4. Clean patient data (with robust date parsing)
# ------------------------------
df_patient_clean = df_patient \
    .withColumn("admit_datetime", parse_timestamp(col("dateadmit"), col("timeadmit"))) \
    .withColumn("discharge_datetime", parse_timestamp(col("datedisch"), col("timedisch"))) \
    .withColumn("length_of_stay", datediff(col("discharge_datetime"), col("admit_datetime"))) \
    .withColumn("birthdate_parsed", try_to_date(col("birthdate"), "yyyy-MM-dd")) \
    .withColumn("age_years_from_birth", 
                floor(datediff(col("admit_datetime"), col("birthdate_parsed")) / 365.25).cast("int")) \
    .withColumn("age_from_string", extract_years_udf(col("age"))) \
    .withColumn("age_mismatch_flag",
                when(col("age_from_string").isNotNull() & (col("age_years_from_birth") != col("age_from_string")), 1)
                .otherwise(0)) \
    .withColumn("sex", 
                when(col("sex").isin("M","Male","m","male"), "M")
                .when(col("sex").isin("F","Female","f","female"), "F")
                .otherwise("Unknown")) \
    .withColumn("service", 
                when(col("service").isNull(), "General").otherwise(col("service"))) \
    .drop("birthdate_parsed", "age_years_from_birth", "age_from_string")

# ------------------------------
# 5. Clean charges data (unchanged)
# ------------------------------
df_charges_clean = df_charges \
    .withColumn("calculated_amount", col("qty") * col("price")) \
    .withColumn("amount_discrepancy_flag",
                when(abs(col("amount") - col("calculated_amount")) > 0.01, 1).otherwise(0)) \
    .withColumn("amount_corrected", 
                when(col("amount_discrepancy_flag") == 1, col("calculated_amount")).otherwise(col("amount")))

# Aggregate charges per admission
df_charges_aggregated = df_charges_clean \
    .groupBy("pin", "adm_no") \
    .agg(
        sum("amount_corrected").alias("total_charges"),
        count("*").alias("charge_line_items"),
        sum("amount_discrepancy_flag").alias("discrepancies_count")
    )

# ------------------------------
# 6. Join patient and aggregated charges
# ------------------------------
df_silver = df_patient_clean.join(df_charges_aggregated, on=["pin", "adm_no"], how="left") \
    .fillna({"total_charges": 0, "charge_line_items": 0, "discrepancies_count": 0})

# ------------------------------
# 7. Write silver Delta table
# ------------------------------
df_silver.write.format("delta").mode("overwrite") \
    .partitionBy("service") \
    .saveAsTable("default.silver_patient_charges")

print("Silver table saved successfully!")
row_count = spark.table("default.silver_patient_charges").count()
print("Row count:", row_count)

# Optional: show sample
display(spark.table("default.silver_patient_charges").select("adm_no", "finaldiag", "admit_datetime", "length_of_stay", "total_charges").limit(5))