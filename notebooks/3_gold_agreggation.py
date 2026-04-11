from pyspark.sql.functions import *

df_silver = spark.table("default.silver_patient_charges")

# 1. Average length of stay by diagnosis
df_silver.groupBy("finaldiag") \
    .agg(avg("length_of_stay").alias("avg_los_days"), count("*").alias("admission_count")) \
    .orderBy(col("avg_los_days").desc()) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_avg_los_by_diagnosis")

# 2. Revenue by service
df_silver.groupBy("service") \
    .agg(sum("total_charges").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_revenue_by_service")

# 3. Monthly admission trend
df_silver.withColumn("year_month", date_format("admit_datetime", "yyyy-MM")) \
    .groupBy("year_month") \
    .agg(count("adm_no").alias("admissions")) \
    .orderBy("year_month") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("default.gold_admission_trend")

print("✅ Gold tables created")