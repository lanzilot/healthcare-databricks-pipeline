# Save patient bronze table
df_patient_raw.write.format("delta").mode("overwrite") \
    .saveAsTable("default.bronze_patient")

# Save charges bronze table
df_charges_raw.write.format("delta").mode("overwrite") \
    .saveAsTable("default.bronze_charges")

print("✅ Bronze tables saved as default.bronze_patient and default.bronze_charges")
