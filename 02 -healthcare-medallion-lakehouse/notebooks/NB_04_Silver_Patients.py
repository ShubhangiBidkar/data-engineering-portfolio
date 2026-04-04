from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Read bronze_patients via shortcut already set up in Silver Lakehouse

df = spark.table("bronze_patients")

raw_count = df.count()
print(f"Records from bronze_patients : {raw_count}")
print("Schema")
df.printSchema()


# Bronze stores everything as strings
# Silver casts to proper types before applying quality rules


df_typed = (
    df
    .withColumn("DateOfBirth",to_date(col("DateOfBirth"),"yyyy-MM-dd"))
    .withColumn("Age",col("Age").cast("integer"))
    .withColumn("RegistrationDate",to_timestamp(col("RegistrationDate")))
    .withColumn("LastModifiedDate",to_timestamp(regexp_replace(col("LastModifiedDate"), "[^0-9\\- :]", "")))
    .withColumn("IsActive",when(upper(trim(col("IsActive")))== 'Y',True).otherwise(False)) 
)

print("Types cast successfully ✅")
print(f"Sample LastModifiedDate: {df_typed.select('LastModifiedDate').first()[0]}")


# Apply quality rules one by one
# Print count after each rule so you can see exactly what was dropped


df_clean = df_typed

# Rule 1: Drop null PatientID — cannot identify or join without it
before = df_clean.count()
df_clean =df_clean.filter(col("PatientID").isNotNull())
print(f"Rule 1 — Null PatientID dropped:     {before - df_clean.count()} rows")

# Rule 2: Drop invalid ages
before = df_clean.count()
df_clean = (
    df_clean.filter(
        col('Age').isNotNull() &
        (col('Age') >= 0) &
        (col('Age')<=120)
    )
)
print(f"Rule 2 — Invalid age dropped:         {before - df_clean.count()} rows")

# Rule 3: Drop null LastModifiedDate — watermark column must exist
before = df_clean.count()
df_clean = df_clean.filter(col("LastModifiedDate").isNotNull())
print(f"Rule 3 — Null LastModifiedDate:       {before - df_clean.count()} rows")

# Rule 4: Standardise Gender
# Rule 4: Standardise Gender
df_clean = df_clean.withColumn("Gender",
    when(upper(trim(col("Gender"))) == "MALE",   lit("Male"))
   .when(upper(trim(col("Gender"))) == "FEMALE", lit("Female"))
   .otherwise(lit("Unknown")))
print("Rule 4 — Gender standardised ✅")

# Rule 5: Standardise State to uppercase 2-letter code
df_clean = df_clean.withColumn("State",
    upper(trim(col("State"))))
print("Rule 5 — State standardised ✅")

# Rule 6: Standardise InsuranceType
df_clean = df_clean.withColumn("InsuranceType",
    initcap(trim(col("InsuranceType"))))
print("Rule 6 — InsuranceType standardised ✅")

# Rule 7: Validate ICD-10 format — must start with letter followed by digits
df_clean = df_clean.withColumn("ICD10_Valid",
    col("PrimaryDiagnosis").rlike("^[A-Z][0-9]"))
print("Rule 7 — ICD10 validation flag added ✅")

# Rule 8: Deduplicate on PatientID — keep most recent record
window_spec = Window.partitionBy("PatientID").orderBy(
    col("LastModifiedDate").desc()
)
before = df_clean.count()
df_clean = df_clean \
    .withColumn("_row_num", row_number().over(window_spec)) \
    .filter(col("_row_num") == 1) \
    .drop("_row_num")
print(f"Rule 8 — Duplicates removed:          {before - df_clean.count()} rows")

print(f"\nFinal clean count: {df_clean.count()} of {raw_count} records passed")



# Add audit columns for lineage tracking
df_silver = df_clean \
    .withColumn("_silver_timestamp", current_timestamp()) \
    .withColumn("_source_layer",     lit("Bronze")) \
    .withColumn("_quality_passed",   lit(True))

# Drop Bronze-specific audit columns — not needed in Silver
df_silver = df_silver.drop(
    "_ingestion_timestamp",
    "_source_system",
    "_load_pattern"
)

print(f"Silver columns: {len(df_silver.columns)}")


# Full overwrite Silver on every run
# Silver is always rebuilt from Bronze — idempotent by design
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_patients")

final_count = spark.table("silver_patients").count()
print(f"silver_patients written: {final_count} rows ✅")


print("=== Silver Patients Summary ===")

# Row count
print(f"Total rows: {spark.table('silver_patients').count()}")

# Gender distribution
spark.sql("""
    SELECT   Gender, COUNT(*) AS Count
    FROM     silver_patients
    GROUP BY Gender
    ORDER BY Count DESC
""").show()

# Insurance type breakdown
spark.sql("""
    SELECT   InsuranceType, COUNT(*) AS Patients
    FROM     silver_patients
    GROUP BY InsuranceType
    ORDER BY Patients DESC
""").show()

# ICD-10 validation check
spark.sql("""
    SELECT   ICD10_Valid, COUNT(*) AS Count
    FROM     silver_patients
    GROUP BY ICD10_Valid
""").show()

# Active vs inactive
spark.sql("""
    SELECT   IsActive, COUNT(*) AS Count
    FROM     silver_patients
    GROUP BY IsActive
""").show()

# Sample
spark.table("silver_patients") \
    .select("PatientID", "FirstName", "LastName",
            "Age", "Gender", "State",
            "InsurancePayer", "PrimaryDiagnosis",
            "ICD10_Valid", "IsActive") \
    .show(5, truncate=False)
