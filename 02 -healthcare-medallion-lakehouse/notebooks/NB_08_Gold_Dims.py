from pyspark.sql.functions import (
    col, monotonically_increasing_id,
    current_timestamp, lit, row_number,
    trim, initcap
)
from pyspark.sql.window import Window

# Read Silver tables from Silver Lakehouse
# Cross-Lakehouse read — same workspace, no credentials needed
df_patients     = spark.read.format("delta").table(
    "MediCore_Silver_Lakehouse.silver_patients")
df_claims       = spark.read.format("delta").table(
    "MediCore_Silver_Lakehouse.silver_claims")
df_appointments = spark.read.format("delta").table(
    "MediCore_Silver_Lakehouse.silver_appointments")
df_dim_date_src = spark.read.format("delta").table(
    "MediCore_Silver_Lakehouse.dim_date")

print(f"silver_patients:     {df_patients.count()} rows")
print(f"silver_claims:       {df_claims.count()} rows")
print(f"silver_appointments: {df_appointments.count()} rows")
print(f"dim_date (Silver):   {df_dim_date_src.count()} rows")


# Build dim_patient — patient dimension for star schema
df_dim_patient = df_patients.select(
    "PatientID",
    "MRN",
    "FirstName",
    "LastName",
    "DateOfBirth",
    "Age",
    "Gender",
    "BloodType",
    "Ethnicity",
    "State",
    "ZipCode",
    "InsurancePayer",
    "InsurancePayerCode",
    "InsuranceType",
    "PrimaryDiagnosis",
    "PrimaryDiagnosisDesc",
    "Specialty",
    "IsActive"
) \
.withColumn("PatientKey",
    (monotonically_increasing_id() + 1).cast("long")) \
.withColumn("_gold_timestamp", current_timestamp())

# Reorder — key first
df_dim_patient = df_dim_patient.select(
    "PatientKey", "PatientID", "MRN",
    "FirstName", "LastName", "DateOfBirth", "Age", "Gender",
    "BloodType", "Ethnicity", "State", "ZipCode",
    "InsurancePayer", "InsurancePayerCode", "InsuranceType",
    "PrimaryDiagnosis", "PrimaryDiagnosisDesc", "Specialty",
    "IsActive", "_gold_timestamp"
)

print(f"dim_patient rows:    {df_dim_patient.count()}")
print(f"dim_patient columns: {len(df_dim_patient.columns)}")
df_dim_patient.show(3, truncate=False)


# Extract unique providers from silver_claims
# Deduplicate on ProviderID — one row per provider
window_prov = Window.partitionBy("ProviderID").orderBy(
    col("ServiceDate").desc()
)

df_dim_provider = df_claims.select(
    "ProviderID",
    "ProviderName",
    "ProviderSpecialty"
).distinct() \
.withColumn("ProviderKey",
    (monotonically_increasing_id() + 1).cast("long")) \
.withColumn("_gold_timestamp", current_timestamp())

# Reorder — key first
df_dim_provider = df_dim_provider.select(
    "ProviderKey", "ProviderID", "ProviderName",
    "ProviderSpecialty", "_gold_timestamp"
)

print(f"dim_provider rows:    {df_dim_provider.count()}")
print(f"Unique providers:     {df_dim_provider.count()}")
df_dim_provider.show(truncate=False)

# Extract unique facilities from silver_claims
df_dim_facility = df_claims.select(
    "FacilityID",
    "FacilityName",
    "FacilityType"
).distinct() \
.withColumn("FacilityState",
    # Get state by joining back to appointments which has FacilityState
    lit(None).cast("string")) \
.withColumn("FacilityKey",
    (monotonically_increasing_id() + 1).cast("long")) \
.withColumn("_gold_timestamp", current_timestamp())

# Enrich with state from appointments
df_fac_state = df_appointments.select(
    "FacilityID", "FacilityState"
).distinct()

df_dim_facility = df_dim_facility.drop("FacilityState") \
    .join(df_fac_state, on="FacilityID", how="left")

# Reorder — key first
df_dim_facility = df_dim_facility.select(
    "FacilityKey", "FacilityID", "FacilityName",
    "FacilityType", "FacilityState", "_gold_timestamp"
)

print(f"dim_facility rows:    {df_dim_facility.count()}")
df_dim_facility.show(truncate=False)


# Promote dim_date from Silver to Gold — no transformation needed
df_dim_date_gold = df_dim_date_src.drop("_silver_timestamp") \
    .withColumn("_gold_timestamp", current_timestamp())

print(f"dim_date rows: {df_dim_date_gold.count()}")
df_dim_date_gold.select(
    "DateKey", "FullDate", "Year", "MonthName",
    "Quarter", "DayName", "IsWeekend"
).show(5, truncate=False)

# Write dim_patient
df_dim_patient.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_patient")
print(f"dim_patient written:  {spark.table('dim_patient').count()} rows ✅")

# Write dim_provider
df_dim_provider.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_provider")
print(f"dim_provider written: {spark.table('dim_provider').count()} rows ✅")

# Write dim_facility
df_dim_facility.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_facility")
print(f"dim_facility written: {spark.table('dim_facility').count()} rows ✅")

# Write dim_date
df_dim_date_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")
print(f"dim_date written:     {spark.table('dim_date').count()} rows ✅")

print("\n=== All Gold Dims Complete ===")


print("=== Gold Dimension Tables Summary ===\n")

for table in ["dim_patient", "dim_provider", "dim_facility", "dim_date"]:
    df_t   = spark.table(table)
    count  = df_t.count()
    cols   = len(df_t.columns)
    key_col = [c for c in df_t.columns if c.endswith("Key")][0]
    unique_keys = df_t.select(key_col).distinct().count()
    print(f"{table:20s}  rows: {count:5d}  cols: {cols:3d}  unique keys: {unique_keys}")

print("\ndim_patient sample:")
spark.sql("""
    SELECT PatientKey, PatientID, FirstName, LastName,
           State, InsurancePayer, PrimaryDiagnosis
    FROM dim_patient
    LIMIT 5
""").show(truncate=False)

print("\ndim_provider:")
spark.sql("SELECT * FROM dim_provider").show(truncate=False)

print("\ndim_facility:")
spark.sql("SELECT * FROM dim_facility").show(truncate=False)

print("\ndim_date sample:")
spark.sql("""
    SELECT DateKey, FullDate, Year, MonthName,
           Quarter, DayName, IsWeekend, FiscalYear
    FROM dim_date
    LIMIT 5
""").show(truncate=False)
