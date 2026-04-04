from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# All columns loaded as strings — Bronze preserves raw source fidelity
# Type casting and validation happens in Silver

schema = StructType([
    StructField("AppointmentID",         StringType(), False),
    StructField("PatientID",             StringType(), True),
    StructField("MRN",                   StringType(), True),
    StructField("PatientName",           StringType(), True),
    StructField("PatientDOB",            StringType(), True),
    StructField("PatientAge",            StringType(), True),
    StructField("InsurancePayer",        StringType(), True),
    StructField("ProviderID",            StringType(), True),
    StructField("ProviderName",          StringType(), True),
    StructField("ProviderSpecialty",     StringType(), True),
    StructField("ProviderRole",          StringType(), True),
    StructField("FacilityID",            StringType(), True),
    StructField("FacilityName",          StringType(), True),
    StructField("FacilityType",          StringType(), True),
    StructField("FacilityState",         StringType(), True),
    StructField("AppointmentType",       StringType(), True),
    StructField("AppointmentStatus",     StringType(), True),
    StructField("ScheduledDateTime",     StringType(), True),
    StructField("ScheduledEndDateTime",  StringType(), True),
    StructField("ActualEndDateTime",     StringType(), True),
    StructField("DurationScheduledMins", StringType(), True),
    StructField("DurationActualMins",    StringType(), True),
    StructField("WaitTimeMins",          StringType(), True),
    StructField("DiagnosisCode",         StringType(), True),
    StructField("DiagnosisDescription",  StringType(), True),
    StructField("MedicalSpecialty",      StringType(), True),
    StructField("ReferralSource",        StringType(), True),
    StructField("IsTelemedicine",        StringType(), True),
    StructField("FollowUpRequired",      StringType(), True),
    StructField("FollowUpDays",          StringType(), True),
    StructField("CopayAmount",           StringType(), True),
    StructField("Notes",                 StringType(), True),
    StructField("LastModifiedDate",      StringType(), True),
])

df_appointments = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("multiLine", "false") \
    .schema(schema) \
    .load("Files/raw_ingestion/healthcare/appointments.csv")

raw_count = df_appointments.count()
print(f"Records read from appointments.csv: {raw_count}")



df_bronze_appointments = df_appointments \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system",       lit("ADLS_Gen2_appointments_csv")) \
    .withColumn("_load_pattern",        lit("Full overwrite"))

print(f"Columns: {len(df_bronze_appointments.columns)}")



df_bronze_appointments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_appointments")

final_count = spark.table("bronze_appointments").count()
print(f"bronze_appointments loaded: {final_count} rows ✅")



print("=== Bronze Appointments Summary ===")

# Row count
print(f"Total rows: {spark.table('bronze_appointments').count()}")

# Appointment status distribution
spark.sql("""
    SELECT   AppointmentStatus,
             COUNT(*) AS Count
    FROM     bronze_appointments
    GROUP BY AppointmentStatus
    ORDER BY Count DESC
""").show()

# Appointment type breakdown
spark.sql("""
    SELECT   AppointmentType,
             COUNT(*) AS Count
    FROM     bronze_appointments
    GROUP BY AppointmentType
    ORDER BY Count DESC
""").show()

# No-show rate (key clinical KPI)
spark.sql("""
    SELECT
        ROUND(
            SUM(CASE WHEN AppointmentStatus = 'No-Show' THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100, 1
        ) AS NoShowRatePct
    FROM bronze_appointments
""").show()

# Sample records
spark.table("bronze_appointments") \
    .select("AppointmentID", "PatientID", "ProviderName",
            "AppointmentType", "AppointmentStatus", "ScheduledDateTime") \
    .show(5, truncate=False)
