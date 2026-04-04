from pyspark.sql.functions import (
    col, trim, initcap, upper, current_timestamp,
    lit, when, to_date, to_timestamp,
    regexp_replace, datediff, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Read bronze_appointments via shortcut in Silver Lakehouse
df = spark.table("bronze_appointments")

raw_count = df.count()
print(f"Records read from bronze_appointments: {raw_count}")
print("\nSchema (all strings in Bronze — expected):")
df.printSchema()

df_typed = df \
    .withColumn("ScheduledDateTime",
        to_timestamp(col("ScheduledDateTime"))) \
    .withColumn("ScheduledEndDateTime",
        to_timestamp(col("ScheduledEndDateTime"))) \
    .withColumn("ActualEndDateTime",
        to_timestamp(
            regexp_replace(col("ActualEndDateTime"), "[^0-9\\- :]", "")
        )) \
    .withColumn("DurationScheduledMins",
        col("DurationScheduledMins").cast("integer")) \
    .withColumn("DurationActualMins",
        col("DurationActualMins").cast("integer")) \
    .withColumn("WaitTimeMins",
        col("WaitTimeMins").cast("integer")) \
    .withColumn("PatientAge",
        col("PatientAge").cast("integer")) \
    .withColumn("CopayAmount",
        col("CopayAmount").cast("double")) \
    .withColumn("FollowUpDays",
        col("FollowUpDays").cast("integer")) \
    .withColumn("IsTelemedicine",
        when(upper(trim(col("IsTelemedicine"))) == "Y", True)
        .otherwise(False)) \
    .withColumn("FollowUpRequired",
        when(upper(trim(col("FollowUpRequired"))) == "Y", True)
        .otherwise(False)) \
    .withColumn("LastModifiedDate",
        to_timestamp(
            regexp_replace(col("LastModifiedDate"), "[^0-9\\- :]", "")
        ))

print("All types cast successfully ✅")
print(f"\nSample ScheduledDateTime: {df_typed.select('ScheduledDateTime').first()[0]}")
print(f"Sample DurationScheduledMins: {df_typed.select('DurationScheduledMins').first()[0]}")


df_clean = df_typed

# Rule 1: Drop null AppointmentID — primary key must exist
before = df_clean.count()
df_clean = df_clean.filter(col("AppointmentID").isNotNull())
print(f"Rule 1 — Null AppointmentID dropped:       {before - df_clean.count()} rows")

# Rule 2: Drop null PatientID — cannot link appointment to a patient
before = df_clean.count()
df_clean = df_clean.filter(col("PatientID").isNotNull())
print(f"Rule 2 — Null PatientID dropped:            {before - df_clean.count()} rows")

# Rule 3: Drop null ScheduledDateTime — appointment must have a time
before = df_clean.count()
df_clean = df_clean.filter(col("ScheduledDateTime").isNotNull())
print(f"Rule 3 — Null ScheduledDateTime dropped:    {before - df_clean.count()} rows")

# Rule 4: Drop negative scheduled durations — physically impossible
before = df_clean.count()
df_clean = df_clean.filter(
    col("DurationScheduledMins").isNull() |
    (col("DurationScheduledMins") >= 0)
)
print(f"Rule 4 — Negative duration dropped:         {before - df_clean.count()} rows")

# Rule 5: Standardise AppointmentStatus — trim + title case
# Handles: "no-show", "NO SHOW", "No-Show " → "No-Show"
df_clean = df_clean.withColumn("AppointmentStatus",
    initcap(trim(col("AppointmentStatus"))))
print("Rule 5 — AppointmentStatus standardised ✅")

# Rule 6: Standardise AppointmentType
df_clean = df_clean.withColumn("AppointmentType",
    initcap(trim(col("AppointmentType"))))
print("Rule 6 — AppointmentType standardised ✅")

# Rule 7: Add IsNoShow flag — key clinical KPI
# No-show appointments have status No-Show and actual duration = 0
df_clean = df_clean.withColumn("IsNoShow",
    when(col("AppointmentStatus") == "No-show", True)
    .otherwise(False))
print("Rule 7 — IsNoShow flag added ✅")

# Rule 8: Add EfficiencyRatio — actual vs scheduled duration
# Completed appointments only — tells us how well time is being used
# 1.0 = perfectly on time, < 1.0 = finished early, > 1.0 = ran over
df_clean = df_clean.withColumn("EfficiencyRatio",
    when(
        (col("AppointmentStatus") == "Completed") &
        (col("DurationScheduledMins") > 0),
        spark_round(
            col("DurationActualMins") / col("DurationScheduledMins"),
            2
        )
    ).otherwise(lit(None)))
print("Rule 8 — EfficiencyRatio calculated ✅")

# Rule 9: Deduplicate on AppointmentID — keep most recent record
window_spec = Window.partitionBy("AppointmentID").orderBy(
    col("LastModifiedDate").desc()
)
before = df_clean.count()
df_clean = df_clean \
    .withColumn("_row_num", row_number().over(window_spec)) \
    .filter(col("_row_num") == 1) \
    .drop("_row_num")
print(f"Rule 9 — Duplicates removed:                {before - df_clean.count()} rows")

print(f"\nFinal clean count: {df_clean.count()} of {raw_count} passed")

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

print(f"Final Silver columns: {len(df_silver.columns)}")
print("Audit columns added ✅")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_appointments")

final_count = spark.table("silver_appointments").count()
print(f"silver_appointments written: {final_count} rows ✅")

print("=== Silver Appointments Summary ===")

# Total row count
print(f"Total rows: {spark.table('silver_appointments').count()}")

# Appointment status distribution
# Expected: Completed ~72%, No-Show ~13%, Cancelled ~9%
print("\nStatus distribution:")
spark.sql("""
    SELECT
        AppointmentStatus,
        COUNT(*)                            AS Count,
        ROUND(COUNT(*) * 100.0
            / SUM(COUNT(*)) OVER(), 1)      AS Pct
    FROM silver_appointments
    GROUP BY AppointmentStatus
    ORDER BY Count DESC
""").show()

# No-show rate — headline clinical KPI
print("\nNo-show rate:")
spark.sql("""
    SELECT
        COUNT(*)                                                        AS TotalAppointments,
        SUM(CASE WHEN IsNoShow = true THEN 1 ELSE 0 END)               AS NoShows,
        ROUND(
            SUM(CASE WHEN IsNoShow = true THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100, 1
        )                                                               AS NoShowRatePct
    FROM silver_appointments
""").show()

# Average wait time by appointment type
print("\nAvg wait time by appointment type:")
spark.sql("""
    SELECT
        AppointmentType,
        COUNT(*)                        AS Appointments,
        ROUND(AVG(WaitTimeMins), 1)     AS AvgWaitMins,
        ROUND(AVG(CopayAmount), 2)      AS AvgCopay
    FROM silver_appointments
    WHERE AppointmentStatus = 'Completed'
    GROUP BY AppointmentType
    ORDER BY AvgWaitMins DESC
""").show()

# Telemedicine vs in-person breakdown
print("\nTelemedicine breakdown:")
spark.sql("""
    SELECT
        IsTelemedicine,
        COUNT(*)    AS Appointments,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS Pct
    FROM silver_appointments
    GROUP BY IsTelemedicine
""").show()

# Efficiency ratio for completed appointments
print("\nEfficiency ratio (completed appointments):")
spark.sql("""
    SELECT
        ROUND(AVG(EfficiencyRatio), 2)  AS AvgEfficiency,
        ROUND(MIN(EfficiencyRatio), 2)  AS MinEfficiency,
        ROUND(MAX(EfficiencyRatio), 2)  AS MaxEfficiency
    FROM silver_appointments
    WHERE EfficiencyRatio IS NOT NULL
""").show()
