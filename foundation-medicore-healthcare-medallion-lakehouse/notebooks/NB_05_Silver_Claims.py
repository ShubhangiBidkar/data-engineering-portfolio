from pyspark.sql.functions import (
    col, trim, initcap, upper, current_timestamp,
    lit, when, to_date, to_timestamp,
    regexp_replace, round as spark_round
)

# Read bronze_claims via shortcut
df = spark.table("bronze_claims")

raw_count = df.count()
print(f"Records read from bronze_claims: {raw_count}")

# Cast all numeric and date columns from string to proper types
# Bronze stored everything as strings — Silver enforces types

df_typed = df \
    .withColumn("ServiceDate",
        to_date(col("ServiceDate"), "yyyy-MM-dd")) \
    .withColumn("SubmissionDate",
        to_date(col("SubmissionDate"), "yyyy-MM-dd")) \
    .withColumn("ProcessedDate",
        to_date(col("ProcessedDate"), "yyyy-MM-dd")) \
    .withColumn("BilledAmount",
        col("BilledAmount").cast("double")) \
    .withColumn("ApprovedAmount",
        col("ApprovedAmount").cast("double")) \
    .withColumn("DeniedAmount",
        col("DeniedAmount").cast("double")) \
    .withColumn("PatientResponsibility",
        col("PatientResponsibility").cast("double")) \
    .withColumn("LengthOfStay",
        col("LengthOfStay").cast("integer")) \
    .withColumn("LastModifiedDate",
        to_timestamp(
            regexp_replace(col("LastModifiedDate"), "[^0-9\\- :]", "")
        ))

print("Types cast successfully ✅")


df_clean = df_typed

# Rule 1: Drop null ClaimID — primary key must exist
before = df_clean.count()
df_clean = df_clean.filter(col("ClaimID").isNotNull())
print(f"Rule 1 — Null ClaimID dropped:          {before - df_clean.count()} rows")

# Rule 2: Drop null PatientID — cannot join to dim_patient without it
before = df_clean.count()
df_clean = df_clean.filter(col("PatientID").isNotNull())
print(f"Rule 2 — Null PatientID dropped:         {before - df_clean.count()} rows")

# Rule 3: Drop negative billed amounts — invalid financial record
before = df_clean.count()
df_clean = df_clean.filter(
    col("BilledAmount").isNotNull() &
    (col("BilledAmount") > 0)
)
print(f"Rule 3 — Invalid BilledAmount dropped:   {before - df_clean.count()} rows")

# Rule 4: Drop where ApprovedAmount exceeds BilledAmount
# Approved cannot be more than what was billed — data error
before = df_clean.count()
df_clean = df_clean.filter(
    col("ApprovedAmount").isNull() |
    (col("ApprovedAmount") <= col("BilledAmount"))
)
print(f"Rule 4 — ApprovedAmount > Billed:        {before - df_clean.count()} rows")

# Rule 5: Standardise ClaimStatus — trim and title case
df_clean = df_clean.withColumn("ClaimStatus",
    initcap(trim(col("ClaimStatus"))))
print("Rule 5 — ClaimStatus standardised ✅")

# Rule 6: Validate ICD-10 format
df_clean = df_clean.withColumn("ICD10_Valid",
    col("DiagnosisCode").rlike("^[A-Z][0-9]"))
print("Rule 6 — ICD10 validation flag added ✅")

# Rule 7: Fill null DenialReason for non-denied claims
df_clean = df_clean.withColumn("DenialReason",
    when(
        col("ClaimStatus") == "Denied",
        when(col("DenialReason").isNull(), lit("Reason not specified"))
        .otherwise(col("DenialReason"))
    ).otherwise(lit("N/A")))
print("Rule 7 — DenialReason standardised ✅")

# Rule 8: Add calculated column — denial flag
df_clean = df_clean.withColumn("IsDenied",
    when(col("ClaimStatus") == "Denied", True)
    .otherwise(False))
print("Rule 8 — IsDenied flag added ✅")

# Rule 9: Add claim processing lag in days
from pyspark.sql.functions import datediff
df_clean = df_clean.withColumn("ClaimLagDays",
    datediff(col("SubmissionDate"), col("ServiceDate")))
print("Rule 9 — ClaimLagDays calculated ✅")

# Rule 10: Deduplicate on ClaimID — keep latest
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("ClaimID").orderBy(
    col("LastModifiedDate").desc()
)
before = df_clean.count()
df_clean = df_clean \
    .withColumn("_row_num", row_number().over(window_spec)) \
    .filter(col("_row_num") == 1) \
    .drop("_row_num")
print(f"Rule 10 — Duplicates removed:            {before - df_clean.count()} rows")

print(f"\nFinal clean count: {df_clean.count()} of {raw_count} passed")


df_silver = df_clean \
    .withColumn("_silver_timestamp", current_timestamp()) \
    .withColumn("_source_layer",     lit("Bronze")) \
    .withColumn("_quality_passed",   lit(True))

# Drop Bronze audit columns
df_silver = df_silver.drop(
    "_ingestion_timestamp",
    "_source_system",
    "_load_pattern"
)

print(f"Silver columns: {len(df_silver.columns)}")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_claims")

final_count = spark.table("silver_claims").count()
print(f"silver_claims written: {final_count} rows ✅")


print("=== Silver Claims Summary ===")

# Row count
print(f"Total rows: {spark.table('silver_claims').count()}")

# Claim status distribution
spark.sql("""
    SELECT   ClaimStatus,
             COUNT(*)                               AS Claims,
             ROUND(SUM(BilledAmount), 2)            AS TotalBilled,
             ROUND(AVG(BilledAmount), 2)            AS AvgBilled,
             ROUND(AVG(ApprovedAmount), 2)          AS AvgApproved
    FROM     silver_claims
    GROUP BY ClaimStatus
    ORDER BY Claims DESC
""").show()

# Denial rate by payer (key Power BI KPI)
spark.sql("""
    SELECT
        InsurancePayer,
        COUNT(*)                                                     AS TotalClaims,
        SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1 ELSE 0 END)     AS DeniedClaims,
        ROUND(
            SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100, 1
        )                                                            AS DenialRatePct
    FROM silver_claims
    GROUP BY InsurancePayer
    ORDER BY DenialRatePct DESC
""").show()

# Average claim processing lag
spark.sql("""
    SELECT
        ROUND(AVG(ClaimLagDays), 1) AS AvgClaimLagDays,
        MIN(ClaimLagDays)           AS MinLag,
        MAX(ClaimLagDays)           AS MaxLag
    FROM silver_claims
    WHERE ClaimLagDays >= 0
""").show()

# ICD-10 validation
spark.sql("""
    SELECT ICD10_Valid, COUNT(*) AS Count
    FROM   silver_claims
    GROUP BY ICD10_Valid
""").show()
