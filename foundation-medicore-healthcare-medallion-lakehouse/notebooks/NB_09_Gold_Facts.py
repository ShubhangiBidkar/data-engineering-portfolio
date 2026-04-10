from pyspark.sql.functions import (
    col, current_timestamp, lit,
    when, round as spark_round, coalesce
)

# Read Silver claims
df_claims = spark.read.format("delta").table(
    "MediCore_Silver_Lakehouse.silver_claims")

# Read Gold dimension tables — these have the surrogate keys
df_dim_patient  = spark.table("dim_patient")
df_dim_provider = spark.table("dim_provider")
df_dim_facility = spark.table("dim_facility")
df_dim_date     = spark.table("dim_date")

print(f"silver_claims:   {df_claims.count()} rows")
print(f"dim_patient:     {df_dim_patient.count()} rows")
print(f"dim_provider:    {df_dim_provider.count()} rows")
print(f"dim_facility:    {df_dim_facility.count()} rows")
print(f"dim_date:        {df_dim_date.count()} rows")


# Step 1: Prepare dim key lookups — only the columns needed for joining
dim_patient_keys  = df_dim_patient.select("PatientKey",  "PatientID")
dim_provider_keys = df_dim_provider.select("ProviderKey", "ProviderID")
dim_facility_keys = df_dim_facility.select("FacilityKey", "FacilityID")
dim_date_keys     = df_dim_date.select("DateKey", "FullDate")

# Step 2: Join all dimension keys onto claims
df_fact = df_claims \
    .join(dim_patient_keys,
          on="PatientID",
          how="left") \
    .join(dim_provider_keys,
          on="ProviderID",
          how="left") \
    .join(dim_facility_keys,
          on="FacilityID",
          how="left") \
    .join(dim_date_keys,
          df_claims.ServiceDate == dim_date_keys.FullDate,
          how="left")

print(f"Rows after joins: {df_fact.count()}")
print(f"Columns after joins: {len(df_fact.columns)}")

# Quick check — how many rows have all 4 keys populated
complete_keys = df_fact.filter(
    col("PatientKey").isNotNull()  &
    col("ProviderKey").isNotNull() &
    col("FacilityKey").isNotNull() &
    col("DateKey").isNotNull()
).count()

print(f"Rows with all 4 keys: {complete_keys} of {df_fact.count()}")



# Select and calculate final fact table columns
df_fact_final = df_fact.select(

    # Natural key
    col("ClaimID"),

    # Foreign keys — link to 4 dimension tables
    col("PatientKey"),
    col("ProviderKey"),
    col("FacilityKey"),
    col("DateKey"),

    # Source string keys — kept for debugging and lineage
    col("PatientID"),
    col("ProviderID"),
    col("FacilityID"),
    col("ServiceDate"),

    # Financial measures
    col("BilledAmount"),
    col("ApprovedAmount"),
    col("DeniedAmount"),
    col("PatientResponsibility"),

    # Calculated financial measures
    spark_round(
        col("ApprovedAmount") - col("PatientResponsibility"),
        2
    ).alias("NetRevenueAmount"),

    spark_round(
        col("DeniedAmount"),
        2
    ).alias("DenialImpactAmount"),

    # Clinical measures
    col("LengthOfStay"),
    col("ClaimLagDays"),

    # Descriptive attributes kept in fact for convenience
    col("ClaimStatus"),
    col("IsDenied"),
    col("DenialReason"),
    col("CPTCode"),
    col("CPTDescription"),
    col("ProcedureCategory"),
    col("DiagnosisCode"),
    col("DiagnosisDescription"),
    col("ICD10_Valid"),
    col("IsInpatient"),

    # Date columns for convenience
    col("SubmissionDate"),
    col("ProcessedDate"),

    # Audit
    current_timestamp().alias("_gold_timestamp")
)

print(f"fact_claims columns: {len(df_fact_final.columns)}")
print(f"fact_claims rows:    {df_fact_final.count()}")

print("=== Orphaned Fact Check ===")
print("(All counts must be 0 for Gold to be trustworthy)\n")

orphaned_patient  = df_fact_final.filter(col("PatientKey").isNull()).count()
orphaned_provider = df_fact_final.filter(col("ProviderKey").isNull()).count()
orphaned_facility = df_fact_final.filter(col("FacilityKey").isNull()).count()
orphaned_date     = df_fact_final.filter(col("DateKey").isNull()).count()

print(f"Null PatientKey  (orphaned patients):   {orphaned_patient}")
print(f"Null ProviderKey (orphaned providers):  {orphaned_provider}")
print(f"Null FacilityKey (orphaned facilities): {orphaned_facility}")
print(f"Null DateKey     (orphaned dates):      {orphaned_date}")

total_orphaned = orphaned_patient + orphaned_provider + orphaned_facility + orphaned_date

if total_orphaned == 0:
    print("\nAll foreign keys resolved — no orphaned facts ✅")
else:
    print(f"\nWARNING: {total_orphaned} orphaned fact rows detected")
    print("Investigate before writing to Gold")

    # Show sample orphaned rows for debugging
    if orphaned_patient > 0:
        print("\nSample orphaned PatientIDs:")
        df_fact_final.filter(col("PatientKey").isNull()) \
            .select("ClaimID", "PatientID", "ServiceDate") \
            .show(5, truncate=False)


df_fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_claims")

final_count = spark.table("fact_claims").count()
print(f"fact_claims written: {final_count} rows ✅")


# Optimise fact_claims for query performance
# Run after every Gold build
print("Running OPTIMIZE on fact_claims...")

spark.sql("""
    OPTIMIZE fact_claims
    ZORDER BY (DiagnosisCode, ClaimStatus, ServiceDate)
""")

print("OPTIMIZE complete ✅")

# Check table statistics after optimisation
# spark.sql("DESCRIBE DETAIL fact_claims") \
#     .select("numFiles", "sizeInBytes", "numRows") \
#     .show(truncate=False)

# Check table statistics (numRows is not in DESCRIBE DETAIL)
stats_df = spark.sql("DESCRIBE DETAIL fact_claims").select("numFiles", "sizeInBytes")
stats_df.show(truncate=False)

# Print the row count separately
print(f"Total Rows: {spark.table('fact_claims').count()}")


print("=== Gold Layer — Final Verification ===\n")

# 1. Row counts across all Gold tables
print("Table counts:")
for t in ["fact_claims", "dim_patient", "dim_provider",
          "dim_facility", "dim_date"]:
    print(f"  {t:20s}: {spark.table(t).count():>6} rows")

# 2. Revenue summary by payer — headline finance KPI
print("\nRevenue by insurance payer:")
spark.sql("""
    SELECT
        p.InsurancePayer,
        COUNT(*)                            AS TotalClaims,
        ROUND(SUM(f.BilledAmount), 2)       AS TotalBilled,
        ROUND(SUM(f.ApprovedAmount), 2)     AS TotalApproved,
        ROUND(SUM(f.NetRevenueAmount), 2)   AS NetRevenue,
        ROUND(SUM(f.DeniedAmount), 2)       AS TotalDenied,
        ROUND(
            SUM(f.DeniedAmount) /
            NULLIF(SUM(f.BilledAmount), 0) * 100, 1
        )                                   AS DenialRatePct
    FROM      fact_claims  f
    JOIN      dim_patient  p ON f.PatientKey = p.PatientKey
    GROUP BY  p.InsurancePayer
    ORDER BY  TotalBilled DESC
""").show(truncate=False)

# 3. Claims by facility state
print("\nClaims by facility state:")
spark.sql("""
    SELECT
        fac.FacilityState,
        fac.FacilityType,
        COUNT(*)                        AS Claims,
        ROUND(SUM(f.BilledAmount), 2)   AS TotalBilled
    FROM      fact_claims   f
    JOIN      dim_facility  fac ON f.FacilityKey = fac.FacilityKey
    GROUP BY  fac.FacilityState, fac.FacilityType
    ORDER BY  Claims DESC
""").show(truncate=False)

# 4. Monthly claim trend
print("\nMonthly claim volume:")
spark.sql("""
    SELECT
        d.Year,
        d.MonthName,
        COUNT(*)                            AS Claims,
        ROUND(SUM(f.BilledAmount), 2)       AS TotalBilled,
        ROUND(SUM(f.DeniedAmount), 2)       AS TotalDenied
    FROM      fact_claims f
    JOIN      dim_date    d ON f.DateKey = d.DateKey
    GROUP BY  d.Year, d.MonthName, d.Month
    ORDER BY  d.Year, d.Month
""").show(20, truncate=False)

# 5. Top 5 diagnosis codes by claim volume
print("\nTop 5 diagnosis codes:")
spark.sql("""
    SELECT
        DiagnosisCode,
        DiagnosisDescription,
        COUNT(*)                        AS Claims,
        ROUND(SUM(BilledAmount), 2)     AS TotalBilled
    FROM  fact_claims
    GROUP BY DiagnosisCode, DiagnosisDescription
    ORDER BY Claims DESC
    LIMIT 5
""").show(truncate=False)

print("\n=== Gold Layer Complete ✅ ===")
print("Ready for Semantic Model and Power BI")
