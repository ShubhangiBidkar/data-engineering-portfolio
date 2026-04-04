from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (StructType, StructField, StringType,
                                DoubleType, IntegerType, DateType)

# Define schema explicitly — never infer on production pipelines
# Parquet infers well but CSV infers poorly — dates become strings,
# decimals become doubles or strings depending on nulls

schema = StructType([
    StructField("ClaimID",              StringType(),  False),
    StructField("PatientID",            StringType(),  True),
    StructField("MRN",                  StringType(),  True),
    StructField("PatientName",          StringType(),  True),
    StructField("PatientDOB",           StringType(),  True),
    StructField("InsurancePayer",       StringType(),  True),
    StructField("InsurancePayerCode",   StringType(),  True),
    StructField("InsuranceMemberID",    StringType(),  True),
    StructField("ProviderID",           StringType(),  True),
    StructField("ProviderName",         StringType(),  True),
    StructField("ProviderSpecialty",    StringType(),  True),
    StructField("FacilityID",           StringType(),  True),
    StructField("FacilityName",         StringType(),  True),
    StructField("FacilityType",         StringType(),  True),
    StructField("ServiceDate",          StringType(),  True),
    StructField("SubmissionDate",       StringType(),  True),
    StructField("ProcessedDate",        StringType(),  True),
    StructField("CPTCode",              StringType(),  True),
    StructField("CPTDescription",       StringType(),  True),
    StructField("ProcedureCategory",    StringType(),  True),
    StructField("DiagnosisCode",        StringType(),  True),
    StructField("DiagnosisDescription", StringType(),  True),
    StructField("BilledAmount",         StringType(),  True),
    StructField("ApprovedAmount",       StringType(),  True),
    StructField("DeniedAmount",         StringType(),  True),
    StructField("PatientResponsibility",StringType(),  True),
    StructField("ClaimStatus",          StringType(),  True),
    StructField("DenialReason",         StringType(),  True),
    StructField("LengthOfStay",         StringType(),  True),
    StructField("IsInpatient",          StringType(),  True),
    StructField("LastModifiedDate",     StringType(),  True),
])

# Read from shortcut path
# Bronze reads everything as strings — type casting happens in Silver
df_claims = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("multiLine", "false") \
    .schema(schema) \
    .load("Files/raw_ingestion/healthcare/claims.csv")

raw_count = df_claims.count()
print(f"Records read from claims.csv: {raw_count}")



# Read from shortcut path
# Bronze reads everything as strings — type casting happens in Silver

df_bronze_claims =( 
    df_claims
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("ADLS_Gen2_claims_csv"))
    .withColumn("_load_pattern",lit("Full.overwrite"))
)

print(f"Schema after audit columns: {len(df_bronze_claims.columns)} columns")


# Full overwrite — replace entire table on every run
# This guarantees Bronze always mirrors the source CSV exactly
# If a claim was corrected in the billing system and a new CSV exported,
# the correction lands in Bronze cleanly with no stale records

df_bronze_claims.write\
    .format("delta")\
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_claims")

final_count = spark.table("bronze_claims").count()
print(f"bronze_claims loaded : {final_count} rows ✅")    


print("=== Bronze Claims Summary ===")

# Row count
print(f"Total rows: {spark.table('bronze_claims').count()}")

# Claim status distribution
spark.sql("""
    SELECT   ClaimStatus,
             COUNT(*) AS ClaimCount
    FROM     bronze_claims
    GROUP BY ClaimStatus
    ORDER BY ClaimCount DESC
""").show()

# Top 5 payers
spark.sql("""
    SELECT   InsurancePayer,
             COUNT(*) AS Claims
    FROM     bronze_claims
    GROUP BY InsurancePayer
    ORDER BY Claims DESC
    LIMIT 5
""").show()

# Sample records
spark.table("bronze_claims") \
    .select("ClaimID", "PatientID", "CPTCode",
            "BilledAmount", "ClaimStatus", "ServiceDate") \
    .show(5, truncate=False)



