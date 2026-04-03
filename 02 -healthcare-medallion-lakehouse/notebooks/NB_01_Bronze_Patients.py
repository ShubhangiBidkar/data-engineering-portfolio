# Read current watermark from Bronze Lakehouse control table
# This tells us where the last successful run stopped

old_watermark = spark.sql(""" 
    SELECT LastWatermarkDate
    FROM watermark_control
    WHERE TableName = 'Patients'
""").collect()[0][0]

print(f"Old watermark: {old_watermark}")
print(f"Extracting patients WHERE LastModifiedDate > {old_watermark}")


# Read incremental patient records from Azure SQL
# Replace YOUR_SERVER and YOUR_DATABASE with your actual values

jdbc_url = "Sql server url"

df_patients = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.Patients") \
    .option("user", "******") \
    .option("password", "******") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

# Apply watermark filter — only new or updated records
df_delta = df_patients.filter(
    df_patients.LastModifiedDate > old_watermark
)

delta_count = df_delta.count()
print(f"Total records in source: {df_patients.count()}")
print(f"Delta records to process: {delta_count}")

if delta_count == 0:
    print("No new records since last run — exiting")
    notebookutils.notebook.exit("No new records")



from pyspark.sql.functions import current_timestamp, lit

# Add metadata columns before writing to Bronze
# Bronze never transforms data — only adds audit columns
df_bronze = df_delta \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system",       lit("Azure_SQL_dbo_Patients")) \
    .withColumn("_load_pattern",        lit("Incremental"))

print(f"Columns in Bronze: {len(df_bronze.columns)}")
print("Schema:")
df_bronze.printSchema()



from delta.tables import DeltaTable

target_table = "bronze_patients"

if not spark.catalog.tableExists(target_table):
    # First run — create table
    print(f"First run — creating {target_table}")
    df_bronze.write \
        .format("delta") \
        .saveAsTable(target_table)
    print(f"Created {target_table} with {df_bronze.count()} records ✅")

else:
    # Subsequent runs — MERGE on PatientID
    # Bronze uses MERGE not append to avoid duplicates if pipeline reruns
    target = DeltaTable.forName(spark, target_table)

    target.alias("target").merge(
        df_bronze.alias("source"),
        "target.PatientID = source.PatientID"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

    print(f"MERGE complete into {target_table} ✅")

# Final count
final_count = spark.table(target_table).count()
print(f"Total records in {target_table}: {final_count}")



from pyspark.sql.functions import max as spark_max

# Calculate new watermark from MAX of what we just processed
new_watermark = df_bronze.agg(
    spark_max("LastModifiedDate")
).collect()[0][0]

print(f"Old watermark: {old_watermark}")
print(f"New watermark: {new_watermark}")

# Update watermark control table
spark.sql(f"""
    UPDATE watermark_control
    SET    LastWatermarkDate = CAST('{new_watermark}' AS TIMESTAMP)
    WHERE  TableName = 'Patients'
""")

print(f"Watermark advanced to {new_watermark} ✅")


print("=== Bronze Patients Summary ===")

# Row count
total = spark.table("bronze_patients").count()
print(f"Total rows in bronze_patients: {total}")

# Active vs inactive
spark.sql("""
    SELECT IsActive, COUNT(*) AS PatientCount
    FROM   bronze_patients
    GROUP BY IsActive
""").show()

# Watermark confirmation
spark.sql("""
    SELECT TableName, LastWatermarkDate
    FROM   watermark_control
    WHERE  TableName = 'Patients'
""").show(truncate=False)

# Sample records
print("Sample records:")
spark.table("bronze_patients") \
    .select("PatientID", "FirstName", "LastName",
            "State", "InsurancePayer", "LastModifiedDate") \
    .show(5, truncate=False)

