# ─────────────────────────────────────────────────────────────────────────────
## This notebook is called by the Fabric Data Factory pipeline
## The pipeline passes old_watermark as a parameter
## ─────────────────────────────────────────────────────────────────────────────
 
try:
    old_watermark = getArgument("old_watermark")
except:
    old_watermark = "1900-01-01 00:00:00"  # default = load all history

print(f"Processing records with LastModifiedDate > {old_watermark}")

from delta.tables import DeltaTable
from pyspark.sql.functions import (col, current_timestamp, lit,
                                    when, initcap, trim, max as spark_max)
from pyspark.sql.types import DoubleType, IntegerType

print("Imports done ✅")

# Read parquet without enforcing schema
# Let Spark read whatever types the Copy Activity wrote
staging_path = "Files/staging/shipment_delta"

df_staged = spark.read.parquet(staging_path)

# Print what types actually came in — useful for debugging
print("Actual schema from Parquet:")
df_staged.printSchema()

# Cast columns that may have type mismatches
df_staged = df_staged \
    .withColumn("WeightKG",         col("WeightKG").cast(DoubleType())) \
    .withColumn("DeclaredValueUSD", col("DeclaredValueUSD").cast(DoubleType())) \
    .withColumn("ContainerCount",   col("ContainerCount").cast(IntegerType()))

staged_count = df_staged.count()
print(f"Records read from staging: {staged_count}")

if staged_count == 0:
    print("No new records to process. Exiting.")
    notebookutils.notebook.exit("No new records")



# ─────────────────────────────────────────────────────────────────────────────
## Apply data quality rules before writing to Silver
## Bad data must never reach the Lakehouse target table
## Each rule is applied independently so you can track exactly what was dropped
## ─────────────────────────────────────────────────────────────────────────────
 
# Rule 1: Drop rows with null primary key — cannot UPSERT without a key
df_clean = df_staged.filter(col("ShipmentID").isNotNull())
print(f"After null PK check:         {df_clean.count()} rows")
 
# Rule 2: Drop rows with invalid weight
df_clean = df_clean.filter(col("WeightKG") > 0)
print(f"After weight check:          {df_clean.count()} rows")
 
# Rule 3: Drop rows where departure is after estimated arrival
df_clean = df_clean.filter(col("DepartureDate") < col("EstimatedArrival"))
print(f"After date logic check:      {df_clean.count()} rows")
 
# Rule 4: Standardise Status — trim whitespace and title case
df_clean = df_clean.withColumn("Status", initcap(trim(col("Status"))))
 
# Rule 5: Fill null DelayReason with N/A
df_clean = df_clean.withColumn(
    "DelayReason",
    when(col("DelayReason").isNull(), "N/A").otherwise(col("DelayReason"))
)
 
# Rule 6: Add audit columns for lineage tracking
df_clean = df_clean.withColumn("_load_timestamp", current_timestamp())
df_clean = df_clean.withColumn("_watermark_start", lit(old_watermark))
 
# Final quality report
total_dropped = staged_count - df_clean.count()
print(f"\nQuality Summary:")
print(f"  Staged:  {staged_count}")
print(f"  Clean:   {df_clean.count()}")
print(f"  Dropped: {total_dropped}")

## ─────────────────────────────────────────────────────────────────────────────
## MERGE logic:
##   If ShipmentID already exists in target → UPDATE all columns
##   If ShipmentID is new                  → INSERT
##
## This handles both new shipments AND status updates on existing shipments
## A plain INSERT would create duplicates for updated records
## ─────────────────────────────────────────────────────────────────────────────
 
target_table_name = "ShipmentLogs_Silver"
 
if not spark.catalog.tableExists(target_table_name):
    # First pipeline run — table does not exist yet, create it
    print(f"First run — creating {target_table_name}")
    df_clean.write.format("delta").saveAsTable(target_table_name)
    print(f"Table created with {df_clean.count()} records ✅")
 
else:
    # Subsequent runs — MERGE into existing table
    target = DeltaTable.forName(spark, target_table_name)
 
    target.alias("target").merge(
        df_clean.alias("source"),
        "target.ShipmentID = source.ShipmentID"   # match on primary key
    ).whenMatchedUpdateAll(                         # existing row → update
    ).whenNotMatchedInsertAll(                      # new row → insert
    ).execute()
 
    print(f"UPSERT complete into {target_table_name} ✅")
 
# Confirm final row count
final_count = spark.table(target_table_name).count()
print(f"Total records in {target_table_name}: {final_count}")

## ─────────────────────────────────────────────────────────────────────────────
## Calculate new watermark from MAX of what we actually processed
## This is more accurate than using a pre-captured ceiling value
## because it reflects exactly what landed in the Silver table
## ─────────────────────────────────────────────────────────────────────────────
 
# Get MAX LastModifiedDate from the clean batch we just processed
new_watermark = df_clean.agg(spark_max("LastModifiedDate")).collect()[0][0]
 
print(f"Old watermark: {old_watermark}")
print(f"New watermark: {new_watermark}")
 
# Update watermark control table
spark.sql(f"""
    UPDATE watermark_control
    SET    LastWatermarkDate = CAST('{new_watermark}' AS TIMESTAMP)
    WHERE  TableName = 'ShipmentLogs'
""")
 
print(f"Watermark advanced to: {new_watermark} ✅")
 
 # Post Load Validation
## ─────────────────────────────────────────────────────────────────────────────
## Run after every pipeline execution to confirm the load is clean
## These outputs appear in the pipeline run logs for monitoring
## ─────────────────────────────────────────────────────────────────────────────
 
print("=== Status Distribution ===")
spark.table(target_table_name) \
     .groupBy("Status") \
     .count() \
     .orderBy("count", ascending=False) \
     .show(truncate=False)
 
print("=== Watermark Control ===")
spark.sql("SELECT * FROM watermark_control").show(truncate=False)
 
print("=== Pipeline Run Complete ✅ ===")
print(f"  Records processed : {df_clean.count()}")
print(f"  Records dropped   : {total_dropped}")
print(f"  Silver table total: {final_count}")
print(f"  Watermark moved   : {old_watermark}  →  {new_watermark}")
