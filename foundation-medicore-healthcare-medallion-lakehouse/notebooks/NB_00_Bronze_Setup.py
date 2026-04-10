# Create watermark control table in Bronze Lakehouse
# This table tracks incremental load progress for Patients
# Claims and Appointments use full overwrite so their rows are for reference only


spark.sql("""
    CREATE TABLE IF NOT EXISTS watermark_control(
        TableName STRING NOT NULL,
        LastWatermarkDate TIMESTAMP NOT NULL,
        SourceSystem STRING,
        LoadPattern STRING,
        Notes STRING
    )
    USING DELTA
""")

# Check if already seeded to avoid duplicate inserts on re-run
count = spark.sql("SELECT COUNT(*) FROM watermark_control").collect()[0][0]

if count == 0:
    spark.sql("""
        INSERT INTO watermark_control VALUES
        ('Patients',
         CAST('1900-01-01 00:00:00' AS TIMESTAMP),
         'Azure SQL',
         'Incremental',
         'Watermark on LastModifiedDate — extracts only new or updated patients'),

        ('Claims',
         CAST('1900-01-01 00:00:00' AS TIMESTAMP),
         'ADLS Gen2',
         'Full overwrite',
         'Daily CSV export — full Bronze overwrite on every run'),

        ('Appointments',
         CAST('1900-01-01 00:00:00' AS TIMESTAMP),
         'ADLS Gen2',
         'Full overwrite',
         'Daily CSV export — full Bronze overwrite on every run')
    """)
    print("Watermark table seeded with 3 rows ✅")
else:
    print(f"Watermark table already has {count} rows — skipping seed")


print("=== watermark_control ===")
spark.sql("SELECT * FROM watermark_control").show(truncate=False)

# Verify the ADLS Gen2 shortcut is accessible
# This reads the shortcut path and checks both CSV files are visible

import os
from notebookutils import mssparkutils

files = mssparkutils.fs.ls("Files/raw_ingestion/healthcare")
print("Files visible via shortcut:")
for f in files:
    print(f"  {f.name}  —  {f.size:,} bytes")
