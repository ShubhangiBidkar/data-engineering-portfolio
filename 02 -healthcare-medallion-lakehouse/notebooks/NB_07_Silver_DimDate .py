# Get min/max from BOTH claims and appointments
from pyspark.sql.functions import min as spark_min, max as spark_max

min_claims = spark.table("silver_claims").agg(spark_min("ServiceDate")).collect()[0][0]
max_claims = spark.table("silver_claims").agg(spark_max("ServiceDate")).collect()[0][0]

min_appts = spark.table("silver_appointments").agg(
    spark_min("ScheduledDateTime").cast("date")).collect()[0][0]
max_appts = spark.table("silver_appointments").agg(
    spark_max("ScheduledDateTime").cast("date")).collect()[0][0]

# Use the wider range
from datetime import date
min_date = min(min_claims, min_appts)
max_date = max(max_claims, max_appts)

print(f"Extended date range: {min_date} to {max_date}")

from pyspark.sql.functions import (
    explode, sequence, to_date, col,
    year, month, quarter, dayofweek,
    weekofyear, date_format, lit,
    when, current_timestamp
)

# Generate one row per calendar day using Spark SQL sequence
date_spine = spark.sql(f""" 
    SELECT explode(
        sequence(
            DATE '{min_date}',
            DATE '{max_date}',
            INTERVAL 1 DAY
        )
    )AS FullDate
""")

print(f"Date spine generated: {date_spine.count()} days")
print(f"First date: {date_spine.first()[0]}")

df_dim_date = date_spine \
    .withColumn("DateKey",
        date_format(col("FullDate"), "yyyyMMdd").cast("integer")) \
    .withColumn("Year",
        year(col("FullDate"))) \
    .withColumn("Month",
        month(col("FullDate"))) \
    .withColumn("MonthName",
        date_format(col("FullDate"), "MMMM")) \
    .withColumn("MonthShort",
        date_format(col("FullDate"), "MMM")) \
    .withColumn("Quarter",
        quarter(col("FullDate"))) \
    .withColumn("QuarterName",
        date_format(col("FullDate"), "'Q'Q yyyy")) \
    .withColumn("DayOfWeek",
        dayofweek(col("FullDate"))) \
    .withColumn("DayName",
        date_format(col("FullDate"), "EEEE")) \
    .withColumn("DayShort",
        date_format(col("FullDate"), "EEE")) \
    .withColumn("WeekOfYear",
        weekofyear(col("FullDate"))) \
    .withColumn("IsWeekend",
        dayofweek(col("FullDate")).isin(1, 7)) \
    .withColumn("IsMonthEnd",
        col("FullDate") == date_format(
            col("FullDate"), "yyyy-MM-dd"
        ).substr(1, 7).cast("string")) \
    .withColumn("IsMonthEnd",
        date_format(col("FullDate"), "d").cast("integer") ==
        date_format(
            date_format(col("FullDate"), "yyyy-MM-01").cast("date"),
            "d"
        ).cast("integer")) \
    .withColumn("IsQuarterEnd",
        (col("Month").isin(3, 6, 9, 12)) &
        (date_format(col("FullDate"), "d").cast("integer") ==
         date_format(
             date_format(col("FullDate"), "yyyy-MM-01").cast("date"),
             "d"
         ).cast("integer"))) \
    .withColumn("FiscalYear",
        when(col("Month") >= 10,
             col("Year") + 1
        ).otherwise(col("Year"))) \
    .withColumn("FiscalQuarter",
        when(col("Month").isin(10, 11, 12), lit(1))
       .when(col("Month").isin(1, 2, 3),   lit(2))
       .when(col("Month").isin(4, 5, 6),   lit(3))
       .otherwise(lit(4))) \
    .withColumn("_silver_timestamp", current_timestamp())

print(f"dim_date columns: {len(df_dim_date.columns)}")
print("\nSample rows:")
df_dim_date.select(
    "DateKey", "FullDate", "Year", "Month", "MonthName",
    "Quarter", "DayName", "IsWeekend", "FiscalYear", "FiscalQuarter"
).show(5, truncate=False)

from pyspark.sql.functions import last_day

# Fix IsMonthEnd using last_day() — handles all month lengths correctly
df_dim_date = df_dim_date \
    .withColumn("IsMonthEnd",
        col("FullDate") == last_day(col("FullDate"))) \
    .withColumn("IsQuarterEnd",
        (col("FullDate") == last_day(col("FullDate"))) &
        col("Month").isin(3, 6, 9, 12))

print("IsMonthEnd and IsQuarterEnd fixed ✅")

# Verify month-end detection
print("\nSample month-end dates:")
df_dim_date.filter(col("IsMonthEnd") == True) \
    .select("FullDate", "MonthName", "Year", "IsMonthEnd", "IsQuarterEnd") \
    .show(6, truncate=False)


df_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")

final_count = spark.table("dim_date").count()
print(f"dim_date written: {final_count} rows ✅")


print("=== dim_date Summary ===")

# Row count and date range
spark.sql("""
    SELECT
        COUNT(*)            AS TotalDays,
        MIN(FullDate)       AS StartDate,
        MAX(FullDate)       AS EndDate,
        COUNT(DISTINCT Year) AS YearsSpanned
    FROM dim_date
""").show()

# Distribution by year
spark.sql("""
    SELECT Year, COUNT(*) AS DaysInYear
    FROM   dim_date
    GROUP BY Year
    ORDER BY Year
""").show()

# Weekend vs weekday split
spark.sql("""
    SELECT
        IsWeekend,
        COUNT(*) AS Days
    FROM dim_date
    GROUP BY IsWeekend
""").show()

# Fiscal year distribution
spark.sql("""
    SELECT FiscalYear, FiscalQuarter, COUNT(*) AS Days
    FROM   dim_date
    GROUP BY FiscalYear, FiscalQuarter
    ORDER BY FiscalYear, FiscalQuarter
""").show()

# Critical check: every claim date has a matching DateKey
unmatched = spark.sql("""
    SELECT COUNT(*) AS UnmatchedClaims
    FROM   silver_claims sc
    LEFT JOIN dim_date   dd
           ON sc.ServiceDate = dd.FullDate
    WHERE  dd.DateKey IS NULL
""").collect()[0][0]

print(f"Unmatched claim dates: {unmatched}  (must be 0)")

if unmatched == 0:
    print("All claim dates covered by dim_date ✅")
else:
    print(f"WARNING: {unmatched} claims have no matching date — extend date range")
