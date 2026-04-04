# Case Study 01 — Incremental Data Ingestion Pipeline

**Industry:** Global Logistics & Supply Chain  
**Platform:** Microsoft Fabric — Data Factory + Lakehouse + PySpark  
**Stack:** Azure SQL · Delta Lake · PySpark 
---

## Business Problem

FastFreight Solutions manages 2.4 million shipments annually across 38 countries. Their analytics platform ran a nightly full reload of a 47-million-row Azure SQL database — causing 6-hour pipeline runtimes, compute overruns, and stale dashboards.

| Pain Point | Business Impact |
|---|---|
| Full table reload nightly | 6hr 14min runtime — breached 90-min SLA |
| No change detection | Status updates (Delayed → Delivered) missed or duplicated |
| No audit trail | Impossible to trace which load caused bad dashboard data |
| Compute cost overrun | Fabric capacity maxed out, blocking other workloads |

---

## Solution

An automated incremental ingestion pipeline in Microsoft Fabric using the **watermark pattern** — extracting only records modified since the last successful run.

```
Azure SQL DB (source)
    ↓
Fabric Data Factory Pipeline
    ├── Lookup Activity       →  reads watermark_control table
    ├── Copy Activity         →  WHERE LastModifiedDate > watermark
    └── PySpark Notebook      →  quality checks + MERGE + watermark update
    ↓
Fabric Lakehouse — ShipmentLogs_Silver (Delta Lake)
```

---

## Architecture

![Alt text for the image](https://github.com/ShubhangiBidkar/data-engineering-portfolio/blob/main/01-incremental-ingestion/architecture.svg)


---

## Key Technical Decisions

**Why one Lookup instead of two?**  
The watermark ceiling is calculated inside the notebook from `MAX(LastModifiedDate)` of the actual processed batch — not pre-captured at pipeline start. This ensures the watermark always reflects exactly what landed in the Silver table, making the pipeline fully idempotent.

**Why Delta Lake?**  
ACID transactions, schema enforcement, and native UPSERT (MERGE) support. Raw Parquet cannot handle status updates without creating duplicates.

**Why Parquet for staging?**  
The Copy Activity writes to `Files/staging` as Parquet — preserves SQL data types exactly and is significantly faster for Spark to read than CSV.

**Why MERGE instead of INSERT?**  
Shipment statuses update throughout the day. A plain INSERT creates duplicate rows per shipment. MERGE ensures each ShipmentID has exactly one row reflecting its latest state.

---

## Pipeline Activities

| Step | Activity | Purpose |
|---|---|---|
| 1 | Lookup_OldWatermark | Reads `LastWatermarkDate` from `watermark_control` Delta table |
| 2 | Copy_DeltaRows | Extracts rows `WHERE LastModifiedDate > @watermark` into Parquet staging |
| 3 | Notebook_Upsert | Quality checks → MERGE into Silver → advance watermark |



<img width="1359" height="653" alt="06_pipeline_canvas" src="https://github.com/user-attachments/assets/4bc08e3d-5c73-4a7b-94ef-fe4b7fde26ec" />


---

## Data Quality Rules

Applied in `NB_Incremental_Upsert.py` before every UPSERT:

1. Drop records with null `ShipmentID` — cannot MERGE without primary key
2. Drop records with `WeightKG <= 0` — invalid cargo weight
3. Drop records where `DepartureDate >= EstimatedArrival` — impossible dates
4. Standardise `Status` — trim whitespace, apply title case
5. Fill null `DelayReason` with `N/A` — downstream reporting consistency

---


## Implementation Screenshots

### Watermark Control Table
<img width="1263" height="952" alt="01_watermark_table" src="https://github.com/user-attachments/assets/04ee4b13-91b6-4f7b-b031-1b83139e9587" />


### Lookup Activity — Settings
<img width="1318" height="997" alt="02_lookup_settings" src="https://github.com/user-attachments/assets/535c7ccd-1c97-47b3-a988-34c441b966b3" />


### Copy Activity — Source (Dynamic Watermark Query)
<img width="1287" height="997" alt="03_copy_source" src="https://github.com/user-attachments/assets/f1a04f1b-2582-4e3d-93c8-cf5b500a3b42" />


### Copy Activity — Destination (Parquet Staging)
<img width="1106" height="868" alt="04_copy_destination" src="https://github.com/user-attachments/assets/05de4401-bacf-4d37-931e-322b9b30d1d7" />


### PySpark Notebook — MERGE Code
<img width="871" height="646" alt="05_notebook_merge" src="https://github.com/user-attachments/assets/208b7f49-fc1c-48d2-ba7d-c6474b7ea71b" />


### Notebook output — Day 2 incremental run
<img width="1049" height="965" alt="08_notebook_output png" src="https://github.com/user-attachments/assets/09a3933c-16cc-4400-beed-afbf2dc07a7f" />


### Silver Table — 580 Rows in Lakehouse
<img width="1321" height="1208" alt="07_silver_table" src="https://github.com/user-attachments/assets/8960539b-b75a-4d45-a8ea-70aed27fe18b" />


---

## Results

| Metric | Before | After |
|---|---|---|
| Pipeline runtime | 6 hr 14 min | 22 minutes |
| Rows processed per run | 47,000,000 | ~115 (delta only) |
| Fabric CU consumption | Maxed out | Reduced 94% |
| Duplicate records | Frequent | Zero |
| Monthly compute cost | ~$4,200 | ~$310 |
| Data freshness | Often stale | Daily within SLA |

---

## Files

```
01-incremental-ingestion/
├── README.md                          ← this file
├── architecture.svg                   ← pipeline architecture diagram
├── sql/
│   └── 01_create_tables.sql           ← Azure SQL schema + indexes + watermark queries
├── notebooks/
│   └── NB_Incremental_Upsert.py       ← PySpark notebook (7 cells)
└── data/
    ├── shipment_day1_initial_load.csv  ← 500 shipment records (full history)
    ├── shipment_day2_delta.csv         ← 115 records (80 new + 35 updates)
    ├── watermark_initial.csv           ← watermark control seeded at 1900-01-01
    └── dataset_reference.json          ← column definitions and dataset summary
```


*Part of the [Data Engineering Portfolio](../README.md)*
