# Blackwood Estates — The Global Snapshot Initiative

**Industry:** Luxury Real Estate · Global Multi-Region Agency  
**Platform:** Microsoft Fabric — Data Factory + Lakehouse + PySpark  
**Stack:** ADLS Gen2 · Delta Lake · PySpark · Switch Activity · DP-700 · DP-600  

---

## About the Company

Founded in 1887, Blackwood Estates has established itself as the premier luxury real estate agency operating across 14 countries and 6 continents. With a portfolio spanning private Caribbean islands, historic European chateaus, and ultra-modern city penthouses, the firm brokers transactions for the world's most discerning buyers and sellers.

Headquartered in Mayfair, London, Blackwood operates through three regional hubs — North America (New York), Europe (London), and Asia-Pacific (Singapore) — each managing its own listing inventory independently. The firm processes 8,400 active property listings annually with an average transaction value of $2.1M USD.

---

## Business Problem

Despite its global reach, Blackwood Estates' data infrastructure had not evolved beyond spreadsheets. Each regional office exported property listing data monthly in whatever format their local team preferred — the North America office in CSV, Europe in JSON, Asia-Pacific in Excel. Column names, date formats, area units, and status values differed entirely between regions.

A single data analyst spent **3 full working days every month** manually downloading files, renaming columns, converting date formats, resolving area unit differences, and consolidating everything into a master spreadsheet. By the time the consolidated report reached leadership it was already 72 hours stale — and still contained errors introduced during manual processing.

| Pain Point | Business Impact |
|---|---|
| 3 different file formats (CSV, JSON, Excel) | No single pipeline could ingest all three automatically |
| Column names differ per region | `ListingPrice` vs `asking_price` vs `Price_USD` — same field, 3 names |
| Date formats differ per region | MM/DD/YYYY vs DD-MM-YYYY vs YYYY/MM/DD |
| Area units differ | sqft (USA/APAC) vs sqm (Europe) — silent comparison errors |
| Country format differs | Full name vs ISO2 code vs Full name |
| No arrival schedule | Files land anytime — no trigger, no notification |
| No error handling | Bad files silently corrupted the master spreadsheet |
| 3-day manual cycle | Leadership made capital allocation decisions on stale data |

---

## Executive Recommendation: Objective

Design and implement an automated, auditable wildcard file ingestion pipeline within Microsoft Fabric that delivers a standardised global property listing dataset by 08:00 UTC daily — regardless of which regional office sent files, in what format, or with what schema — enabling Blackwood's leadership to make real-time capital allocation and marketing decisions across all regions.

---

## Challenges Identified

🔶 **Multi-format ingestion** — CSV, JSON, and Excel each require fundamentally different reading strategies in PySpark. No single Spark reader handles all three.

🔶 **Schema drift across regions** — 13 column name conflicts, 3 date format variants, 2 area unit systems, and 3 status value sets must all be resolved to a single unified schema.

🔶 **No naming convention** — Files arrive as `listings_jan_2024.csv`, `property_data_Q1.json`, `uk_listings.xlsx` — any pipeline relying on fixed filenames breaks immediately.

🔶 **Unsupported file handling** — Without a quarantine mechanism, an XML or PDF file in the landing folder crashes the entire pipeline and blocks all other regions.

🔶 **No audit trail** — Impossible to prove which regional file introduced a data error or when the data was loaded.

🔶 **Manual process cost** — Estimated $67K annual cost in analyst time for a task that should require zero human intervention.

---

## Solution Approach

✅ Built a **metadata-driven wildcard pipeline** using Get Metadata + ForEach + Switch routing. The pipeline discovers files dynamically — no hardcoded filenames anywhere. Any file dropped in the landing folder is processed automatically.

✅ Implemented a **Switch Activity** classifying files by extension — `.csv` routes to NB_01, `.json` to NB_02, `.xlsx` to NB_03, and any unsupported format to the quarantine path. This was chosen over nested If Condition activities because Switch evaluates the extension once and routes directly — O(1) routing vs O(n) sequential evaluation.

✅ Resolved all 13 **schema conflicts** automatically in format-specific notebooks — column renaming, date parsing with explicit format strings, sqm → sqft conversion (× 10.764), ISO2 country code resolution, and status value standardisation.

✅ Applied the **quarantine pattern** — unsupported files are isolated in `Files/quarantine/`, logged to `quarantine_log` with reason and timestamp, and the pipeline continues processing other regional files. One bad file never blocks the others.

✅ Implemented **idempotent Bronze notebooks** — each ingestor deletes existing rows for its SourceRegion before appending. Safe to rerun any number of times without data duplication.

✅ Applied **OPTIMIZE + ZORDER** on the Silver table (Country, Status, PropertyType) for query performance aligned with Power BI filter patterns.

---

## Architecture

![Architecture Diagram](./architecture/cs1_architecture.svg)

```
Regional Files (ADLS Gen2 · Files/incoming/)
         ↓
PL_Blackwood_Wildcard
├── Get Metadata      → discovers all files in incoming/
├── ForEach           → loops over every file found
│   └── Switch (@last(split(item().name,'.')))
│       ├── csv   → NB_01_Ingest_CSV  + Move_To_Processed
│       ├── json  → NB_02_Ingest_JSON + Move_To_Processed
│       ├── xlsx  → NB_03_Ingest_Excel + Move_To_Processed
│       └── default → Log_Quarantine + Move_To_Quarantine
├── NB_04_Silver_Listings → unified schema + quality rules + OPTIMIZE
└── Log_Success           → pipeline_log entry
         ↓
Blackwood_Lakehouse (bronze / silver schemas)
         ↓
Data Activator → OneLake events → alert on quarantine file
```

---

## Schema Conflicts Resolved

The core engineering challenge — unifying 3 completely incompatible schemas:

| Concept | USA CSV | Europe JSON | APAC Excel | Unified Silver |
|---|---|---|---|---|
| Listing ID | `ListingID` | `listing_id` | `PropertyRef` | `ListingID` |
| Price | `ListingPrice` | `asking_price` | `Price_USD` | `ListingPrice` (USD) |
| Bedrooms | `Bedrooms` | `num_bedrooms` | `BedCount` | `Bedrooms` |
| Area | `SqFt` (sqft) | `area_sqm` (sqm) | `FloorArea_sqft` (sqft) | `AreaSqFt` (always sqft) |
| Date | MM/DD/YYYY | DD-MM-YYYY | YYYY/MM/DD | yyyy-MM-dd |
| Country | Full name | ISO2 code | Full name | Full name always |
| Status | Active/Sold/Under Contract | active/sold | Available/Sold | Active/Sold/Other |

---

## Pipeline Activities

| Step | Activity | Type | Purpose |
|---|---|---|---|
| 1 | `GetFiles_Incoming` | Get Metadata | Discovers all files in `Files/incoming/` |
| 2 | `ForEach_Files` | ForEach | Loops over every file — batch=3, sequential off |
| 3 | `Switch1` | Switch | Routes by extension: `@last(split(item().name,'.'))` |
| 4 | `Run_NB01_CSV` | Notebook | CSV ingestion — column rename + date parse |
| 5 | `Run_NB02_JSON` | Notebook | JSON ingestion — sqm→sqft + ISO2 resolution |
| 6 | `Run_NB03_Excel` | Notebook | Excel via openpyxl→Pandas→Spark |
| 7 | `Log_Quarantine` | Notebook | Logs unsupported file to `quarantine_log` |
| 8 | `Move_To_Processed` | Copy data | Moves file from `incoming/` to `processed/` |
| 9 | `Move_To_Quarantine` | Copy data | Moves bad file to `quarantine/` |
| 10 | `Run_NB04_Silver` | Notebook | 10 quality rules + dedup + OPTIMIZE |
| 11 | `Log_Success` | Notebook | Run summary to `pipeline_log` |
| 12 | `Alert_Failure` | Notebook | Failure logging |

---

## Data Quality Rules — Silver Layer

| Rule | What it checks | Action on failure |
|---|---|---|
| 1 | Null `ListingID` | Drop — cannot identify without PK |
| 2 | Null or zero `ListingPrice` | Drop — invalid financial record |
| 3 | Null `City` | Drop — geographic analysis impossible |
| 4 | Null `ListDate` | Drop — time analysis impossible |
| 5 | Negative `Bedrooms` | Drop — physically impossible |
| 6 | Negative `Bathrooms` | Drop — physically impossible |
| 7 | `AreaSqFt` outside 50–50,000 | Drop — outside realistic range |
| 8 | `PropertyType` standardisation | Flat/Studio/Condo→Apartment · Single Family→House |
| 9 | `Country` standardisation | `initcap(trim())` — consistent casing |
| 10 | Deduplication on `ListingID` | Keep latest by `_ingestion_ts` |

---

## Dataset

| File | Format | Rows | Region | Key challenge |
|---|---|---|---|---|
| `listings_usa.csv` | CSV | 800 | USA + Canada | Standard schema — baseline |
| `listings_europe.json` | JSON | 600 | UK + EU (6 countries) | snake_case + sqm + ISO2 codes |
| `listings_apac.xlsx` | Excel | 400 | AU + SG + HK | Extra columns + YYYY/MM/DD dates |
| `corrupted_data.txt` | TXT | — | — | Quarantine test file |

**Realistic data includes:**
- Real property types: Villa, Penthouse, Apartment, House, Townhouse
- Real city names across 14 countries
- Realistic price ranges ($150K–$6M)
- 3 different date format standards
- 2 area unit standards (sqft and sqm)
- APAC-specific columns: ViewType, FurnishedStatus (null for other regions)

---

## Key Technical Decisions

**Why Switch Activity instead of If Condition?**  
Switch evaluates the file extension expression once and routes directly to the matching case. Nested If Conditions evaluate sequentially — every file goes through the CSV check before the JSON check. Switch is O(1) routing. It also makes all routing logic visible simultaneously in the pipeline canvas — a cleaner design for a portfolio audience.

**Why openpyxl → Pandas → Spark for Excel?**  
Spark has no native Excel reader. Excel is a binary format. The production pattern is openpyxl reads the workbook bytes, Pandas creates a DataFrame, `astype(str)` normalises all types, then `spark.createDataFrame()` converts to Spark. The `astype(str)` step is critical — it prevents openpyxl's Python type inference from conflicting with Spark's type system.

**Why convert sqm to sqft in Bronze?**  
Unit conversion is deterministic and lossless — 1 sqm = 10.764 sqft always. Converting in Bronze means Silver and Gold never need to know which unit a row originally used. A query comparing area across regions always compares like-for-like without any additional logic.

**Why resolve ISO2 country codes in the Bronze notebook?**  
Resolving in Bronze means Silver and Gold always receive consistent country values. If resolution happened in Silver, a join failure or code change would produce silently wrong country values that propagate into Gold and Power BI. Fixing it at source prevents an entire category of downstream data quality bugs.

---

## Business Outcomes

🟢 **RELIABILITY** — Elimination of manual copy-paste errors and missing file incidents via automated wildcard ingestion. Zero human touchpoints in the daily data load.

🟢 **SPEED** — Reduction of reporting latency from 72 hours (3-day manual cycle) to fully automated daily refresh. Leadership sees a unified global portfolio view by 08:00 UTC.

🟢 **COMPLIANCE** — Full audit trail via `pipeline_log` and `quarantine_log` Delta tables. Every file load timestamped and traceable. Quarantined files preserved for investigation.

🟢 **SCALABILITY** — Adding a 15th regional office requires uploading files to the landing folder. Zero pipeline changes. Zero notebook changes. The Switch Default case handles any new format automatically until a new case is added.

---

## Results

| Metric | Before | After |
|---|---|---|
| Data preparation time | 3 working days manual | Fully automated — zero analyst time |
| Data freshness | 72-hour lag | Daily automated refresh |
| Schema errors | Frequent — manual mapping | Zero — resolved in notebook code |
| Bad file handling | Silent corruption | Quarantine + alert + audit log |
| Formats supported | Excel only (manual) | CSV + JSON + Excel + extensible |
| Audit trail | None | `pipeline_log` + `quarantine_log` Delta tables |
| Pipeline runtime | N/A | Under 4 minutes for 1,800 rows |
| Rerun safety | Full data loss risk | Idempotent — safe to rerun any number of times |

---

## Implementation Screenshots

### Bronze Lakehouse — Folder Structure
![Bronze Folders](./screenshots/01_bronze_lakehouse_folders.png)

### Incoming Files — All 4 Files Uploaded
![Incoming Files](./screenshots/02_incoming_files_uploaded.png)

### NB_01 — USA CSV Ingestion Output
![NB_01 Output](./screenshots/03_nb01_csv_output.png)

### NB_02 — Europe JSON Output (sqm→sqft + ISO2 resolved)
![NB_02 Output](./screenshots/04_nb02_json_output.png)

### NB_03 — APAC Excel Output
![NB_03 Output](./screenshots/05_nb03_excel_output.png)

### NB_04 — Silver Unified Output (quality rules + property types)
![NB_04 Output](./screenshots/06_nb04_silver_output.png)

### Bronze Listings — All 3 Regions
![Bronze Table](./screenshots/07_bronze_listings_all_regions.png)

### Silver Listings — Unified Schema
![Silver Table](./screenshots/08_silver_listings_unified.png)

### Pipeline Canvas — Main View
![Pipeline Canvas](./screenshots/09_pipeline_canvas_main.png)

### Switch Activity — All 4 Cases
![Switch Cases](./screenshots/10_pipeline_switch_cases.png)

### Pipeline Run — Success
![Pipeline Run](./screenshots/11_pipeline_run_success.png)

### Pipeline Log Table
![Pipeline Log](./screenshots/12_pipeline_log_table.png)

### Quarantine Log — corrupted_data.txt
![Quarantine Log](./screenshots/13_quarantine_log_table.png)

### Processed + Quarantine Folders After Run
![Folders After](./screenshots/14_processed_quarantine_folders.png)

---

## Files

```
01-blackwood-estates/
├── README.md
├── architecture/
│   └── cs1_architecture.svg
├── data/
│   ├── listings_usa.csv
│   ├── listings_europe.json
│   ├── listings_apac.xlsx
│   ├── corrupted_data.txt
│   └── dataset_reference.json
├── notebooks/
│   ├── NB_00_Setup.py
│   ├── NB_01_Ingest_CSV.py
│   ├── NB_02_Ingest_JSON.py
│   ├── NB_03_Ingest_Excel.py
│   ├── NB_04_Silver_Listings.py
│   ├── NB_Log_Quarantine.py
│   └── NB_Log_Pipeline.py
└── screenshots/
```

## Concepts Covered

| Concept | Where demonstrated |
|---|---|
| Wildcard file loading | Get Metadata + ForEach — no hardcoded filenames |
| Switch Activity routing | Extension-based classification — O(1) routing |
| Multi-format ingestion | CSV (Spark native) · JSON (Spark native) · Excel (openpyxl→Pandas→Spark) |
| Schema drift resolution | 13 column conflicts resolved in format-specific notebooks |
| Unit conversion | sqm → sqft in NB_02 (× 10.764) |
| ISO2 country resolution | WHEN chain in NB_02 — 10 EU country codes |
| Quarantine pattern | Unsupported files isolated — pipeline continues |
| Idempotent Bronze | Delete SourceRegion rows before append — safe to rerun |
| Full overwrite Silver | Always rebuilt from Bronze — consistent quality rules |
| Structured audit logging | `pipeline_log` + `quarantine_log` Delta tables |
| OPTIMIZE + ZORDER | `silver_listings` ZORDERed by Country + Status + PropertyType |
| Data Activator design | OneLake events → Eventstream → Activator (documented) |

---

## Interview Q&A

**Q: Why did you use a Switch activity instead of If Condition activities?**  
Switch evaluates the file extension expression once and routes directly to the matching case — O(1) routing. Nested If Condition activities evaluate sequentially — every file passes through the CSV check before the JSON check. Switch is not only faster but makes all routing logic visible simultaneously in the pipeline canvas without drilling into nested True/False paths. For a portfolio, Switch also communicates the intent more clearly to a technical reviewer.

**Q: What happens when an unsupported file lands in incoming/?**  
The Switch Default case handles it. The file is copied to `Files/quarantine/`, a row is written to `quarantine_log` with filename, extension, reason, and timestamp, and the pipeline continues processing the remaining files. One bad file never blocks other regions. The quarantine_log is designed to be monitored by Data Activator — when resolved=false rows appear an alert fires to the operations team.

**Q: How does the pipeline handle reruns safely?**  
Each Bronze ingestor deletes existing rows for its SourceRegion before appending — `target.delete("SourceRegion = 'USA'")`. This is the idempotency pattern. Whether a rerun happens due to pipeline failure, manual trigger, or data correction, Bronze always ends up with exactly one set of rows per region. Silver is full overwrite so it is also idempotent by design.

**Q: Why convert area units in Bronze rather than Silver?**  
Unit conversion is deterministic — 1 sqm = 10.764 sqft always. Converting in Bronze means Silver and Gold never need conditional logic based on source region. If conversion happened in Silver a join failure or missing SourceRegion tag would produce silently wrong area values that propagate downstream. Fixing it at source eliminates an entire category of data quality bugs.

**Q: How would you scale this to 50 regional offices?**  
The pipeline already scales horizontally — ForEach with Sequential=Off processes files in parallel up to batch count. Adding a new regional office requires only uploading files to the landing folder. Zero pipeline changes are needed for existing formats. For a new format (e.g. XML) one new Switch case and one new notebook are added — no existing logic is touched. At 50+ offices with metadata-driven schema mapping I would move column rename configurations to a Delta config table rather than hardcoding them in each notebook.

---

*Part of the [Data Engineering Portfolio](../README.md) — 22 case studies across Microsoft Fabric and Azure Databricks*
