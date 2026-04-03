# Case Study 02 — Healthcare Medallion Lakehouse

**Industry:** Healthcare — Multi-Site Hospital Network  
**Platform:** Microsoft Fabric — Data Factory + Lakehouse + PySpark + Power BI  
**Stack:** Azure SQL · ADLS Gen2 · Delta Lake · PySpark · DAX 

---

## Business Problem

MediCore Health Network operates 12 hospitals and 48 outpatient clinics across 6 US states, processing 2.1 million patient interactions annually. Three disconnected operational systems — a patient registration database, a billing claims exporter, and an EHR appointment system — had never been unified.

| Pain Point | Business Impact |
|---|---|
| No unified patient view | Same patient appeared with different IDs across 3 systems |
| Manual reporting | Finance analysts spent 3+ hours daily joining exports in Excel |
| Inconsistent metrics | Claim denial rates differed by 8–12% depending on which extract was used |
| No operational visibility | Leadership had no daily view of no-show rates or claim turnaround |
| Slow claim reporting | Revenue cycle reports were 5–7 days behind — too late to intervene |

---

## Solution

An end-to-end analytics platform on Microsoft Fabric using **Medallion Architecture** — three Lakehouses (Bronze, Silver, Gold) processing 7,300 records daily from three source systems into a star schema served to Power BI.

```
SOURCE SYSTEMS
├── Azure SQL DB          →  dbo.Patients (1,000 rows — incremental watermark)
├── ADLS Gen2 Shortcut    →  claims.csv (3,500 rows — daily billing export)
└── ADLS Gen2 Shortcut    →  appointments.csv (2,800 rows — EHR export)
         ↓
PL_01_Bronze_Ingestion
├── NB_00 — watermark_control setup
├── NB_01 — Patients: MERGE on PatientID (incremental)
├── NB_02 — Claims: full overwrite
└── NB_03 — Appointments: full overwrite
         ↓
MediCore_Bronze_Lakehouse
├── bronze_patients · bronze_claims · bronze_appointments
└── watermark_control (Delta table)
         ↓
PL_02_Silver_Transform
├── NB_04 — silver_patients (8 quality rules + dedup)
├── NB_05 — silver_claims (10 quality rules + IsDenied + ClaimLagDays)
├── NB_06 — silver_appointments (9 quality rules + IsNoShow + EfficiencyRatio)
└── NB_07 — dim_date (date spine from claims + appointments range)
         ↓
MediCore_Silver_Lakehouse
         ↓
PL_03_Gold_Build
├── NB_08 — dim_patient · dim_provider · dim_facility · dim_date
└── NB_09 — fact_claims (4 FK joins + OPTIMIZE ZORDER)
         ↓
MediCore_Gold_Lakehouse — Star Schema
         ↓
PL_Master_Orchestration → Logic App → Gmail alert on failure
         ↓
Power BI Semantic Model → MediCore Dashboard (3 pages + drillthrough)
```

---

## Architecture

![Architecture Diagram](./architecture/cs2_architecture.svg)

---

## Key Technical Decisions

**Why three separate Lakehouses instead of one?**  
Bronze, Silver, and Gold are owned by different teams with different access levels in production. Bronze contains raw PII exactly as received from the source. Silver contains cleansed data with governance applied. Separating them allows an analyst to be granted access to Silver/Gold without ever seeing raw Bronze PII — a DP-700 governance pattern.

**Why incremental for Patients but full overwrite for Claims and Appointments?**  
Azure SQL has a reliable `LastModifiedDate` watermark column on every row — making incremental ingestion safe and efficient. The ADLS Gen2 CSV exports from the billing and EHR systems have no row-level timestamp — only a service date. Without a watermark, incremental ingestion risks missing updates when records are corrected. Full overwrite guarantees Bronze always mirrors the source exactly.

**Why is watermark_control in Bronze, not Silver?**  
The incremental load decision happens at the Bronze layer — the Bronze notebook reads the old watermark, extracts records newer than that timestamp, writes to `bronze_patients`, then updates the watermark. Silver and Gold have no incremental load logic — they always rebuild fully from Bronze.

**Why full overwrite Silver?**  
Silver is a derived layer — always rebuilt from Bronze. If quality rules change, Silver must be rebuilt from scratch to apply the new rules consistently to all rows. An incremental Silver would accumulate rows processed with different rule versions, producing inconsistent data.

**Why surrogate keys in Gold?**  
The fact table joins on integer keys (PatientKey = 1) instead of string keys (PatientID = 'PAT-000001'). Integer comparisons in Spark and SQL engines are 3–5x faster at scale. Surrogate keys also decouple Gold from source system identifier changes.

**Why ADLS Gen2 shortcuts instead of copying files?**  
A shortcut is a pointer — not a copy. Claims and appointments CSV files stay in ADLS Gen2. The Bronze Lakehouse reads them as if they are local with zero duplication and zero data movement cost. This is a DP-700 exam topic — Create and manage shortcuts to data.

---


## Implementation Screenshots

### Fabric Workspace — 3 Lakehouses
<img width="2462" height="955" alt="image" src="https://github.com/user-attachments/assets/46e9d4d5-f043-4943-a4dd-77d4e03df770" />

<img width="2482" height="371" alt="image" src="https://github.com/user-attachments/assets/5f03880e-5403-4af4-af45-e4aa38a6b708" />



### Bronze Lakehouse — Tables + Shortcut
<img width="2489" height="1020" alt="image" src="https://github.com/user-attachments/assets/47a70d8e-e3f4-4b9c-a91d-7ea6af3c0f97" />


### Watermark Control Table
<img width="2470" height="573" alt="image" src="https://github.com/user-attachments/assets/e9606cb9-e633-4be6-895b-577eb5c58ace" />


### NB_01 — Bronze Patients MERGE Output
<img width="2217" height="1123" alt="image" src="https://github.com/user-attachments/assets/38f3ab4d-a10b-43c1-89d8-4f393b633246" />

### NB_05 — Silver Claims Quality Rules Output
<img width="2202" height="1114" alt="image" src="https://github.com/user-attachments/assets/3401f181-4316-4b83-a765-f4d590da69ca" />

<img width="1504" height="686" alt="image" src="https://github.com/user-attachments/assets/d1ffc709-314d-42c4-acad-94e5568a2a32" />


### NB_09 — Gold Facts Orphan Check + OPTIMIZE
<img width="2093" height="927" alt="image" src="https://github.com/user-attachments/assets/8d440b8c-3767-4c55-92f2-eccc32b5a3a7" />

<img width="2041" height="624" alt="image" src="https://github.com/user-attachments/assets/d3173471-995a-4a6d-8646-23cec0828124" />



### Gold Lakehouse — Star Schema Tables
<img width="2504" height="917" alt="image" src="https://github.com/user-attachments/assets/59b05d8b-fef3-484a-bfaf-064e5a936883" />


### PL_01_Bronze_Ingestion — Pipeline Canvas
<img width="2505" height="594" alt="image" src="https://github.com/user-attachments/assets/1addc801-45b7-4d7d-9d3a-386db21d26c5" />


### PL_Master_Orchestration — Pipeline Canvas
<img width="2440" height="632" alt="image" src="https://github.com/user-attachments/assets/4ef988ad-7274-4639-9f16-64b01fff8081" />


### Logic App — Gmail Alert Configuration
<img width="1846" height="1036" alt="image" src="https://github.com/user-attachments/assets/82ea5a1e-2a1f-46f1-aa85-f9c454c5f798" />

<img width="747" height="644" alt="image" src="https://github.com/user-attachments/assets/4211ea5e-4660-4953-822c-012769f95383" />



### Power BI — Page 1 Executive Overview
**Story:** Here is the health of our revenue cycle at a glance.
<img width="1565" height="894" alt="image" src="https://github.com/user-attachments/assets/74482a51-997c-41d7-9102-775d82d85da3" />


### Power BI — Page 2 Revenue Cycle
**Story:** Where is our money going and why are claims being denied?
<img width="1563" height="891" alt="image" src="https://github.com/user-attachments/assets/5b7d98eb-3ab6-42cd-8108-c691fe4a9410" />


### Power BI — Page 3 Clinical Operations
**Story:** How efficiently are we running our facilities and serving patients?
<img width="1538" height="867" alt="image" src="https://github.com/user-attachments/assets/2dca7b1d-17fd-4417-bed6-dac703da2bf8" />


### Power BI — Payer Drillthrough
<img width="1553" height="869" alt="image" src="https://github.com/user-attachments/assets/55f41afa-4301-4be7-9078-2367550de42b" />


### Power BI — Filter Panel Open
**Story:** Full detail on a single payer — right-click any payer bar to open.
<img width="1303" height="1008" alt="image" src="https://github.com/user-attachments/assets/2fda0870-6eb1-4d8e-b2b9-2fa6f310db98" />



### Power BI — Model View (Relationships)
<img width="2268" height="1367" alt="image" src="https://github.com/user-attachments/assets/b0f48557-876a-4f49-9d73-5d372d9816dc" />

---






*Part of the [Data Engineering Portfolio](../README.md)*
