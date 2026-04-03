# Case Study 02 — Healthcare Medallion Lakehouse


**Industry:** Global Logistics & Supply Chain  
**Platform:** Microsoft Fabric — Data Factory + Lakehouse + PySpark  
**Stack:** Azure SQL · Delta Lake · PySpark · Parquet · DP-600  
---

## Business Problem

MediCore Health Network operates 12 hospitals and 48 outpatient clinics across 6 US states.
Three disconnected source systems — Azure SQL (patient registrations), ADLS Gen2 billing exports
(claims), and EHR exports (appointments) — had never been unified. Analysts spent 70% of their
time preparing data and 30% analysing it.
The ask: Build a unified analytics platform that gives hospital leadership, revenue cycle
managers, and clinical operations teams a single version of the truth — refreshed daily,
with no manual intervention.

#### The ask: 
Build a unified analytics platform that gives hospital leadership, revenue cycle
managers, and clinical operations teams a single version of the truth — refreshed daily,
with no manual intervention.

---
## Solution

An end-to-end Healthcare Medallion Lakehouse in Microsoft Fabric that automates data ingestion and multi-stage transformation using a watermark pattern to deliver a unified, gold-standard Star Schema for clinical and financial analytics.
```text
SOURCE SYSTEMS
├── Azure SQL          → dbo.Patients (1,000 rows, incremental watermark)
├── ADLS Gen2          → claims.csv (3,500 rows, daily export)
└── ADLS Gen2          → appointments.csv (2,800 rows, daily export)

MICROSOFT FABRIC — MEDALLION ARCHITECTURE
├── MediCore_Bronze_Lakehouse
│   ├── bronze_patients        ← incremental MERGE from Azure SQL
│   ├── bronze_claims          ← full overwrite from CSV shortcut
│   ├── bronze_appointments    ← full overwrite from CSV shortcut
│   └── watermark_control      ← Delta table tracks load progress
│
├── MediCore_Silver_Lakehouse
│   ├── silver_patients        ← 8 quality rules + dedup
│   ├── silver_claims          ← 10 quality rules + IsDenied + ClaimLagDays
│   ├── silver_appointments    ← 9 quality rules + IsNoShow + EfficiencyRatio
│   └── dim_date               ← date spine from claim date range
│
└── MediCore_Gold_Lakehouse
    ├── dim_patient            ← surrogate key + patient attributes
    ├── dim_provider           ← extracted from claims
    ├── dim_facility           ← extracted from claims + appointments
    ├── dim_date               ← promoted from Silver
    └── fact_claims            ← 4 FK joins + financial measures

ORCHESTRATION
├── PL_01_Bronze_Ingestion     ← 3 Bronze notebooks in sequence
├── PL_02_Silver_Transform     ← 4 Silver notebooks in sequence
├── PL_03_Gold_Build           ← 2 Gold notebooks in sequence
└── PL_Master_Orchestration    ← chains all 3 + Logic App email alert on failure

POWER BI
└── MediCore Dashboard         ← 3 pages + drillthrough + sliding filter panel
    ├── Page 1: Executive Overview
    ├── Page 2: Revenue Cycle Deep Dive
    ├── Page 3: Clinical Operations
    └── Payer Drillthrough
  ```  
    
---
