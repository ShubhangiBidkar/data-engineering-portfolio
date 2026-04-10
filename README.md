# Data Engineering Portfolio
### Microsoft Fabric · Azure Databricks · PySpark · Delta Lake · DP-700 · DP-600 · Databricks Associate

---

## About

End-to-end data engineering portfolio built across 22 real-world case studies covering the full modern data stack. Each case study is a self-contained project with a realistic business problem, production-grade datasets, PySpark notebooks, orchestration pipelines, and documented architecture.


**Platform split:**
- CS1–CS13: Microsoft Fabric (Lakehouse · Data Factory · Power BI · KQL · Data Activator)
- CS14–CS22: Azure Databricks (Unity Catalog · Delta Live Tables · Workflows · MLflow)

---

## Portfolio Progress

```
Phase 1 — Microsoft Fabric    ████████░░░░░░  3 of 13 complete
Phase 2 — Azure Databricks    ░░░░░░░░░░░░░░  0 of 9 complete
```

---

## Phase 1 — Microsoft Fabric Case Studies (CS1–CS13)

### CS1 — Blackwood Estates · Real Estate
**Core challenge:** Wildcard file loading · changing data shapes · multi-format ingestion

**Business problem:**
Blackwood Estates operates across 14 countries. Three regional offices send property listing files monthly in different formats (CSV, JSON, Excel) with conflicting schemas, different column names, different date formats, and no arrival schedule. A data analyst spent 3 days per month manually consolidating them into a master spreadsheet.

**Solution:** Metadata-driven pipeline using Get Metadata + ForEach + Switch routing. Files classified by extension and routed to format-specific PySpark notebooks. Unsupported files quarantined automatically. Unified Silver schema resolves all schema conflicts.

**Key patterns:** Switch activity · wildcard file discovery · openpyxl → Pandas → Spark · quarantine pattern · idempotent Bronze · sqm → sqft conversion · ISO2 country resolution

**Status:** ✅ Complete → [View case study](./01-blackwood-estates/)

---

### CS2 — Global Freight Forwarders · Logistics
**Core challenge:** Incremental ingestion · nested JSON · deduplication

**Business problem:**
Global Freight Forwarders receives shipment status updates from carrier partners via REST API every 6 hours. Each API response is deeply nested JSON — one parent shipment with multiple legs, each leg with multiple events. Manual extraction takes 8 hours daily, leaving the operations dashboard 6–12 hours stale and unable to intervene on delayed shipments in time.

**Solution:** Incremental JSON ingestion pipeline using watermark pattern. Raw JSON preserved in Bronze for audit. Nested arrays flattened using PySpark explode() across 3 tables — shipments, legs, events. Azure Monitor alerting on pipeline failure and delay rate threshold.

**Key patterns:** Nested JSON flattening · explode() on arrays · multi-hop Bronze · watermark incremental · Azure Monitor alerts · delay detection

**Status:** 🔜 In progress → [View case study](./02-global-freight-forwarders/)

---

### CS3 — Metropolis Data Lakehouse · Government
**Core challenge:** Dynamic router pipeline · quarantine · multi-source consolidation

**Business problem:**
Metropolis City Council receives data from 14 government departments — planning, transport, utilities, social services — each with different file formats, delivery schedules, and data quality standards. There is no single ingestion framework. Failed files block downstream reporting for the entire city.

**Solution:** Dynamic router pipeline that classifies incoming files by department and quality tier, routes to department-specific processing, quarantines files that fail validation, and consolidates into a unified city data lakehouse.

**Key patterns:** Dynamic routing · department-specific quality rules · quarantine with reprocessing · cross-department joins · government data governance

**Status:** 🔜 Planned → [View case study](./03-metropolis-datalakehouse/)

---

### CS4 — Innovate Solutions · Technology
**Core challenge:** PySpark column-level encryption · salary data · PII protection

**Business problem:**
Innovate Solutions processes employee salary data across 12 countries with different privacy regulations. Salary figures, national ID numbers, and bank account details must be encrypted at the column level before landing in the Lakehouse. Different analyst roles should see different levels of data masking.

**Solution:** PySpark column-level encryption using AES-256 with Azure Key Vault key management. Role-based data masking at the Silver layer. Audit log of every access to encrypted columns.

**Key patterns:** Column-level encryption · AES-256 · Azure Key Vault integration · role-based masking · PII classification · encryption audit log

**Status:** 🔜 Planned → [View case study](./04-innovate-solutions/)

---

### CS5 — Global Corp Finance · Financial Services
**Core challenge:** Broadcast joins · Delta Time Travel · large table optimisation

**Business problem:**
Global Corp Finance runs daily reconciliation between a 500-million-row transaction ledger and a 2-million-row account master. The join takes 4 hours on their current Spark cluster due to shuffle spills. Historical reconciliation requires point-in-time snapshots — regulators need to see exactly what the data looked like on any given day.

**Solution:** Broadcast join for the account master (small side), AQE-optimised shuffle for the ledger (large side), ZORDER optimisation on join keys. Delta Time Travel for point-in-time snapshots with configurable retention.

**Key patterns:** Broadcast join hints · AQE · skew handling · Delta Time Travel · RESTORE command · point-in-time queries · reconciliation pattern

**Status:** 🔜 Planned → [View case study](./05-global-corp-finance/)

---

### CS6 — Smart Pricing Pilot · Energy
**Core challenge:** Conditional logic · Phantom Spike filtration · anomaly detection

**Business problem:**
Smart Pricing Pilot collects electricity consumption readings from 2.4 million smart meters every 15 minutes. Roughly 0.3% of readings are phantom spikes — meter malfunctions that record 10-100x normal consumption. These spikes corrupt pricing models and inflate customer bills. Current manual review catches only 40% of spikes.

**Solution:** Statistical anomaly detection in PySpark using rolling window Z-score. Reads classified as phantom spikes are quarantined, flagged for meter investigation, and excluded from pricing calculations. Legitimate high-consumption events (e.g. industrial demand) preserved.

**Key patterns:** Rolling window functions · Z-score anomaly detection · conditional spike filtration · smart meter data at scale · 15-minute interval processing

**Status:** 🔜 Planned → [View case study](./06-smart-pricing-pilot/)

---

### CS7 — North Meridian Retail · Retail
**Core challenge:** 3NF to Star Schema conversion · dimensional modelling

**Business problem:**
North Meridian Retail operates a legacy OLTP database in Third Normal Form — 47 tables, heavily normalised, with recursive category hierarchies and multi-currency pricing. Analysts cannot run meaningful queries without 12-table joins. The business needs a dimensional model for Power BI.

**Solution:** Full 3NF to Star Schema conversion in Fabric. Recursive category hierarchy flattened using PySpark self-joins. Multi-currency pricing resolved to USD at ingestion. Conformed dimensions shared across 3 fact tables — sales, inventory, returns.

**Key patterns:** 3NF normalisation · star schema design · recursive hierarchy flattening · conformed dimensions · slowly changing dimensions Type 1 · multi-currency conversion

**Status:** 🔜 Planned → [View case study](./07-north-meridian-retail/)

---

### CS8 — Global Credit Corp · Financial Services
**Core challenge:** T-SQL API ingestion · JSON currency rates · real-time FX

**Business problem:**
Global Credit Corp processes loan applications across 38 currencies. Exchange rates are pulled from an external REST API every hour. The current process hardcodes rates in a spreadsheet updated manually — rates are sometimes 3 days stale, causing incorrect loan valuations and compliance failures.

**Solution:** Fabric SQL Database with T-SQL stored procedures pulling live FX rates from the API. Rates stored as a versioned Delta table — every rate has a valid_from and valid_to timestamp. Loan valuations always use the rate that was active at the time of the transaction.

**Key patterns:** T-SQL API ingestion · Fabric SQL Database · JSON parsing in T-SQL · versioned reference data · bi-temporal rate tables · FX conversion at query time

**Status:** 🔜 Planned → [View case study](./08-global-credit-corp/)

---

### CS9 — Veritas Global · Legal
**Core challenge:** Defensive T-SQL · SCD Type 2 · recursive hierarchies

**Business problem:**
Veritas Global manages legal case files across 22 jurisdictions. Each case has a client hierarchy — a parent company with subsidiaries and sub-subsidiaries. Case status changes must be tracked historically — regulators require a full audit of every status change with who made it and when. Defensive SQL patterns required for all writes.

**Solution:** SCD Type 2 implementation in Fabric SQL Database with full history tracking. Recursive CTEs to resolve client hierarchies. Defensive T-SQL with TRY/CATCH, explicit transactions, and deadlock retry logic.

**Key patterns:** SCD Type 2 · MERGE for history tracking · recursive CTEs · defensive T-SQL · TRY/CATCH · explicit transactions · jurisdiction-aware partitioning

**Status:** 🔜 Planned → [View case study](./09-veritas-global/)

---

### CS10 — Inventory Data Migration · Logistics
**Core challenge:** On-premises gateway · PostgreSQL to Lakehouse · migration pattern

**Business problem:**
Inventory Data Migration Co needs to migrate 12 years of inventory history from an on-premises PostgreSQL database to Microsoft Fabric. The database is 800GB, contains 200 tables, and must remain operational during migration. Zero data loss and zero downtime are hard requirements.

**Solution:** On-premises data gateway to connect Fabric pipelines to the local PostgreSQL. Incremental migration strategy — historical data migrated in date-range batches, live data incrementally synced. Validation queries confirm row counts and checksums match before cutover.

**Key patterns:** On-premises gateway setup · PostgreSQL JDBC connector · batched historical migration · incremental sync pattern · checksum validation · cutover strategy

**Status:** 🔜 Planned → [View case study](./10-inventory-migration/)

---

### CS11 — CareNet Hospitals · Healthcare
**Core challenge:** Fabric SQL Database · GraphQL API · clinical data integration

**Business problem:**
CareNet Hospitals uses a modern EHR system that exposes patient data only through a GraphQL API — no direct database access. Clinical data needs to land in Fabric for analytics. GraphQL queries are complex — nested fragments, pagination, and rate limiting make bulk extraction non-trivial.

**Solution:** Python-based GraphQL client that paginates through the API, handles rate limits with exponential backoff, and lands raw responses in Bronze. Fabric SQL Database for relational clinical views. Cross-Lakehouse joins between clinical data and the hospital's financial data in Gold.

**Key patterns:** GraphQL pagination · rate limit handling · exponential backoff · Fabric SQL Database · cross-lakehouse joins · clinical data governance · HIPAA-aligned masking

**Status:** 🔜 Planned → [View case study](./11-carenet-hospitals/)

---

### CS12 — TelcoPrime · Telecom
**Core challenge:** Format standardisation · upstream quality checks · CDR processing

**Business problem:**
TelcoPrime processes 800 million Call Detail Records (CDRs) daily from 14 network switches, each producing files in a slightly different format — different column orders, different timestamp formats, different encoding for international numbers. Downstream billing fails when CDR format deviates from expected.

**Solution:** Format standardisation layer that normalises all 14 CDR formats before any downstream processing. Upstream quality checks catch format deviations at the Bronze boundary and alert the network operations team before billing is affected.

**Key patterns:** CDR processing at scale · format normalisation · upstream quality gates · international number standardisation · high-volume partitioning strategy · billing pipeline integration

**Status:** 🔜 Planned → [View case study](./12-telcoprime/)

---

### CS13 — Live Market Intelligence · Capital Markets
**Core challenge:** Eventstream · KQL · time-windowed aggregations · Data Activator

**Business problem:**
Live Market Intelligence provides real-time market data to hedge funds. Stock prices, options chains, and order book depth stream in at 50,000 events per second. Analysts need 1-minute, 5-minute, and 15-minute VWAP (Volume Weighted Average Price) windows updated in real time. Anomalous price movements must trigger alerts within 3 seconds.

**Solution:** Fabric Eventstream ingesting market data feed. KQL queries for time-windowed aggregations. Eventhouse (KQL Database) for ultra-low-latency reads. Data Activator monitors VWAP deviation and fires alerts within the 3-second SLA.

**Key patterns:** Eventstream ingestion · KQL time windows · VWAP calculation · Eventhouse · Data Activator real-time alerts · streaming vs batch hybrid · tick data handling

**Status:** 🔜 Planned → [View case study](./13-live-market-intelligence/)

---

## Phase 2 — Azure Databricks Case Studies (CS14–CS22)

### CS14 — SanteFlux Privacy · Health Tech
**Core challenge:** Salted hashing · PII · Unity Catalog data governance

**Business problem:**
SanteFlux processes health records for 8 million patients across 6 countries under GDPR, HIPAA, and local regulations. Patient identifiers must be pseudonymised before leaving the secure processing zone. Re-identification must be mathematically infeasible even if the hash table is compromised.

**Solution:** Salted SHA-256 hashing for patient identifiers with per-patient salts stored in a separate secure vault. Unity Catalog column masking policies enforce that only authorised roles see raw PII. Automated PII scan on every new table registered in Unity Catalog.

**Key patterns:** Salted hashing · SHA-256 · Unity Catalog column masking · PII classification · GDPR right-to-erasure implementation · data product certification

**Status:** 🔜 Planned → [View case study](./14-santeflux-privacy/)

---

### CS15 — AeroSurf BlueGuard · Public Safety
**Core challenge:** Schema drift · SCD Type 2 · sensor data evolution

**Business problem:**
AeroSurf BlueGuard ingests data from 4,000 aerial surveillance sensors. Sensor firmware updates every 3 months introduce new fields, rename existing fields, and occasionally drop fields. The pipeline must handle schema drift gracefully — new fields should be captured, renamed fields should be mapped, dropped fields should be preserved historically.

**Solution:** Schema drift detection using Delta schema evolution with mergeSchema. SCD Type 2 tracking for sensor metadata — when a sensor's field schema changes, the old schema version is closed and a new version opened. Historical reads always use the schema version active at the time.

**Key patterns:** Schema drift detection · mergeSchema · SCD Type 2 on sensor metadata · schema versioning · backward compatibility · firmware change tracking

**Status:** 🔜 Planned → [View case study](./15-aerosurf-blueguard/)

---

### CS16 — Northwind Traders · Retail
**Core challenge:** Spark internals · lazy evaluation · file fragmentation

**Business problem:**
Northwind Traders' Databricks pipelines take 6 hours to run despite only processing 50GB of data. Profiling reveals 180,000 small files (the small file problem), excessive shuffles on string join keys, and no partition pruning. The team does not understand why adding more workers makes performance worse.

**Solution:** Deep Spark internals investigation — file compaction with OPTIMIZE, Z-ordering on join keys, partition strategy redesign, broadcast join for small dimensions, AQE enabled. Explains lazy evaluation, DAG optimisation, and why more workers do not help with shuffle-bound jobs.

**Key patterns:** Spark DAG analysis · small file problem · OPTIMIZE · Z-ordering · broadcast hints · AQE · shuffle optimisation · partition pruning · explain() plan reading

**Status:** 🔜 Planned → [View case study](./16-northwind-traders/)

---

### CS17 — NordicStream AdTech · Advertising
**Core challenge:** Data skew · performance tuning · salting

**Business problem:**
NordicStream's ad impression pipeline joins a 2-billion-row impressions table to an advertiser dimension table. 3 advertisers (Google, Meta, Amazon) account for 60% of all impressions, causing severe data skew — those 3 partition keys take 45 minutes while all other partitions finish in 2 minutes.

**Solution:** Salting technique to distribute skewed keys across multiple partitions. Salted join with duplicate advertiser dimension rows. Before/after performance comparison showing 45-minute partition reduced to 4 minutes. AQE skew join hint as alternative approach.

**Key patterns:** Data skew identification · salting technique · skewed join handling · AQE skew join · partition statistics · Spark UI interpretation · before/after benchmarking

**Status:** 🔜 Planned → [View case study](./17-nordicstream-adtech/)

---

### CS18 — EcoFlusso Italia · Green Energy
**Core challenge:** Data quality rules framework · expectation enforcement

**Business problem:**
EcoFlusso Italia aggregates energy production data from 3,200 solar and wind installations across Italy. Each installation has different acceptable ranges for output values — a 500kW solar array has different valid output bounds than a 5MW wind turbine. A single quality rules engine must handle all installations with per-asset configuration.

**Solution:** Configurable data quality rules framework in PySpark. Rules stored in a Delta configuration table — each asset has its own min/max bounds, null tolerance, and freshness requirement. Rules engine evaluates every record against its asset-specific rules and routes failures to a quality report table.

**Key patterns:** Configurable quality rules engine · rules-as-data pattern · per-asset configuration · quality scoring · Great Expectations integration · quality dashboard

**Status:** 🔜 Planned → [View case study](./18-ecoflusso-italia/)

---

### CS19 — EuroStream Finance · FinTech
**Core challenge:** Spark Declarative Pipelines · quality gates · DLT

**Business problem:**
EuroStream Finance processes payment transactions across 28 EU countries under PSD2 regulation. Pipelines must enforce data quality gates — a transaction with a missing beneficiary IBAN must never reach the Gold layer. Quality failures must be tracked, reported to compliance, and retried after remediation.

**Solution:** Delta Live Tables (DLT) pipeline with declarative quality expectations. EXPECT clauses quarantine invalid transactions. Quality metrics surfaced in the DLT event log. Compliance report generated from quality failure history.

**Key patterns:** Delta Live Tables · EXPECT quality gates · quarantine tables · DLT event log · pipeline lineage · compliance reporting · declarative vs imperative pipelines

**Status:** 🔜 Planned → [View case study](./19-eurostream-finance/)

---

### CS20 — PixelStrike Gaming · Gaming
**Core challenge:** Custom Databricks App · match log aggregation · player analytics

**Business problem:**
PixelStrike Gaming generates 2 billion match events per day across 140 countries. Players need to see their personal performance stats within 60 seconds of a match ending. The current batch pipeline takes 4 hours — players are abandoning the app before their stats appear.

**Solution:** Custom Databricks App for real-time match log aggregation. Structured Streaming for near-real-time player stat computation. Delta as the serving layer — reads optimised for single-player lookups. Custom Databricks App exposes stats via REST endpoint consumed by the game client.

**Key patterns:** Structured Streaming · stateful aggregation · Databricks Apps · low-latency serving · player cohort analysis · tournament bracket processing · gaming event schema

**Status:** 🔜 Planned → [View case study](./20-pixelstrike-gaming/)

---

### CS21 — AgriYield · Agriculture
**Core challenge:** Unity Catalog · Row Level Security · multi-tenant data sharing

**Business problem:**
AgriYield provides precision agriculture analytics to 4,000 farms across 8 countries. Each farm must only see their own yield data — but AgriYield's data science team needs aggregated cross-farm insights for model training. Data sharing agreements with 3 agricultural research universities require controlled access to anonymised data.

**Solution:** Unity Catalog with row-level security enforced at the table level. Farm-level data isolation using dynamic views with session_user() predicates. Anonymised research dataset shared via Delta Sharing with the universities. Data lineage tracked end to end through Unity Catalog.

**Key patterns:** Unity Catalog RLS · dynamic views · Delta Sharing · data products · multi-tenant isolation · anonymisation for research · data lineage tracking

**Status:** 🔜 Planned → [View case study](./21-agriyield/)

---

### CS22 — Velocity Sports · Sports
**Core challenge:** Multi-task Databricks Workflow · GPS data · real-time athlete analytics

**Business problem:**
Velocity Sports provides performance analytics to 12 professional sports teams. GPS trackers on 800 athletes generate location, speed, acceleration, and heart rate data at 10Hz — 10 readings per second per athlete. During live matches the coaching team needs real-time fatigue scores to make substitution decisions. Post-match the full session must be processed for detailed performance reports.

**Solution:** Multi-task Databricks Workflow — Task 1 ingests raw GPS streams, Task 2 computes rolling fatigue scores, Task 3 generates position heatmaps, Task 4 produces the coaching dashboard feed, Task 5 runs the full post-match report. Tasks run in dependency order with conditional branching — Task 5 only runs after the match ends.

**Key patterns:** Multi-task Workflows · task dependencies · conditional branching · GPS data processing · rolling window fatigue scoring · heatmap generation · real-time vs post-match hybrid

**Status:** 🔜 Planned → [View case study](./22-velocity-sports/)

---

## Foundation Case Studies

Built before the main 22-case curriculum to establish core patterns. Referenced throughout the portfolio.

| Study | Focus | Patterns |
|---|---|---|
| [FastFreight Logistics](./foundation-fastfreight/) | Incremental ingestion · watermark pattern | Lookup + Copy + Notebook · Delta MERGE · idempotency |
| [MediCore Healthcare](./foundation-medicore/) | Medallion architecture · star schema · Power BI | Bronze/Silver/Gold · surrogate keys · HTML DAX visuals |

---

## Skills Matrix

| Skill | Case studies |
|---|---|
| Incremental ingestion (watermark) | FastFreight · CS2 · CS8 |
| Full overwrite ingestion | CS1 · CS3 |
| Medallion architecture | MediCore · CS3 · CS6 |
| Nested JSON flattening | CS2 |
| Wildcard file loading | CS1 |
| Multi-format ingestion (CSV/JSON/Excel) | CS1 |
| Schema drift handling | CS1 · CS15 |
| SCD Type 2 | CS9 · CS15 |
| Star schema design | MediCore · CS7 |
| 3NF to Star Schema | CS7 |
| Column-level encryption | CS4 |
| PII and data masking | CS4 · CS14 |
| Row Level Security | CS21 |
| Unity Catalog | CS14 · CS21 |
| Broadcast joins | CS5 |
| Data skew + salting | CS17 |
| Spark internals | CS16 |
| Delta Time Travel | CS5 |
| OPTIMIZE + ZORDER + VACUUM | FastFreight · MediCore · CS1 · CS2 |
| Quarantine pattern | CS1 · CS3 |
| Structured Streaming | CS13 · CS20 |
| KQL + Eventstream | CS13 |
| Delta Live Tables | CS19 |
| Data quality rules framework | CS18 |
| Azure Monitor alerting | CS2 |
| Data Activator alerting | CS13 |
| Logic App alerting | MediCore |
| GraphQL API ingestion | CS11 |
| T-SQL API ingestion | CS8 |
| On-premises gateway | CS10 |
| Databricks Workflows | CS22 |
| Databricks Apps | CS20 |
| Delta Sharing | CS21 |
| Power BI HTML visuals | MediCore |
| Power BI field parameters | MediCore · CS1 |

---

## Repository Structure

```
data-engineering-portfolio/
├── README.md                          ← this file
├── foundation-fastfreight/            ← incremental ingestion foundation
├── foundation-medicore/               ← medallion architecture foundation
├── 01-blackwood-estates/              ← wildcard file loading ✅
├── 02-global-freight-forwarders/      ← JSON incremental 🔜
├── 03-metropolis-datalakehouse/       ← dynamic router 🔜
├── 04-innovate-solutions/             ← column encryption 🔜
├── 05-global-corp-finance/            ← broadcast joins 🔜
├── 06-smart-pricing-pilot/            ← anomaly detection 🔜
├── 07-north-meridian-retail/          ← 3NF to star schema 🔜
├── 08-global-credit-corp/             ← T-SQL API ingestion 🔜
├── 09-veritas-global/                 ← SCD Type 2 🔜
├── 10-inventory-migration/            ← on-premises gateway 🔜
├── 11-carenet-hospitals/              ← GraphQL API 🔜
├── 12-telcoprime/                     ← format standardisation 🔜
├── 13-live-market-intelligence/       ← Eventstream + KQL 🔜
├── 14-santeflux-privacy/              ← salted hashing + Unity Catalog 🔜
├── 15-aerosurf-blueguard/             ← schema drift + SCD 🔜
├── 16-northwind-traders/              ← Spark internals 🔜
├── 17-nordicstream-adtech/            ← data skew + salting 🔜
├── 18-ecoflusso-italia/               ← quality rules framework 🔜
├── 19-eurostream-finance/             ← Delta Live Tables 🔜
├── 20-pixelstrike-gaming/             ← Databricks Apps 🔜
├── 21-agriyield/                      ← Unity Catalog RLS 🔜
└── 22-velocity-sports/                ← multi-task Workflows 🔜
```

---

## Tech Stack

| Category | Technologies |
|---|---|
| Platforms | Microsoft Fabric · Azure Databricks · Azure Data Lake Storage Gen2 |
| Languages | PySpark · Python · SQL · DAX · KQL · T-SQL |
| Storage | Delta Lake · Parquet · JSON · CSV · Excel |
| Orchestration | Fabric Data Pipelines · Databricks Workflows · Delta Live Tables |
| Serving | Power BI · KQL Database (Eventhouse) · Databricks SQL |
| Governance | Unity Catalog · Microsoft Purview · Row Level Security |
| Alerting | Azure Monitor · Logic Apps · Data Activator · Power Automate |

