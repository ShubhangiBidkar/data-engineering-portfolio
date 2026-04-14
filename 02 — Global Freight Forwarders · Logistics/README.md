# Global Freight Forwarders — The Carrier Intelligence Initiative

**Industry:** Global Logistics & Supply Chain  
**Platform:** Microsoft Fabric — Data Factory + Lakehouse + PySpark  
**Stack:** ADLS Gen2 · Delta Lake · PySpark · Nested JSON · Azure Monitor · DP-700  

---

## About the Company

Global Freight Forwarders (GFF) is a logistics broker operating across 28 countries, connecting shippers with ocean and air carriers for the movement of containerised cargo. Founded in 1994 in Rotterdam, GFF manages 8,400 active shipments at any given time — from automotive components shipped from Hamburg to Singapore, to pharmaceutical cargo routed from Mumbai to New York.

GFF does not own vessels or aircraft. Their value is intelligence — knowing exactly where every shipment is, when it will arrive, and whether a delay is developing before the customer notices. Their revenue model depends on proactive intervention: a delayed shipment caught at the leg level saves a customer $40K in demurrage fees and earns GFF a renewal.

---

## Business Problem

GFF's 8 carrier partners transmit shipment status updates every 6 hours via a REST API. Each API response is a deeply nested JSON object — one parent shipment record containing a `legs` array, each leg containing an `events` array with GPS coordinates, timestamps, and status updates. This three-level nesting makes the data entirely unqueryable in its raw form.

A team of 4 data analysts spent 8 hours every day manually extracting this nested JSON into Excel — copy-pasting leg data, reformatting timestamps, and building pivot tables to identify delayed shipments. By the time the operations dashboard was updated, the data was already 6–12 hours stale. Shipments were routinely missed for intervention because the delay flag appeared in the data only after the customer had already complained.

| Pain Point | Business Impact |
|---|---|
| Nested JSON not parsed | 4 analysts spend 8 hours daily on manual extraction — $180K annual cost |
| Full API re-pull every run | 200MB payload processed entirely even when only 3% of records changed |
| No incremental logic | Cannot identify which shipments changed since the last run |
| 6–12 hour staleness | Operations team cannot intervene on delayed shipments in time |
| No audit trail of API responses | Cannot prove to customers which carrier update caused which delay flag |
| No leg-level analysis | Impossible to identify which port or transport mode causes most delays |
| No structured delay alerting | Delayed shipments identified only when customers complain |

---

## Executive Recommendation: Objective

Design and implement an automated incremental JSON ingestion pipeline in Microsoft Fabric that processes only new and updated shipments since the last run, flattens nested leg and event data into queryable tables, and delivers a carrier intelligence Gold layer refreshed every 6 hours — enabling GFF's operations team to identify and intervene on delayed shipments before the customer notices.

---

## Challenges Identified

🔶 **Three-level JSON nesting** — Shipment → Legs[] → Events[]. Standard SQL cannot query array elements. PySpark's `explode()` function is required to normalise nested arrays into flat queryable rows.

🔶 **Variable array depth** — Each shipment has 1 to 4 legs. Each leg has 1 to 5 events. Fixed-position column extraction fails. A general explode strategy is required.

🔶 **Incremental identification** — The API returns all 1,200 shipments on every call, but only 3–5% are updated each run. Without a watermark the pipeline reprocesses 1,200 rows when only 36 changed — a 97% compute waste.

🔶 **Audit preservation** — The raw API response must be preserved exactly as received before any transformation. If a flattening bug is found months later the data must be reprocessable without re-calling the API.

🔶 **Cross-fact analysis** — Carrier delay rates need to be analysable at both the shipment level and the leg level. Two fact tables must share conformed dimensions — the same `carrier_key = 3` must mean Maersk in both `fact_shipments` and `fact_legs`.

🔶 **Alert on failure** — Pipeline failure must trigger an immediate notification via Azure Monitor — a different alerting approach from Logic Apps and Data Activator used in other case studies.

---

## Solution Approach

✅ Implemented **watermark incremental ingestion** on `shipment.last_modified` — the field set by carriers when they update a shipment's status. On each run only shipments with `last_modified > last_watermark_ts` are processed. Run 1 processed 1,200 shipments. Run 2 processed 180 (150 updated + 30 new) — a 97% reduction in compute.

✅ Built a **two-stage Bronze pattern** — `bronze_shipments_raw` stores the entire API response as a single JSON string blob (immutable audit record, one row per batch). `bronze_shipments_flat`, `bronze_legs`, and `bronze_events` are derived queryable tables. If flattening logic changes, we reprocess from the raw blob without re-calling the API.

✅ Applied **PySpark `explode()` twice** — first to flatten the legs array (1,200 shipments → 3,063 legs), then to flatten the events array within each leg (3,063 legs → 9,300 events). This produces three fully normalised flat tables from a three-level nested JSON structure.

✅ Used **Filter Activity** to select only `.json` files from the landing folder — chosen over Switch Activity because CS2 processes only one format. Filter reduces the file list before ForEach iterates. Switch would add unnecessary complexity with a Default case for a scenario where non-JSON files should simply be excluded not quarantined.

✅ Built a **Gold star schema with conformed dimensions** — `dim_carrier` and `dim_port` are shared between `fact_shipments` and `fact_legs`, enabling cross-fact queries: "For shipments delayed at Hamburg, which transport mode caused the leg-level delay?" This is the Kimball conformed dimension pattern.

✅ Configured **Azure Monitor Action Group** alerting on pipeline failure — independent of pipeline code, fires within 5 minutes of failure detection, demonstrates a third distinct alerting pattern across the portfolio.

---

## Architecture

![Architecture Diagram](./architecture/cs2_architecture.svg)

```
REST API Response (JSON in ADLS Gen2 · Files/api_batches/)
         ↓
PL_GFF_Incremental
├── GetFiles_ApiFolder    → lists all JSON files (parallel)
├── Lookup_Watermark      → reads last_watermark_ts (parallel)
├── Filter_JSON_Files     → keeps only .json files
├── Set_CurrentBatch      → selects batch1 or batch2 based on watermark
├── ForEach_JSON_Batches  → loops over selected batch
│   ├── NB_01_Bronze_Raw      → blob preservation + watermark filter
│   ├── NB_02_Bronze_Flatten  → explode() legs then events
│   └── Move_To_Processed     → file moves out of landing zone
├── NB_03_Silver_Shipments → 10 quality rules + FreightValueBand
├── NB_04_Silver_Legs      → 8 quality rules + DelayCategory
├── NB_05_Gold_Facts       → star schema + OPTIMIZE ZORDER
└── Log_Success            → pipeline_log entry
         ↓ (red failure arrows)
Alert_Failure → Azure Monitor → Action Group → Email
```

---

## New Pattern — Nested JSON Flattening

The key engineering concept introduced in CS2. A three-level nested JSON becomes three flat queryable tables:

```
API JSON (3 levels deep)          After explode() (3 flat tables)
─────────────────────────         ─────────────────────────────────
shipment {                        bronze_shipments_flat
  shipment_id: "GFF-000001"       shipment_id | carrier | status | ...
  carrier: "Maersk"               1,200 rows
  legs: [
    {                             bronze_legs
      leg_id: "LEG-01"            shipment_id | leg_id | origin | ...
      origin: "Hamburg"           3,063 rows (avg 2.5 per shipment)
      events: [
        {                         bronze_events
          event_id: "EVT-01"      shipment_id | leg_id | event_type | lat | lon
          event_type: "Departed"  9,300 rows (avg 3 per leg)
        }
      ]
    }
  ]
}
```

---

## Gold Star Schema

```
dim_carrier              dim_port (role-playing)         dim_date
──────────               ───────────────────────         ────────
carrier_key (PK)         port_key (PK)                   date_key (PK)
carrier_id               port_name                       full_date
carrier_name             country                         year · month
carrier_code             port_code                       quarter · day_name

fact_shipments                              fact_legs
──────────────                              ─────────
carrier_key (FK)                            carrier_key (FK)
origin_port_key (FK → dim_port)             leg_origin_port_key (FK)
dest_port_key (FK → dim_port)               leg_dest_port_key (FK)
departure_date_key (FK)                     shipment_id (→ fact_shipments)
freight_value_usd                           transport_mode
delay_days · is_delayed                     delay_hours · is_delayed_leg
freight_value_band                          delay_category
```

---

## Data Quality Rules

### silver_shipments (10 rules)
| Rule | Check | Action |
|---|---|---|
| 1 | Null `shipment_id` | Drop — PK required |
| 2 | Null `carrier_id` | Drop — cannot attribute |
| 3 | Zero/negative `freight_value_usd` | Drop — invalid |
| 4 | Null origin or destination port | Drop — route undefined |
| 5 | Origin = destination port | Drop — not a valid route |
| 6 | Null `departure_date` | Drop — time analysis impossible |
| 7 | `estimated_arrival` before `departure_date` | Drop — impossible timeline |
| 8 | Standardise `status` to title case | Transform |
| 9 | Invalid `container_count` (0 or >100) | Drop |
| 10 | Deduplicate on `shipment_id` | Keep latest `last_modified` |

### silver_legs (8 rules)
| Rule | Check | Action |
|---|---|---|
| 1 | Null `leg_id` | Drop |
| 2 | Null `shipment_id` | Drop |
| 3 | Null origin or destination port | Drop |
| 4 | Null `leg_sequence` | Drop |
| 5 | Standardise `leg_status` | Transform |
| 6 | Standardise `transport_mode` | Transform |
| 7 | Negative `delay_hours` | Drop |
| 8 | Deduplicate on `leg_id` | Keep latest |

---

## Dataset

| File | Shipments | Legs | Events | Purpose |
|---|---|---|---|---|
| `shipments_batch1.json` | 1,200 | ~3,063 | ~9,300 | Initial full load — Jan 2023 to Mar 2024 |
| `shipments_batch2.json` | 180 | ~487 | ~1,601 | Incremental — 150 updated + 30 new |

**Realistic data includes:**
- 8 real ocean carriers: Maersk, MSC, CMA CGM, Hapag-Lloyd, COSCO, Evergreen, Yang Ming, ONE
- 16 real global ports: Shanghai, Singapore, Rotterdam, Hamburg, Los Angeles, Dubai, Busan, Antwerp, Hong Kong, New York, Felixstowe, Tanjung Pelepas, Jebel Ali, Valencia, Colombo, Qingdao
- 20 real vessel names
- 16.8% shipment delay rate — realistic for ocean freight
- 5 event types per leg: Departed, Arrived, Customs Cleared, Customs Hold, Delayed
- Freight values $15K–$2M per shipment
- Variable leg count (1–4) and event count (1–5) per leg

---

## Business Outcomes

🟢 **RELIABILITY** — Complete elimination of manual JSON extraction. 4 analysts freed from 8-hour daily extraction work. Zero risk of manual copy-paste errors in delay flag calculation.

🟢 **SPEED** — Operations dashboard updated every 6 hours automatically, matching API refresh frequency. Delayed shipments flagged at the leg level — before the shipment-level delay is visible — enabling proactive customer intervention.

🟢 **COMPLIANCE** — Full audit trail via `bronze_shipments_raw` (every API response preserved as blob) and `pipeline_log`. Every run timestamped. Carrier-specific delay attribution provable to customers.

🟢 **SCALABILITY** — 97% compute reduction on incremental runs. Adding a new carrier API requires only pointing a new JSON file at the landing folder. The watermark pattern works identically for any number of sources.

---

## Results

| Metric | Before | After |
|---|---|---|
| Daily analyst extraction time | 8 hours (4 analysts) | Fully automated |
| Annual extraction cost | ~$180K salary | Pipeline compute only |
| Data freshness | 6–12 hour lag | Every 6 hours automated |
| Records processed per incremental run | 1,200 (full reload) | ~180 (3% delta) |
| Compute reduction | Baseline | 97% on incremental runs |
| Leg-level delay analysis | Impossible | Available in `silver_legs` |
| Audit trail | None | `bronze_shipments_raw` — full API history |

---

## Implementation Screenshots

### GFF Lakehouse — Bronze, Silver, Gold Schemas
![Schemas](./screenshots/01_lakehouse_schemas.png)

### Files/api_batches — Both JSON Batches
![API Files](./screenshots/02_files_api_batches.png)

### Watermark Control Table — Seeded at 1900
![Watermark](./screenshots/03_watermark_control_seeded.png)

### NB_01 — Raw Blob Stored + Watermark Filter
![NB_01](./screenshots/04_nb01_bronze_raw_output.png)

### NB_02 — Legs and Events Exploded
![NB_02](./screenshots/05_nb02_flatten_output.png)

### NB_03 — Silver Shipments Quality Rules
![NB_03](./screenshots/06_nb03_silver_shipments_output.png)

### NB_04 — Silver Legs Delay Categories
![NB_04](./screenshots/07_nb04_silver_legs_output.png)

### NB_05 — Gold Star Schema Built
![NB_05](./screenshots/08_nb05_gold_facts_output.png)

### Pipeline Canvas — Full View
![Pipeline](./screenshots/09_pipeline_canvas.png)

### Filter Activity Settings
![Filter](./screenshots/10_filter_activity_settings.png)

### ForEach — Inside Activities
![ForEach](./screenshots/11_foreach_inside_activities.png)

### Pipeline Run 1 — Batch1 Success
![Run1](./screenshots/12_pipeline_run1_success.png)

### Pipeline Run 2 — Incremental Batch2
![Run2](./screenshots/13_pipeline_run2_incremental.png)

### Watermark Advanced After Run 2
![Watermark Advanced](./screenshots/14_watermark_advanced.png)

### Pipeline Log — Both Runs
![Log](./screenshots/15_pipeline_log_table.png)

### Azure Monitor Alert Rule
![Monitor](./screenshots/16_azure_monitor_alert_rule.png)

---

## Files

```
02-global-freight-forwarders/
├── README.md
├── architecture/
│   └── cs2_architecture.svg
├── data/
│   ├── shipments_batch1.json          ← 1,200 shipments · 3-level nested
│   ├── shipments_batch2.json          ← 180 shipments · incremental
│   └── dataset_reference.json
├── notebooks/
│   ├── NB_00_Setup.py
│   ├── NB_01_Bronze_Raw.py
│   ├── NB_02_Bronze_Flatten.py
│   ├── NB_03_Silver_Shipments.py
│   ├── NB_04_Silver_Legs.py
│   ├── NB_05_Gold_Facts.py
│   ├── NB_Log_Success_GFF.py
│   └── NB_Alert_Failure_GFF.py
└── screenshots/
```



## Concepts Covered

| Concept | Where demonstrated |
|---|---|
| Incremental ingestion — watermark | `watermark_control` + NB_01 watermark filter on `last_modified` |
| Nested JSON flattening | NB_02 — `explode()` on legs array then events array |
| Two-stage Bronze | Raw blob (audit) + flat tables (queryable) |
| Filter Activity | Selects `.json` files — chosen over Switch for single-format scenario |
| Filter vs Switch decision | Filter for same-format · Switch for multi-format routing |
| Conformed dimensions | `dim_carrier` + `dim_port` shared by `fact_shipments` + `fact_legs` |
| Role-playing dimension | `dim_port` used twice on `fact_shipments` — origin and destination |
| Unknown member (key=0) | Null-safe joins — orphan facts get key=0 not NULL |
| OPTIMIZE + ZORDER | Both fact tables optimised on common filter columns |
| Azure Monitor alerting | Action Group fires on pipeline failure — no webhook code |
| Idempotent Bronze | Delete by ID before append — safe to rerun |
| Full overwrite Silver | Always rebuilt from Bronze |

---

## Interview Q&A

**Q: Why watermark on `last_modified` and not `extraction_timestamp`?**  
`extraction_timestamp` is when we called the API — it changes every run regardless of whether any shipment data changed. `last_modified` is when a carrier updated a specific shipment's status. Watermarking on `last_modified` at the shipment level precisely identifies the 3–5% of shipments that genuinely changed since the last run. Using `extraction_timestamp` would mark all 1,200 shipments as new on every run — defeating the purpose of incremental ingestion entirely.

**Q: What does explode() do and why is it needed for this JSON?**  
`explode()` takes an array column and creates one row per element. A shipment with 3 legs produces 3 rows after the first explode. A leg with 4 events produces 4 rows after the second explode. Without explode you cannot write a SQL query that filters on event_type or groups by origin_port — those values are buried inside nested array structs. Explode normalises the nested data into flat tables that SQL, DAX, and PySpark window functions can operate on normally.

**Q: Why store the raw JSON blob before flattening?**  
The raw blob in `bronze_shipments_raw` is the immutable audit record — one row per API batch. If the flattening logic has a bug months later we fix the notebook and reprocess from the raw blob without re-calling the API. Without the raw blob, a bug means the data is gone — the API only returns current state, not historical states. This is the standard enterprise pattern for API ingestion: preserve raw first, transform second.

**Q: What is a conformed dimension and why does it matter for GFF?**  
A conformed dimension is shared identically across multiple fact tables. `dim_port` is conformed — port_key = 5 means Hamburg in both `fact_shipments` and `fact_legs`. This enables cross-fact queries: "For shipments originating from Hamburg, which transport mode caused the most leg-level delays?" Without conformance, Hamburg in each fact table would have different keys — the join would produce wrong results or require an additional mapping layer.

**Q: Why Filter Activity for CS2 but Switch Activity for CS1 Blackwood?**  
CS1 had 3 different file formats each needing different processing notebooks — routing was the core requirement. Switch evaluates one expression and routes to multiple outcomes, which is exactly right for multi-format routing. CS2 processes only JSON files — there is no routing decision. Filter reduces the file list to JSON only before the ForEach loop. If a non-JSON file appears it is excluded, not quarantined, because GFF's API only ever produces JSON. Using Switch here would add an unnecessary Default case for a scenario that should not occur.

---

*Part of the [Data Engineering Portfolio](../README.md) — 22 case studies across Microsoft Fabric and Azure Databricks*
