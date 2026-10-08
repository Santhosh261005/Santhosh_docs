# Product Performance & Inventory Optimization Intelligence
## Fixed Implementation Blueprint

> **Project goal:** Build an enterprise-style, product-centric data platform for Odoo e-commerce data that supports initial multi-batch ingestion, incremental batch processing, CDC/WAL processing, later Kafka streaming, Medallion Architecture, Iceberg-based curated layers, Snowflake serving, stakeholder-specific Streamlit dashboards, and a governed AI analytics layer.

---

# 1. Business Scope

The capstone remains strictly **product-centric**.

## Core analytical KPIs

1. **Sales Velocity**
2. **Days of Stock Cover**
3. **ABC Product Classification**
4. **Inventory Health Classification**
   - Stockout Risk
   - Healthy
   - Overstock
   - Non-Moving

Procurement, suppliers, inventory, sales, and categories are included only where they improve product-level decisions.

---

# 2. Architecture Principles

The implementation will follow these rules from the beginning:

- Bronze is **append-only and immutable** from an analytical perspective.
- Silver contains **trusted, conformed current/history state** depending on the entity.
- Gold contains **business-ready dimensional models and KPI marts**.
- Every processing stage must be **idempotent**.
- Batch and streaming use different ingestion mechanisms but **converge onto common downstream processing logic**.
- Source paths, HDFS paths, S3 paths, table names, checkpoints, credentials, and environment settings are **configuration-driven**.
- Reusable logic belongs in common Python libraries, not repeated inside DAGs or jobs.
- Source manifests are treated as an **ingestion contract**.
- WAL events preserve transaction metadata and are processed only according to valid transaction semantics.
- Partitioning is based on data volume and access patterns, not blindly applied to every dataset.
- AI consumes trusted Gold/semantic data rather than rebuilding business rules from Bronze.

---

# 3. Target End-to-End Architecture

```text
                         ODOO / SOURCE
                              |
              +---------------+---------------+
              |                               |
       API FILE BATCHES                   WAL / CDC
       initial 40 batches                WAL files first
              |                         Kafka later
              v                               |
     landing_zone/landing                     |
              |                               |
              +---------------+---------------+
                              |
                        AIRFLOW CONTROL
                              |
                 manifest + file validation
                              |
                              v
                    BRONZE / RAW ZONE
                   append-only history
                              |
                       schema validation
                     +--------+--------+
                     |                 |
                     v                 v
                 GOOD DATA         BAD DATA
                     |            / QUARANTINE
                     |
                     v
               BRONZE -> SILVER
               business DQ rules
               Great Expectations
                     |
                     v
                  SILVER
             Apache Iceberg tables
             MERGE / UPSERT / SCD
                     |
                     v
                   GOLD
             Star schema + KPI marts
             Apache Iceberg tables
                     |
                     v
                 SNOWFLAKE
                     |
             +-------+--------+
             |                |
             v                v
        STREAMLIT         AI ANALYTICS
       stakeholder        Agent / MCP
       dashboards         SQL / RAG / Charts
```

A separate **control plane** spans the complete pipeline:

```text
configuration
batch registry
run registry
reconciliation
DQ metrics
schema contracts
checkpoints
logging
monitoring
rollback metadata
```

---

# 4. Recommended Repository Structure

```text
product-intelligence-capstone/
|
+-- README.md
+-- pyproject.toml
+-- requirements.txt
+-- .gitignore
+-- .env.example
|
+-- config/
|   +-- base.yaml
|   +-- local.yaml
|   +-- hdfs.yaml
|   +-- aws.yaml
|   +-- snowflake.yaml
|   +-- datasets.yaml
|   +-- dq_rules.yaml
|   +-- kpi_rules.yaml
|
+-- src/
|   +-- common/
|   |   +-- config.py
|   |   +-- paths.py
|   |   +-- logging.py
|   |   +-- spark.py
|   |   +-- iceberg.py
|   |   +-- hashing.py
|   |   +-- audit.py
|   |   +-- exceptions.py
|   |
|   +-- contracts/
|   |   +-- schemas.py
|   |   +-- registry.py
|   |
|   +-- ingestion/
|   |   +-- manifest_reader.py
|   |   +-- file_validator.py
|   |   +-- bronze_writer.py
|   |   +-- landing_manager.py
|   |
|   +-- dq/
|   |   +-- structural.py
|   |   +-- business.py
|   |   +-- quarantine.py
|   |
|   +-- silver/
|   |   +-- merge.py
|   |   +-- scd1.py
|   |   +-- scd2.py
|   |   +-- transforms/
|   |
|   +-- cdc/
|   |   +-- wal_parser.py
|   |   +-- transaction_buffer.py
|   |   +-- event_deduplicator.py
|   |   +-- cdc_apply.py
|   |
|   +-- gold/
|   |   +-- dimensions.py
|   |   +-- facts.py
|   |   +-- kpis.py
|   |   +-- impacted_entities.py
|   |
|   +-- serving/
|       +-- snowflake.py
|       +-- mysql.py
|
+-- jobs/
|   +-- landing_to_bronze.py
|   +-- bronze_to_silver.py
|   +-- silver_to_gold.py
|   +-- gold_to_snowflake.py
|   +-- wal_to_silver.py
|   +-- kafka_to_bronze.py
|
+-- dags/
|   +-- initial_batch_pipeline.py
|   +-- incremental_batch_pipeline.py
|   +-- available_now_pipeline.py
|   +-- reconciliation_pipeline.py
|
+-- streamlit_app/
|   +-- app.py
|   +-- pages/
|   +-- components/
|   +-- services/
|
+-- ai/
|   +-- agents/
|   +-- tools/
|   +-- semantic/
|   +-- rag/
|   +-- charts/
|
+-- tests/
|   +-- unit/
|   +-- integration/
|   +-- dq/
|   +-- idempotency/
|   +-- reconciliation/
|
+-- scripts/
    +-- bootstrap_tables.py
    +-- validate_environment.py
```

The exact filenames may evolve, but the **separation of concerns should remain stable**.

---

# 5. Configuration and Path Strategy

No business logic should contain hardcoded full paths.

During early development, jobs may accept defaults through `argparse`, but Airflow will later explicitly pass arguments.

## Conceptual configuration

```text
environment      = local / hdfs / aws
base_path        = configured once
landing_path     = relative path
bronze_path      = relative path
silver_catalog   = configured
silver_database  = configured
gold_database    = configured
checkpoint_path  = configured
```

A path resolver combines the storage backend and logical dataset path.

Example environments:

```text
Local:
file:///data/capstone/...

HDFS:
hdfs:///capstone/...

AWS:
s3a://<bucket>/capstone/...
```

Changing storage must require configuration changes rather than transformation-code changes.

Credentials must come from environment variables, secret stores, Airflow connections, or platform-native authentication. They must never be committed to source control.

---

# 6. Source Batch Contract

The initial dump contains approximately 40 API batches. Each batch should remain independently identifiable and processable.

```text
landing_zone/
|
+-- landing/
|   +-- api-00001/
|   +-- api-00002/
|   +-- ...
|   +-- api-00040/
|
+-- processed/
|   +-- api-00001/
|   +-- ...
|
+-- failed/
```

The pipeline processes batches rather than manually concatenating the 40 exports.

## Manifest-driven ingestion

For each batch:

1. Read manifest.
2. Validate batch status.
3. Read `batch_id`.
4. Discover datasets from the manifest.
5. Validate expected files.
6. Validate file checksum where provided.
7. Validate expected row counts.
8. Validate expected columns/schema contract.
9. Process individual datasets.
10. Write ingestion/reconciliation records.
11. Move the source batch to `processed` only after the required processing succeeds.

The source manifest and internal reconciliation table have different responsibilities:

- **Manifest:** what the source claims it delivered.
- **Reconciliation:** what our platform actually processed.

---

# 7. Batch and Run Identity

Two independent IDs must be maintained.

## `batch_id`

Identifies the logical source batch.

Example:

```text
api-00017
```

## `run_id`

Identifies one execution attempt.

Example:

```text
batch_id = api-00017

run_id = run-1007 -> FAILED
run_id = run-1008 -> SUCCESS
```

This distinction is essential for retries and idempotency.

---

# 8. Landing to Bronze Design

## Landing responsibilities

Landing is temporary source-controlled staging.

Airflow should:

1. Detect/select an eligible batch.
2. Prevent concurrent duplicate processing of the same batch.
3. create a `run_id`.
4. Validate manifest and files.
5. Invoke PySpark ingestion.
6. Reconcile results.
7. Move successfully handled source data from `landing` to `processed`.

Moving a batch prevents normal file-sensor retriggering, but **idempotency must not depend solely on moving files**. The reconciliation/control table must also know that the source batch/file was already processed.

---

# 9. Bronze Zone Contract

Bronze preserves source-level data and ingestion metadata.

## Recommended Bronze metadata

Every Bronze row should carry appropriate metadata such as:

```text
_batch_id
_run_id
_source_system
_source_dataset
_source_file
_source_file_sha256
_ingested_at
_ingestion_date
_record_hash
_schema_version
```

WAL events additionally preserve:

```text
_event_id
_lsn
_transaction_id
_operation
_source_table
```

## Bronze storage

```text
bronze/
|
+-- good-data/
|   +-- <domain>/<dataset>/...
|
+-- bad-data/
|   +-- <domain>/<dataset>/...
|
+-- wal/
|
+-- control/
```

Partition by year/month/day/hour only for datasets where volume and query patterns justify it, particularly WAL/event/high-volume transactional data.

Small reference/master datasets should not be over-partitioned.

---

# 10. Landing -> Bronze Validation

This layer performs **structural/technical validation**, not full business validation.

Checks include:

- file readability
- JSON/JSONL parseability
- expected dataset identity
- expected schema/mandatory columns
- primitive datatype compatibility
- completely broken/malformed records
- manifest row count reconciliation
- checksum validation
- unknown/missing columns
- schema drift detection

## Record-level and file-level failures

Use two failure levels:

### File-level rejection

Used when the file itself is unusable, for example:

```text
corrupt file
invalid encoding
manifest mismatch severe enough to invalidate file
unsupported schema version
```

### Record-level quarantine

If a file is readable but contains some invalid rows:

```text
valid records   -> bronze/good-data
invalid records -> bronze/bad-data
```

Do not reject 100,000 valid records because 20 rows are malformed unless a contract specifically requires atomic file rejection.

---

# 11. Bronze Reconciliation Iceberg Table

Maintain one centralized reconciliation/control table instead of a separate audit table for every dataset.

Suggested logical fields:

```text
run_id
batch_id
source_system
source_dataset
source_file
source_file_sha256
source_strategy
source_operation
manifest_expected_rows
actual_input_rows
valid_records_count
invalid_records_count
bronze_written_count
processing_status
started_at
completed_at
error_code
error_message
```

Useful statuses:

```text
DISCOVERED
VALIDATING
PROCESSING
SUCCESS
FAILED
QUARANTINED
ROLLED_BACK
```

The table supports:

- reconciliation
- retries
- idempotency
- operational monitoring
- panel demonstrations
- root-cause investigation

---

# 12. Bronze Rollback Philosophy

Bronze should normally remain immutable.

Preferred rollback:

```text
bad batch
   |
   v
mark batch INVALID / ROLLED_BACK
   |
   v
exclude from downstream processing
```

Physical file deletion should be an exceptional administrative action, not the default logical rollback mechanism.

Silver rollback can use Iceberg snapshot capabilities where appropriate.

---

# 13. Bronze to Silver Data Quality

Bronze -> Silver performs **business/data-quality validation**.

Great Expectations can be integrated here for declarative checks and DQ reporting.

Typical rules:

```text
primary key must not be null
business key validity
valid numeric ranges
valid states
referential integrity
allowed currency/unit values
quantity rules
duplicate handling
timestamp sanity
relationship checks
```

Example product-centric rules:

```text
sales_order_line.product_id must resolve to a product
inventory_quant.product_id must resolve to a product
purchase_line.product_id must resolve to a product
reserved_quantity must satisfy agreed inventory rules
sales quantity must follow agreed return/cancellation semantics
```

DQ rules should be configuration-driven wherever reasonable.

## DQ output

Store:

```text
rule_id
rule_name
severity
source_dataset
record_key
batch_id
run_id
failed_value/failure context
failure_reason
created_at
```

Rules may have severities such as:

```text
ERROR   -> reject/quarantine
WARNING -> accept but flag
INFO    -> metric only
```

---

# 14. Silver Layer Design

Silver is stored as **Apache Iceberg tables**.

Silver responsibilities:

- typed data
- normalized IDs
- flattened Odoo relation fields where useful
- deduplication
- business DQ
- entity-level MERGE/UPSERT
- SCD handling where justified
- consistent timestamps
- trusted keys
- reusable conformed entities

Potential Silver tables required for the product use case:

```text
silver_products
silver_categories
silver_sales_orders
silver_sales_order_lines
silver_inventory_quants
silver_inventory_locations
silver_inventory_moves
silver_inventory_pickings
silver_purchase_orders
silver_purchase_order_lines
silver_supplier_links
```

Only required source entities should be promoted into the capstone's analytical scope.

---

# 15. Entity Processing Strategy Registry

Do not hardcode the same processing behavior for every source.

Maintain dataset configuration such as:

```text
dataset
source model
primary key
extraction strategy
processing strategy
SCD strategy
partition strategy
required schema
DQ rule set
```

Typical processing strategies:

```text
FULL_REFRESH
INCREMENTAL_UPSERT
APPEND_EVENT
CDC_MERGE
SCD1
SCD2
```

This allows full-refresh and write-date datasets to coexist cleanly.

---

# 16. SCD Strategy

Use SCD only where it serves an analytical requirement.

## SCD Type 1

Use when history is not analytically valuable and only the current trusted state is required.

## SCD Type 2

Use for selected dimension attributes whose historical state matters when interpreting facts.

Likely candidate:

```text
dim_product
```

Potential SCD2 fields:

```text
product_sk
product_id
product_name
category
active status
business attributes...
valid_from
valid_to
is_current
source_event_id
```

The exact tracked attributes must be frozen after profiling the latest product WAL/data.

Do not implement SCD2 on every table simply because WAL history is available.

---

# 17. WAL / CDC Processing Framework

The supplied WAL structure includes:

```text
lsn
transaction_id
event_id
table
operation
decoded_text
```

UPDATE events contain a previous representation (`old-key`) and a new representation (`new-tuple`). BEGIN/COMMIT events provide transaction boundaries.

## Required CDC library components

### WAL parser

Convert source text into normalized events:

```text
event_id
lsn
transaction_id
table
operation
primary_key
before
before_json
after
after_json
processed_at
```

### Transaction handling

Conceptually:

```text
BEGIN
  |
  +-- INSERT
  +-- UPDATE
  +-- DELETE
  |
COMMIT
  |
  v
transaction eligible for downstream application
```

Uncommitted/incomplete transactions must not be treated as fully committed business state.

### WAL idempotency

Use:

```text
event_id       -> event deduplication
lsn            -> ordering/progress
transaction_id -> transaction grouping
```

Replaying an already successfully applied event must not change the final Silver state.

---

# 18. Initial Batch First, WAL Second

Implementation order:

## Phase A

Load the 40 source API batches using batch processing and establish the initial Silver state.

## Phase B

Apply WAL changes incrementally on top of the established state.

Conceptually:

```text
40 API batches
      |
      v
initial/current Silver state
      |
      v
WAL committed changes
      |
      v
updated Silver state/history
```

The same normalized WAL processing logic should later accept Kafka-delivered WAL events.

---

# 19. Spark Execution Modes

Develop processing logic once and expose it through multiple execution modes.

## Mode 1: Normal batch

Start here.

```text
spark.read(...)
```

Use temporary HDFS test output during early validation where useful.

## Mode 2: File streaming with AvailableNow

After batch correctness is proven:

```text
readStream
   |
AvailableNow
   |
process currently available data
   |
checkpoint
   |
terminate
```

This integrates well with scheduled Airflow execution.

## Mode 3: Continuous streaming

Later:

```text
file stream / Kafka
       |
Structured Streaming
       |
micro-batches
       |
checkpoints
       |
common Bronze/Silver processing
```

Do not maintain entirely separate business-transformation code for batch and streaming.

---

# 20. Checkpoint Strategy

Checkpoints are state, not temporary files.

Each streaming query must receive a dedicated configured checkpoint location.

Conceptually:

```text
checkpoints/
+-- wal_to_bronze/
+-- bronze_sales_to_silver/
+-- bronze_inventory_to_silver/
```

Checkpoint locations must not be shared between unrelated streaming queries.

Deleting checkpoints should not be used as a normal retry mechanism.

---

# 21. End-to-End Idempotency Design

Idempotency is a formal project requirement.

## File ingestion

Identify source using combinations such as:

```text
batch_id
source dataset
source file
SHA-256
```

## Bronze

Prevent accidental logical duplication while preserving append-only auditability.

## Silver

Use deterministic primary/business keys and Iceberg MERGE logic.

## WAL

Use `event_id` as the main event identity and preserve `lsn`/transaction metadata.

## Gold

Use deterministic grain and MERGE affected rows/partitions rather than blind append.

## Snowflake

Use deterministic serving keys and MERGE/controlled replacement strategies.

## Test requirement

For every stage:

```text
process input X once
capture result
process exactly the same input X again
compare result
```

Expected business state must remain unchanged.

---

# 22. Gold Modelling Principle

Gold will follow a **star-schema-oriented dimensional model**.

The design sequence is:

```text
business question
    -> grain
    -> measures
    -> dimensions
    -> facts
    -> KPI marts
```

Never start by creating arbitrary fact/dimension table names.

---

# 23. Proposed Gold Star Schema

The final grain should be validated against the complete latest dataset, but this is the recommended baseline.

## Dimensions

### `dim_product`

Primary analytical dimension.

Potential attributes:

```text
product_sk
product_id
product_code
product_name
category_sk
uom
active
valid_from
valid_to
is_current
```

Use SCD2 only for selected attributes where historical accuracy matters.

### `dim_category`

```text
category_sk
category_id
category_name
parent_category
complete_name
```

### `dim_supplier`

Used for replenishment context.

```text
supplier_sk
supplier_id
supplier_name
```

### `dim_location`

```text
location_sk
location_id
location_name
location_type
```

### `dim_date`

```text
date_sk
full_date
day
month
month_name
quarter
year
week
```

---

# 24. Proposed Core Fact Tables

## A. `fact_product_sales_daily`

**Grain:** one row per product per business date, potentially company/currency if required.

Measures may include:

```text
units_sold
sales_revenue
order_count
discount_amount / supporting measures if available
```

This fact supports:

- Sales Velocity
- Revenue contribution
- ABC Classification
- dashboard trends/context

## B. `fact_product_inventory_snapshot`

**Grain:** product x location x snapshot/evaluation date.

For the current-state capstone, the snapshot can be materialized from trusted current quants at each Gold evaluation/run rather than requiring historical source snapshots.

Measures:

```text
on_hand_quantity
reserved_quantity
available_quantity
```

This supports Days of Stock Cover and Inventory Health.

## C. `fact_product_replenishment`

**Grain:** product x purchase/order-line or product x expected replenishment event, depending on final modelling.

Measures/context:

```text
ordered_quantity
received_quantity
outstanding_quantity
scheduled_receipt_date
actual_receipt_date
supplier_lead_time
purchase_status
```

This provides replenishment context for stockout risk.

---

# 25. KPI Mart

Create a business-serving table/mart at a deterministic grain such as:

```text
product x KPI evaluation date
```

Example:

```text
mart_product_inventory_health
```

Potential fields:

```text
as_of_date
product_sk
product_id
category_sk
revenue_analysis_window
units_sold_analysis_window
sales_velocity
revenue_contribution_pct
cumulative_revenue_pct
abc_class
on_hand_quantity
reserved_quantity
available_quantity
days_of_stock_cover
supplier_lead_time
outstanding_incoming_qty
inventory_health
recommended_action
calculation_version
```

KPI thresholds and analysis windows must be configuration-driven.

---

# 26. KPI Definitions

## KPI 1: Sales Velocity

```text
Sales Velocity
=
qualifying units sold in analysis window
/
number of days in analysis window
```

Sales status/return/cancellation semantics must be explicitly documented.

## KPI 2: Days of Stock Cover

```text
Available Stock
= On Hand - Reserved

Days of Stock Cover
= Available Stock / Sales Velocity
```

Zero Sales Velocity must be handled explicitly rather than allowing divide-by-zero or fake infinity values to drive logic.

## KPI 3: ABC Classification

```text
Product Revenue
-> sort descending
-> revenue contribution %
-> cumulative revenue %
-> configurable A/B/C thresholds
```

Because ABC is a portfolio-relative metric, a change to one product may affect classification boundaries for multiple products. Gold incremental logic must account for this.

## KPI 4: Inventory Health

Output classes:

```text
STOCKOUT_RISK
HEALTHY
OVERSTOCK
NON_MOVING
```

Decision logic combines:

```text
Sales Velocity
Available Stock
Days of Stock Cover
ABC priority
Supplier Lead Time / incoming replenishment context where applicable
```

Thresholds must live in configuration or governed business-rule tables, not be scattered through code.

---

# 27. Gold Incremental Processing

Do not rebuild everything when only a small set of products changed unless the KPI requires a global recalculation.

Identify impacted products from changes to:

```text
sales lines
inventory quants
inventory moves
purchase lines
supplier links
product attributes
```

Then:

```text
changed source rows
       |
       v
impacted product IDs
       |
       v
recalculate affected metrics
       |
       v
MERGE Gold rows
```

ABC classification is a special case because it depends on total portfolio revenue and cumulative contribution. Recompute the necessary ABC population for the relevant analysis period when revenue changes.

---

# 28. Snowflake Serving Layer

Snowflake is the primary analytical serving target.

Only trusted Gold business models should be published.

Potential serving schemas:

```text
ANALYTICS.DIMENSIONS
ANALYTICS.FACTS
ANALYTICS.MARTS
ANALYTICS.CONTROL
```

Loads should be idempotent using deterministic keys and MERGE/replacement patterns appropriate to each table.

MySQL may remain an optional alternative serving demonstration but should not duplicate architecture unnecessarily.

---

# 29. Streamlit Stakeholder Dashboards

Do not create one huge dashboard. Create stakeholder-specific views over common Gold data.

## A. Executive / Product Overview

Purpose: overall product portfolio health.

Show:

- total revenue
- active products
- A/B/C distribution
- inventory-health distribution
- revenue at stockout risk
- inventory exposure summary
- products requiring immediate attention

## B. Inventory Manager

Purpose: operational inventory action.

Show:

- Stockout Risk products
- Days of Stock Cover
- available/on-hand/reserved stock
- Non-Moving products
- Overstock products
- filters by category/location/ABC class
- prioritized action table

## C. Procurement Manager

Purpose: replenishment decisions.

Show:

- products requiring replenishment
- stock cover versus supplier lead time
- outstanding incoming quantities
- expected receipts
- supplier/product relationship
- A-class stockout-risk products first

## D. Product / Category Manager

Purpose: commercial product performance.

Show:

- Sales Velocity
- product/category revenue
- units sold
- ABC classification
- product ranking
- inventory health alongside commercial importance

All dashboards consume the same governed Gold data rather than implementing separate KPI formulas inside Streamlit.

---

# 30. Streamlit Application Architecture

Keep UI separate from data access.

```text
Streamlit Page
      |
      v
Service Layer
      |
      v
Repository / Query Layer
      |
      v
Snowflake
```

Avoid putting long SQL queries and core KPI calculations directly inside Streamlit page files.

Use caching carefully for expensive read-only analytical queries.

---

# 31. AI Analytics Layer

AI is introduced **after Gold and dashboards are trusted**.

The agent must not invent KPI formulas or directly reason over uncontrolled Bronze data.

```text
User Question
     |
     v
Analytics Agent
     |
 +---+--------------------+
 |           |            |
 v           v            v
SQL Tool   Knowledge    Chart Tool
           Retrieval
 |           |            |
 v           v            v
Gold Data  KPI Docs     Visualization
Snowflake  / Policies    Output
```

## Structured analytics

Questions such as:

```text
Show A-class products with stock cover below 5 days.
Which category has the most overstocked products?
Why is product X classified as Stockout Risk?
```

should use governed SQL/semantic tools over Gold.

## RAG

RAG should be used for unstructured knowledge such as:

```text
KPI definitions
business rules
inventory policy documents
data dictionaries
pipeline documentation
runbooks
```

RAG is supplementary, not a replacement for structured KPI querying.

## Dynamic chart generation

The AI layer may:

1. interpret user intent;
2. retrieve governed data;
3. choose an allowed chart type;
4. generate a validated chart specification;
5. render the chart in Streamlit.

The LLM should not fabricate chart data.

---

# 32. MCP / Tool Design

Potential tools:

```text
query_product_kpis
get_product_health
get_abc_products
get_stockout_risk
get_replenishment_context
explain_kpi_definition
create_chart
```

Prefer narrow, governed tools over an unrestricted agent with direct access to every database object.

The final backend may use Snowflake-native AI capabilities where permitted by the environment, while DuckDB can support local development/testing through the same logical repository/tool interface.

---

# 33. Airflow DAG Design

Keep orchestration logic thin.

Airflow should orchestrate jobs rather than contain transformation logic.

## Initial batch DAG

Conceptual tasks:

```text
find eligible batch
      |
      v
validate manifest
      |
      v
landing_to_bronze
      |
      v
validate reconciliation
      |
      v
bronze_to_silver
      |
      v
silver_to_gold
      |
      v
publish serving layer
      |
      v
mark success + move landing batch to processed
```

Failure at any stage should record status and preserve enough information for an idempotent retry.

## Parameters passed by Airflow

Examples:

```text
--batch-id
--input-path
--output-path
--environment
--dataset
--run-id
```

Jobs may initially define development defaults using `argparse`, but runtime orchestration ultimately passes explicit values.

---

# 34. Airflow Retry Strategy

Retries must be safe because jobs are idempotent.

Never design jobs where retry means:

```text
append everything again and hope to clean it later
```

Instead:

```text
identify batch/run
check existing processing state
re-enter safe stage
MERGE/deduplicate
reconcile output
```

Airflow retry count and backoff should be configuration-driven.

---

# 35. Kafka Integration Plan

Kafka is introduced only after file-based WAL processing works correctly.

The source WAL records can later become Kafka messages.

```text
PostgreSQL/Odoo WAL
       |
       v
Kafka Topic
       |
       v
Spark Structured Streaming
       |
       v
normalized CDC event
       |
       v
same CDC application logic
       |
       v
Silver / affected Gold
```

Kafka metadata should also be retained in Bronze:

```text
_topic
_partition
_offset
_kafka_timestamp
```

Kafka offsets/checkpoints aid recovery, while `event_id` remains the source event identity for downstream deduplication.

---

# 36. Schema Drift and Evolution Strategy

Classify schema changes.

## Compatible additions

Example:

```text
new nullable column
```

May be accepted according to contract policy and logged.

## Breaking changes

Examples:

```text
mandatory column removed
datatype incompatible
key changed
nested structure changed unexpectedly
```

These should fail/quarantine the affected dataset according to severity rather than silently corrupt Silver.

Track:

```text
schema_version
observed_schema_hash
contract_version
```

Never rely solely on automatic schema inference for production-style processing.

---

# 37. Reconciliation Strategy

Reconcile at every important boundary.

## Landing -> Bronze

```text
manifest rows
=
valid rows + invalid rows
```

subject to documented parser behavior.

## Bronze -> Silver

Track:

```text
input good records
DQ rejects
inserted
updated
duplicates ignored
output affected rows
```

## Silver -> Gold

Track:

```text
impacted products
facts recalculated
mart rows inserted/updated
```

## Gold -> Snowflake

Track source-versus-target row counts/checksums or aggregate controls appropriate to each table.

---

# 38. Observability

Every job should produce structured logs containing:

```text
run_id
batch_id
job_name
dataset
stage
record counts
duration
status
error code
```

Operational metrics should eventually include:

```text
batches succeeded/failed
DQ failure rate
rows processed
Silver insert/update count
Kafka lag where applicable
processing duration
Gold refresh latency
```

---

# 39. Failure Scenarios We Must Test

Before calling the pipeline complete, test at least:

```text
same batch processed twice
same file copied twice
Airflow task fails after Bronze write
Airflow retries task
malformed JSON record
entire broken file
missing required column
new unexpected column
null primary key
duplicate source ID
orphan product reference
WAL event replayed
WAL transaction missing COMMIT
WAL UPDATE replayed
out-of-order WAL input if possible
Silver MERGE rerun
Gold job rerun
Snowflake load rerun
checkpoint restart
```

Expected result: safe recovery without silently duplicating trusted business data.

---

# 40. Testing Strategy

## Unit Tests

Test reusable functions:

```text
path resolver
schema validators
hashing
WAL parser
before/after parsing
KPI formulas
ABC classification
inventory-health rule engine
```

## Integration Tests

Test boundaries:

```text
Landing -> Bronze
Bronze -> Silver
Silver -> Gold
Gold -> Snowflake
WAL -> Silver
```

## Idempotency Tests

Run identical input twice and compare trusted outputs.

## DQ Tests

Inject known bad records and verify correct quarantine/rejection.

## Reconciliation Tests

Ensure expected counts balance across layers.

---

# 41. Development Phases

## Phase 0: Freeze contracts and project skeleton

Deliverables:

- repository structure
- environment configuration
- dataset registry
- source schema contracts
- key definitions
- logging framework
- path resolver
- Spark session builder
- Iceberg configuration
- core test framework

**Exit criterion:** one command can load the correct environment/config without hardcoded business paths.

---

## Phase 1: One Dataset Landing -> Bronze

Start with one representative dataset, preferably a product or transactional dataset.

Build:

- manifest reader
- checksum validation
- schema validation
- Bronze good/bad split
- ingestion metadata
- reconciliation table
- landing -> processed movement

**Exit criterion:** rerunning the same batch does not create an incorrect duplicate trusted ingestion state.

---

## Phase 2: Generalize Landing -> Bronze

Extend the common ingestion framework to required product-centric source datasets.

Do **not** create one completely independent script per dataset.

**Exit criterion:** required datasets can be processed through configuration + shared library functions.

---

## Phase 3: Bronze -> Silver Batch

Build:

- Great Expectations integration
- business DQ
- type normalization
- relation flattening
- deduplication
- Iceberg tables
- MERGE/UPSERT
- SCD framework

**Exit criterion:** one initial batch produces trusted Silver entities with complete DQ/reconciliation results.

---

## Phase 4: Process All 40 Initial Batches

Run all supplied API batches through Airflow in a controlled sequence.

Validate:

- manifest reconciliation
- no unwanted duplicates
- correct current entity state
- correct full-refresh versus incremental behavior
- retry safety

**Exit criterion:** stable initial Silver state established.

---

## Phase 5: WAL / CDC File Processing

Build normalized WAL parser and transaction handler.

Support:

- BEGIN
- INSERT
- UPDATE
- DELETE when observed/available
- COMMIT

Implement:

- transaction grouping
- event deduplication
- LSN tracking
- Silver application
- SCD history where configured

**Exit criterion:** replaying the same committed WAL transaction produces no duplicate business changes.

---

## Phase 6: Gold Dimensional Model

Create:

- dimensions
- facts
- KPI mart
- deterministic keys
- impacted-product calculation
- Gold reconciliation

Validate all four KPIs against manually calculated samples.

**Exit criterion:** business users can answer the four core KPI questions entirely from Gold.

---

## Phase 7: Snowflake Serving

Create:

- target schemas/tables
- load strategy
- MERGE behavior
- reconciliation
- role/credential configuration

**Exit criterion:** rerunning the serving job does not duplicate Snowflake data.

---

## Phase 8: Streamlit Dashboards

Build stakeholder pages over the trusted Snowflake Gold model.

Prioritize:

1. Product/Executive Overview
2. Inventory Manager
3. Procurement Manager
4. Product/Category Manager

**Exit criterion:** dashboard numbers exactly reconcile to Gold SQL results.

---

## Phase 9: AvailableNow Streaming

Convert appropriate file flows to Spark Structured Streaming `AvailableNow` while reusing the established transformations.

Add:

- checkpoints
- restart testing
- micro-batch reconciliation

**Exit criterion:** AvailableNow produces the same business state as batch processing for equivalent input.

---

## Phase 10: Kafka Streaming

Feed WAL events through Kafka.

Reuse:

- WAL schema
- parser
- transaction semantics
- event_id deduplication
- Silver application logic

Add:

- topic/partition/offset metadata
- streaming checkpoints
- lag monitoring

**Exit criterion:** file-WAL and Kafka-WAL paths produce equivalent downstream state for equivalent events.

---

## Phase 11: AI Integration

Start only after Gold/dashboard trust is established.

Build in this order:

1. governed natural-language-to-analytics querying
2. KPI explanation tool
3. dynamic chart tool
4. documentation/KPI-definition RAG
5. MCP integration / tool exposure
6. Snowflake AI agent integration where permitted

**Exit criterion:** AI responses can be traced to governed data/tools and do not fabricate business numbers.

---

## Phase 12: Production-Style Hardening

Complete:

- failure testing
- rollback testing
- idempotency suite
- reconciliation dashboards
- logging/monitoring
- configuration documentation
- architecture documentation
- demo scripts
- final panel walkthrough

---

# 42. Implementation Priority

The project must resist the temptation to implement every trained technology immediately.

Use this order:

```text
Correctness
   |
   v
Idempotent Batch Pipeline
   |
   v
Trusted Silver
   |
   v
Correct Gold KPIs
   |
   v
Snowflake + Dashboard
   |
   v
WAL/CDC
   |
   v
AvailableNow
   |
   v
Kafka
   |
   v
AI / MCP / RAG
```

Some phases may overlap during development, but this dependency order should govern architectural decisions.

---

# 43. Definition of Done for Each Layer

## Landing is done when

- manifest is validated
- files are identifiable by batch
- same batch cannot be accidentally reprocessed as new
- processed/failed lifecycle works

## Bronze is done when

- ingestion is append/audit friendly
- source metadata is preserved
- schema issues are captured
- invalid records are quarantined
- reconciliation balances

## Silver is done when

- trusted schemas exist
- business DQ passes
- MERGE/UPSERT is idempotent
- SCD history works where configured
- WAL changes can be safely applied

## Gold is done when

- star schema grain is documented
- all 4 KPIs are reproducible
- thresholds are governed/configurable
- incremental recomputation is safe

## Serving is done when

- Snowflake reconciles with Gold
- Streamlit does not calculate independent business logic
- stakeholder views answer clearly different business questions

## Streaming is done when

- checkpoints recover correctly
- replay does not corrupt state
- equivalent batch and streaming inputs produce equivalent business outcomes

## AI is done when

- tools query governed data
- generated charts use retrieved data
- structured analytics and RAG responsibilities are separated
- AI answers are explainable and traceable

---

# 44. Final Architecture Checklist

Before final presentation, the project should demonstrate:

- [ ] Product-centric use case with four deep KPIs
- [ ] 40 initial batches processed incrementally
- [ ] Manifest-driven ingestion
- [ ] Landing -> processed lifecycle
- [ ] Bronze good/bad split
- [ ] Bronze reconciliation Iceberg table
- [ ] Schema drift handling
- [ ] Great Expectations/business DQ
- [ ] Silver Iceberg MERGE/UPSERT
- [ ] SCD implementation where justified
- [ ] WAL parser with BEGIN/COMMIT semantics
- [ ] event_id/LSN/transaction-aware CDC
- [ ] Gold dimensional/star model
- [ ] Incremental/affected Gold processing
- [ ] Snowflake serving
- [ ] Stakeholder-specific Streamlit dashboards
- [ ] HDFS/S3 configurable paths
- [ ] Airflow orchestration
- [ ] Batch idempotency
- [ ] Streaming checkpoint recovery
- [ ] Kafka integration
- [ ] Governed AI analytics
- [ ] MCP/tool design
- [ ] RAG only for appropriate unstructured knowledge
- [ ] Dynamic chart generation from governed query results
- [ ] Reconciliation and observability
- [ ] Failure/retry/rollback demonstration

---

# 45. First Concrete Implementation Milestone

Do **not** start by implementing the whole architecture.

The first milestone is:

```text
ONE SOURCE BATCH
      |
      v
manifest validation
      |
      v
Landing
      |
      v
PySpark
      |
 +----+----+
 |         |
 v         v
Bronze   Bronze
Good     Bad
 |         |
 +----+----+
      |
      v
Reconciliation Iceberg table
      |
      v
move source batch to Processed
      |
      v
RERUN SAME BATCH
      |
      v
NO INCORRECT DUPLICATION
```

Once this is proven, generalize it. Then build Silver. Then Gold. This keeps the architecture testable and prevents late redesign.

---

# Final Project Philosophy

> **Build one reusable data platform, not a collection of unrelated scripts.**
>
> Batch files, incremental API exports, WAL files, AvailableNow processing, and Kafka streams may enter differently, but they should converge on common contracts, quality rules, Silver entities, Gold business logic, serving models, and audit controls.
>
> Every new capability should extend the architecture rather than force a rewrite.
