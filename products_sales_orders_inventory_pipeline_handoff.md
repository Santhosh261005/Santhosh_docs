# Multi-Dataset Silver Pipeline Implementation Handoff

## Products, Sales Orders, Inventory Quants, and Inventory Pickings

## 1. Purpose

This document instructs the assigned implementation agent to extend the existing data-engineering project by building complete Landing-to-Bronze and Bronze-to-Silver pipelines for four datasets:

1. `products`
2. `sales_orders`
3. `inventory_quants`
4. `inventory_pickings`

The agent must first inspect and understand the existing project structure, reusable modules, schemas, and completed reference notebooks. The agent must then implement the new datasets by following the validated design patterns already used for `sales_order_lines`.

The assigned agent will also receive the source CSV files and their corresponding `manifest.json` files. The CSV and manifest files are the source of truth for file locations, fields, batch metadata, and record counts.

This task must not begin Gold-layer implementation. Gold facts, dimensions, SCD Type 2 product history, sales velocity, stock cover, ABC classification, and inventory-health KPIs will be designed separately after the required Silver tables are complete.

---

## 2. Important Architecture Direction

Use the existing project implementation as the architectural source of truth.

A previously created business blueprint may be used only to understand:

- business meanings;
- available source fields;
- operational timestamps;
- inventory and sales concepts;
- downstream analytical requirements.

Do not copy an older or conflicting architecture from that blueprint.

The current project architecture is:

```text
Source CSV + manifest
        |
        v
Landing
        |
        | Static Spark CSV processing
        | PERMISSIVE parser mode
        | Schema drift and corruption auditing
        v
Bronze HDFS CSV
        |
        | Spark Structured Streaming
        | foreachBatch + availableNow
        | Exploration + Great Expectations
        | Row-level validation
        | Deduplication
        | Iceberg MERGE
        v
Silver Iceberg current-state tables
        |
        v
Future Gold facts, dimensions, snapshots, and KPIs
```

---

## 3. Required Project Discovery Before Coding

Before editing or creating files, the agent must inspect the project recursively and document the relevant existing files.

At minimum, inspect:

```text
product_analysis/
├── data_profilers/
├── pipelines/
├── jobs/
├── schemas/
└── notebooks/
```

The agent must locate and study the completed order-lines implementation, including files whose names may resemble:

```text
sales_order_lines_landing_to_bronze.ipynb
bronze_to_silver_order_lines.ipynb
sales_order_lines_bronze_to_silver.ipynb
bronze_to_silver_common.py
sales_order_lines_silver.py
sales_order_lines_bronze_to_silver.py
schema_drift.py
exploration.py
great_expectations.py
```

The exact filenames in the repository may differ. The agent must discover the real names rather than assume paths.

### Required discovery output

Before making changes, the agent should record:

- current project tree;
- reusable Landing-to-Bronze entry points;
- reusable Bronze-to-Silver utilities;
- current argument-parser pattern;
- environment-variable pattern;
- logging pattern;
- Spark and Iceberg builder pattern;
- audit-table contract;
- rejected-record contract;
- checkpoint convention;
- staging-table MERGE pattern;
- notebook import pattern.

### Do not duplicate existing utilities

If a reusable utility already exists and is correct, import and reuse it. Do not create a second function with the same responsibility under a different name.

If a reusable utility needs a safe generic enhancement, modify it carefully and regression-test `sales_order_lines` before using the change for new datasets.

---

## 4. Required Deliverables

The agent must implement all four datasets.

### 4.1 Schema files

```text
schemas/products_schema.json
schemas/sales_orders_schema.json
schemas/inventory_quants_schema.json
schemas/inventory_pickings_schema.json
```

### 4.2 Landing-to-Bronze notebooks

```text
notebooks/products_landing_to_bronze.ipynb
notebooks/sales_orders_landing_to_bronze.ipynb
notebooks/inventory_quants_landing_to_bronze.ipynb
notebooks/inventory_pickings_landing_to_bronze.ipynb
```

### 4.3 Bronze-to-Silver notebooks

```text
notebooks/products_bronze_to_silver.ipynb
notebooks/sales_orders_bronze_to_silver.ipynb
notebooks/inventory_quants_bronze_to_silver.ipynb
notebooks/inventory_pickings_bronze_to_silver.ipynb
```

### 4.4 Dataset-specific reusable modules

Use the existing modularization conventions. Proposed names are:

```text
pipelines/products_silver.py
pipelines/sales_orders_silver.py
pipelines/inventory_quants_silver.py
pipelines/inventory_pickings_silver.py
```

If the project already uses a different module layout, follow the established pattern while preserving clear dataset separation.

### 4.5 Executable jobs

Create production-style jobs compatible with `spark-submit` and future Airflow orchestration:

```text
jobs/products_landing_to_bronze.py
jobs/products_bronze_to_silver.py
jobs/sales_orders_landing_to_bronze.py
jobs/sales_orders_bronze_to_silver.py
jobs/inventory_quants_landing_to_bronze.py
jobs/inventory_quants_bronze_to_silver.py
jobs/inventory_pickings_landing_to_bronze.py
jobs/inventory_pickings_bronze_to_silver.py
```

### 4.6 Package initialization

Create or update, as needed:

```text
pipelines/__init__.py
jobs/__init__.py
```

### 4.7 Documentation and validation evidence

The agent must provide:

- updated project tree;
- run instructions for every notebook;
- `spark-submit` examples for every job;
- environment-variable list;
- command-line argument list;
- source, good, rejected, duplicate, and target counts;
- checkpoint rerun evidence;
- schema and table descriptions;
- any assumptions or intentional deviations.

### No centralized dataset configuration file

Do not create or update a centralized `config/datasets.json` or similar shared dataset configuration file. Runtime configuration must use the existing argument parsers, environment variables, typed config objects, and module conventions.

---

## 5. Shared Implementation Standards

## 5.1 Argument parsing

Every executable job must use `argparse`.

Typical runtime arguments include:

```text
--dataset-name
--source-file or --landing-path
--bronze-path
--checkpoint-path
--silver-table
--rejected-table
--audit-table
--exploration-table
--expectation-table
--schema-file
--batch-id
--pipeline-run-id
--max-files-per-trigger
--trigger-type
--log-level
```

Use command-line arguments with environment-variable defaults.

Priority:

```text
explicit command-line argument
    -> environment-variable default
    -> safe code default
```

Jupyter notebooks may use a simulated `NOTEBOOK_ARGS` list or `parse_known_args()` because Jupyter injects kernel arguments. Production jobs must use normal `parse_args()`.

## 5.2 Environment variables

Use `os.getenv()` for environment/deployment settings such as:

```text
SPARK_MASTER
HDFS_DEFAULT_FS
ICEBERG_WAREHOUSE
ICEBERG_SPARK_RUNTIME_JAR
LOG_LEVEL
ENVIRONMENT
```

Do not hardcode secrets. Do not log credentials.

## 5.3 Logging

Use Python `logging`, not `print()`, in production modules and jobs.

Recommended levels:

- `DEBUG`: schemas, detailed configuration, query diagnostics;
- `INFO`: start/end, counts, writes, MERGE completion, reconciliation;
- `WARNING`: non-fatal profiler failures, unknown order state, empty batch;
- `ERROR`: required write or reconciliation failure;
- `logger.exception()`: inside exception handlers to include stack traces.

Important logs must include:

```text
dataset_name
pipeline_run_id
stream_batch_id
source_count
valid_count
rejected_count
duplicate_count
target_table
```

## 5.4 Table naming

Use these Silver and quarantine tables:

```text
datalake.silver.products
datalake.silver.sales_orders
datalake.silver.inventory_quants
datalake.silver.inventory_pickings

datalake.quarantine.products_rejected
datalake.quarantine.sales_orders_rejected
datalake.quarantine.inventory_quants_rejected
datalake.quarantine.inventory_pickings_rejected
```

Use the existing common operational audit table:

```text
datalake.operations.silver_ingestion_audit
```

Use the existing profiler tables:

```text
datalake.operations.explorations
datalake.operations.expectation_profiles
```

## 5.5 Checkpoints

Each Bronze-to-Silver stream must have its own checkpoint:

```text
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/products
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/sales_orders
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/inventory_quants
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/inventory_pickings
```

Never share checkpoints across datasets.

Use a fully qualified HDFS authority for checkpoints. The existing environment uses:

```text
hdfs://localhost:9000
```

## 5.6 Structured Streaming

Bronze-to-Silver must use:

```python
.trigger(availableNow=True)
```

Use:

```python
.option("maxFilesPerTrigger", configurable_value)
```

The stream must process Bronze CSV files using an explicit schema. Do not infer schema in streaming reads.

Inside `foreachBatch`:

```text
cache static batch
materialize batch
data exploration
Great Expectations
transform and validate
split valid/rejected
write rejected
deduplicate
stage and MERGE Silver
reconcile
write audit
unpersist in finally
```

Do not repeat corrupt-record profiling in Bronze-to-Silver. Parser corruption is a Landing-to-Bronze responsibility.

## 5.7 Great Expectations behavior

Great Expectations is for batch-level monitoring and audit evidence.

A failed expectation must not automatically stop valid records from reaching Silver.

Equivalent Spark rules must perform row-level splitting:

```text
GX failure
-> log to expectation audit
-> continue pipeline

Spark row validation failure
-> write record to dataset-specific quarantine table
```

Infrastructure failures in required transformations, quarantine writes, MERGE, or reconciliation must fail the micro-batch and be re-raised so the checkpoint does not advance.

## 5.8 Odoo relation handling

Apply this rule consistently.

### Bronze

Retain raw relation strings exactly as parsed from CSV, for example:

```text
[4, "Deliveries"]
[1, "Units"]
```

### Accepted Silver record

Extract:

```text
numeric relation ID
display name
```

Drop the raw relation string from the accepted Silver table.

### Rejected record

Retain the raw relation string and any successfully parsed relation values.

### Optional false relationship values

Some optional relation columns contain:

```text
False
```

This represents an absent optional relation. It is not malformed data.

Handle all case variations safely if encountered:

```text
False
false
FALSE
null
empty string
```

Required relations may reject missing values. Optional relations must become null parsed IDs/names without rejection.

## 5.9 Current-state upserts

For all four Silver tables:

- MERGE using the dataset-specific primary key;
- latest `write_date` wins;
- for equal `write_date`, use deterministic tie-breakers:
  1. `source_batch_id` descending;
  2. `source_file` descending.

Equal or older source versions must not replace a newer target version.

Use the existing deterministic Iceberg staging-table MERGE design. Do not revert to a temporary-view MERGE source if the source plan contains non-deterministic expressions such as `current_timestamp()`.

## 5.10 Bronze partitioning

Good Bronze records must be written as CSV and partitioned by a derived date from `write_date`:

```text
write_date_partition = DATE(write_date)
```

Apply this to:

```text
products
sales_orders
inventory_quants
inventory_pickings
```

Retain the original `write_date` field unchanged.

Keep `batch_id` in the Bronze path for lineage.

Example:

```text
hdfs://localhost:9000/capstone/warehouse/bronze/products/
  batch_id=<batch-id>/
  file_00000/
  write_date_partition=2026-10-07/
  part-....csv
```

Use the source field format for Landing input timestamps and the actual Spark-written Bronze timestamp format when reading Bronze. The existing order-lines implementation discovered that Spark Bronze CSV timestamps may be stored as:

```text
2026-10-07T17:50:27.000Z
```

Therefore, validate actual Bronze output and use the matching reader timestamp format, commonly:

```text
yyyy-MM-dd'T'HH:mm:ss.SSSX
```

## 5.11 Source lineage

Every accepted and rejected record must support lineage through fields such as:

```text
source_batch_id
source_file
stream_batch_id
pipeline_run_id
created/updated/rejected processing timestamp
```

Use `input_file_name()` and derive `batch_id` from the physical Bronze path because recursive lookup may disable automatic partition-column discovery.

---

## 6. Landing-to-Bronze Requirements

Each dataset must receive a dedicated Landing-to-Bronze notebook and executable job.

Use the completed sales-order-lines Landing-to-Bronze implementation and reusable `schema_drift.py` as the reference.

Each Landing-to-Bronze pipeline must:

1. read the source CSV and its manifest;
2. validate file existence and expected source metadata;
3. load the explicit expected schema from JSON;
4. read using Spark CSV `PERMISSIVE` mode;
5. use the configured corrupt-record column;
6. detect and audit schema drift;
7. split parser-accepted and parser-rejected records;
8. write accepted records to HDFS Bronze as CSV;
9. partition accepted records by `DATE(write_date)`;
10. write corrupt raw records to the established Iceberg audit design;
11. perform source/good/bad/persisted-count reconciliation;
12. log all important operations;
13. use arguments and environment-variable defaults;
14. support `spark-submit` execution;
15. preserve relation columns as raw strings in Bronze.

### Manifest usage

The agent must inspect each supplied manifest and use available metadata such as:

```text
batch identifier
source entity
file path
record count
extraction timestamp
source metadata
```

Do not invent manifest fields. If a manifest lacks a desired field, log the limitation and use established runtime arguments.

### Landing-to-Bronze rejection scope

Only parser-level corruption belongs here. Business null checks, domain checks, relation parsing, and current-state upserts belong in Bronze-to-Silver.

---

## 7. Dataset 1: Products

## 7.1 Source fields

Source CSV header:

```text
id,default_code,name,categ_id,uom_id,list_price,create_date,write_date,active,qty_available,virtual_available,product_tmpl_id
```

## 7.2 Expected source schema

Create `schemas/products_schema.json` with the logical source types:

```json
{
  "id": "long",
  "default_code": "string",
  "name": "string",
  "categ_id": "string",
  "uom_id": "string",
  "list_price": "double",
  "create_date": "timestamp",
  "write_date": "timestamp",
  "active": "boolean",
  "qty_available": "double",
  "virtual_available": "double",
  "product_tmpl_id": "string"
}
```

If the manifest or full CSV reveals a justified type conflict, document it before changing this contract.

## 7.3 Silver grain and key

Silver table:

```text
datalake.silver.products
```

Grain:

```text
One current row per product ID
```

Primary key:

```text
product_id = source id
```

Deduplication and MERGE:

```text
partition by product_id
order by write_date descending,
         source_batch_id descending,
         source_file descending
```

Update target only when incoming `write_date` is newer.

## 7.4 Relation parsing

Parse:

```text
categ_id
-> category_id BIGINT
-> category_name STRING

uom_id
-> uom_id BIGINT
-> uom_name STRING

product_tmpl_id
-> product_template_id BIGINT
-> product_template_name STRING
```

Do not carry raw relation strings into accepted Silver records.

Retain raw relation strings in rejected records.

## 7.5 Proposed Silver columns

```text
product_id
product_code
product_name
category_id
category_name
uom_id
uom_name
list_price
active
qty_available
virtual_available
product_template_id
product_template_name
create_date
write_date
source_batch_id
source_file
stream_batch_id
pipeline_run_id
silver_created_at
silver_updated_at
```

## 7.6 Product validation rules

Mandatory row-level rules:

```text
id must not be null
name must not be null or blank
categ_id must not be null and must parse successfully
uom_id must not be null and must parse successfully
product_tmpl_id must not be null and must parse successfully
list_price must not be null
list_price must be >= 0
active must not be null
qty_available must not be null
virtual_available must not be null
create_date must not be null
write_date must not be null
write_date must be >= create_date when both are present
```

`default_code` may be nullable if the source permits products without an internal reference. Do not reject solely because `default_code` is absent unless the manifest/business rules explicitly require it.

Negative `qty_available` or `virtual_available` should not automatically be rejected. Inventory systems can contain negative balances. Preserve and profile them; Gold can classify or review them later.

Inactive products are valid:

```text
active = false
```

Do not reject or delete inactive products.

## 7.7 Product Great Expectations

At minimum define expectations equivalent to the row-level rules for:

```text
id not null
name not null
category relation not null
UOM relation not null
product template relation not null
list_price not null and >= 0
active not null
qty_available not null
virtual_available not null
create_date not null
write_date not null
```

GX failures must be audited without blocking valid rows.

## 7.8 Product inventory interpretation

For future Gold KPIs:

```text
qty_available
```

is the authoritative usable-stock quantity.

Preserve:

```text
virtual_available
```

as a projected inventory measure, but do not treat it as current sellable stock.

`list_price` is a selling/reference price, not cost.

Future retail-value naming must be:

```text
inventory_retail_value
=
qty_available * list_price
```

Do not name it inventory cost value.

## 7.9 Future Gold SCD Type 2 requirement

Silver remains current-state:

```text
datalake.silver.products
```

Future Gold will create:

```text
datalake.gold.dim_product
```

as SCD Type 2.

Tracked attributes are:

```text
product_code
a product name
category_id
category_name
uom_id
uom_name
list_price
active
```

Do not treat these measures as SCD2 descriptive attributes:

```text
qty_available
virtual_available
```

The products Silver design must preserve enough source and write-date lineage for future SCD2 processing.

---

## 8. Dataset 2: Sales Orders

## 8.1 Source fields

Source CSV header:

```text
id,name,partner_id,date_order,state,amount_untaxed,amount_total,currency_id,create_date,write_date,company_id,user_id,team_id,client_order_ref,ops_order_at,ops_confirmed_at,ops_unmet_demand,ops_closed
```

## 8.2 Expected source schema

Create `schemas/sales_orders_schema.json`:

```json
{
  "id": "long",
  "name": "string",
  "partner_id": "string",
  "date_order": "timestamp",
  "state": "string",
  "amount_untaxed": "double",
  "amount_total": "double",
  "currency_id": "string",
  "create_date": "timestamp",
  "write_date": "timestamp",
  "company_id": "string",
  "user_id": "string",
  "team_id": "string",
  "client_order_ref": "string",
  "ops_order_at": "timestamp",
  "ops_confirmed_at": "timestamp",
  "ops_unmet_demand": "boolean",
  "ops_closed": "boolean"
}
```

## 8.3 Silver grain and key

Silver table:

```text
datalake.silver.sales_orders
```

Grain:

```text
One current row per sales order ID
```

Primary key:

```text
sales_order_id = source id
```

Deduplication and MERGE:

```text
partition by sales_order_id
order by write_date descending,
         source_batch_id descending,
         source_file descending
```

## 8.4 Relation parsing

Parse:

```text
partner_id
-> customer_id
-> customer_name

currency_id
-> currency_id
-> currency_name

company_id
-> company_id
-> company_name

user_id
-> salesperson_id
-> salesperson_name

team_id
-> sales_team_id
-> sales_team_name
```

These are required in the sample except where the full CSV demonstrates optionality. The agent must inspect null distributions before finalizing required versus optional relation checks.

## 8.5 Proposed Silver columns

```text
sales_order_id
sales_order_name
customer_id
customer_name
date_order
order_state
standardized_order_state
amount_untaxed
amount_total
currency_id
currency_name
company_id
company_name
salesperson_id
salesperson_name
sales_team_id
sales_team_name
client_order_ref
ops_order_at
ops_confirmed_at
ops_unmet_demand
ops_closed
create_date
write_date
source_batch_id
source_file
stream_batch_id
pipeline_run_id
silver_created_at
silver_updated_at
```

## 8.6 Sales-order state policy

Do not reject a non-null state solely because it is unfamiliar.

Required behavior:

```text
known non-empty state
-> retain raw state
-> map to standardized known value

unknown non-empty state
-> retain raw state
-> standardized_order_state = OTHER
-> log warning / audit finding
-> do not reject

null or blank state
-> reject
```

Inspect the complete file to identify the actual domain. Likely known states may include:

```text
draft
sent
sale
done
cancel
```

Do not assume this list is complete.

Do not filter draft or cancelled orders out of Silver. Gold will later define sales eligibility.

## 8.7 Sales-order timestamps

Business timestamps:

```text
date_order
ops_order_at
ops_confirmed_at
```

Technical/source lifecycle timestamps:

```text
create_date
write_date
```

Do not substitute `write_date` for the order business date in later Gold KPIs.

## 8.8 Sales-order validation rules

Mandatory rules:

```text
id must not be null
name must not be null or blank
partner_id must not be null and must parse successfully
date_order must not be null
state must not be null or blank
amount_untaxed must not be null
amount_total must not be null
currency_id must not be null and must parse successfully
company_id must not be null and must parse successfully
user_id must not be null and must parse successfully unless full-file profiling proves optionality
team_id must parse successfully when present
ops_order_at must not be null
ops_unmet_demand must not be null
ops_closed must not be null
create_date must not be null
write_date must not be null
write_date must be >= create_date when both are present
amount_total should be >= amount_untaxed when both are present, unless full data shows legitimate exceptions
```

`client_order_ref` may be nullable if permitted by the full dataset.

`ops_confirmed_at` may be nullable for unconfirmed/draft/cancelled states. Validate conditionally rather than globally rejecting nulls.

Do not collapse these distinct business fields:

```text
state
ops_unmet_demand
ops_closed
```

## 8.9 Sales-order Great Expectations

Create batch-level expectations matching core validation rules, including:

```text
ID and order name not null
customer relation not null
order date not null
state not null
monetary amounts not null
currency relation not null
company relation not null
ops_order_at not null
ops_unmet_demand not null
ops_closed not null
create_date not null
write_date not null
```

Unknown but non-empty states should create a warning/audit measure, not a row rejection.

---

## 9. Dataset 3: Inventory Quants

## 9.1 Dataset meaning

`inventory_quants.csv` represents current inventory balances by stock location. It is not a picking-event table and must not be combined with inventory pickings.

## 9.2 Source fields

Source CSV header:

```text
id,product_id,location_id,quantity,reserved_quantity,write_date,company_id
```

## 9.3 Expected source schema

Create `schemas/inventory_quants_schema.json`:

```json
{
  "id": "long",
  "product_id": "string",
  "location_id": "string",
  "quantity": "double",
  "reserved_quantity": "double",
  "write_date": "timestamp",
  "company_id": "string"
}
```

## 9.4 Silver grain and key

Silver table:

```text
datalake.silver.inventory_quants
```

Source-level grain:

```text
One current quant row per source quant ID
```

Primary Silver key:

```text
quant_id = source id
```

Business dimensions associated with a quant:

```text
product_id + location_id + company_id
```

Do not make the MERGE key solely product and location unless full-file profiling proves source IDs are unstable. The source `id` is the safest current-state record key.

Deduplication and MERGE:

```text
partition by quant_id
order by write_date descending,
         source_batch_id descending,
         source_file descending
```

## 9.5 Relation parsing

Parse:

```text
product_id
-> product_id
-> product_name

location_id
-> location_id
-> location_name

company_id
-> company_id
-> company_name
```

`company_id=False` is a valid absent optional relationship. It must produce null `company_id` and `company_name`, not a rejection.

## 9.6 Proposed Silver columns

```text
quant_id
product_id
product_name
location_id
location_name
quantity
reserved_quantity
available_quantity
company_id
company_name
write_date
source_batch_id
source_file
stream_batch_id
pipeline_run_id
silver_created_at
silver_updated_at
```

Derived measure:

```text
available_quantity
=
quantity - reserved_quantity
```

This derived measure must be retained for location-level analysis, but it is not the authoritative product-level stock-cover numerator.

## 9.7 Inventory-quant validation rules

Mandatory rules:

```text
id must not be null
product_id must not be null and must parse successfully
location_id must not be null and must parse successfully
quantity must not be null
reserved_quantity must not be null
write_date must not be null
company_id must parse successfully when present and not False
```

Do not reject negative `quantity` or negative derived `available_quantity` solely because values are negative. External/vendor/transit locations may legitimately produce negative balances.

Do not reject because:

```text
reserved_quantity > quantity
```

Instead, preserve, profile, and audit such cases because location semantics may explain them.

## 9.8 Inventory-location semantics

The sample includes locations such as:

```text
Vendors
WH/Input
```

Do not sum every inventory quant location and treat the total as sellable stock.

The current approved stock-cover source is:

```text
products.qty_available
```

Use inventory quants for:

- detailed location analysis;
- warehouse-flow validation;
- future inventory snapshot aggregation after location classifications become available.

## 9.9 Future Gold inventory snapshot

Silver keeps the latest quant state.

Future Gold will create a historical daily snapshot table, for example:

```text
datalake.gold.fact_product_inventory_snapshot
```

Expected future grain:

```text
snapshot_date + product_id
```

For the first Gold stock-cover KPI, use `products.qty_available` as approved usable stock.

---

## 10. Dataset 4: Inventory Pickings

## 10.1 Dataset meaning

`inventory_pickings.csv` represents inventory receipt, transfer, and delivery lifecycle activity. It is separate from current quantity balances.

## 10.2 Source fields

Source CSV header:

```text
id,name,partner_id,state,origin,location_id,location_dest_id,company_id,scheduled_date,date_done,create_date,write_date,picking_type_id,sale_id,purchase_id,ops_warehouse_stage,ops_handling_seconds
```

## 10.3 Expected source schema

Create `schemas/inventory_pickings_schema.json`:

```json
{
  "id": "long",
  "name": "string",
  "partner_id": "string",
  "state": "string",
  "origin": "string",
  "location_id": "string",
  "location_dest_id": "string",
  "company_id": "string",
  "scheduled_date": "timestamp",
  "date_done": "timestamp",
  "create_date": "timestamp",
  "write_date": "timestamp",
  "picking_type_id": "string",
  "sale_id": "string",
  "purchase_id": "string",
  "ops_warehouse_stage": "string",
  "ops_handling_seconds": "long"
}
```

If the complete CSV contains fractional handling seconds, use `double`; otherwise preserve `long`.

## 10.4 Silver grain and key

Silver table:

```text
datalake.silver.inventory_pickings
```

Grain:

```text
One current row per picking ID
```

Primary key:

```text
picking_id = source id
```

Deduplication and MERGE:

```text
partition by picking_id
order by write_date descending,
         source_batch_id descending,
         source_file descending
```

## 10.5 Relation parsing

Parse:

```text
partner_id
-> partner_id
-> partner_name

location_id
-> source_location_id
-> source_location_name

location_dest_id
-> destination_location_id
-> destination_location_name

company_id
-> company_id
-> company_name

picking_type_id
-> picking_type_id
-> picking_type_name

sale_id
-> sales_order_id
-> sales_order_name

purchase_id
-> purchase_order_id
-> purchase_order_name
```

Optional relationships:

```text
sale_id
purchase_id
company_id when source is False
```

`False` in optional relation columns must produce null parsed values, not rejection.

## 10.6 Proposed Silver columns

```text
picking_id
picking_name
partner_id
partner_name
state
standardized_state
origin
source_location_id
source_location_name
destination_location_id
destination_location_name
company_id
company_name
scheduled_date
date_done
create_date
write_date
picking_type_id
picking_type_name
sales_order_id
sales_order_name
purchase_order_id
purchase_order_name
ops_warehouse_stage
ops_handling_seconds
source_batch_id
source_file
stream_batch_id
pipeline_run_id
silver_created_at
silver_updated_at
```

## 10.7 Inventory-picking validation rules

Mandatory rules:

```text
id must not be null
name must not be null or blank
state must not be null or blank
location_id must not be null and must parse successfully
location_dest_id must not be null and must parse successfully
scheduled_date must not be null
create_date must not be null
write_date must not be null
picking_type_id must not be null and must parse successfully
ops_warehouse_stage must not be null or blank
ops_handling_seconds must not be null
ops_handling_seconds must be >= 0
write_date must be >= create_date when both are present
```

Conditional rules:

```text
if state indicates done/completed,
date_done should not be null

if date_done is present,
it should generally be >= scheduled_date only if business semantics require it;
do not reject early completions without confirming the requirement
```

Optional relation rules:

```text
partner_id may be nullable depending on picking type
sale_id may be False/null
purchase_id may be False/null
company_id may be False/null if the source uses a shared/external location row
```

## 10.8 State policy

As with sales orders:

- preserve raw non-empty state;
- map known values to standardized values;
- map unknown non-empty values to `OTHER`;
- log a warning/audit finding;
- reject only null/blank mandatory state.

Do not filter non-done pickings from Silver. Gold operational facts may require scheduled, assigned, done, cancelled, and waiting states.

## 10.9 Business timestamps

Use these for future operational analysis:

```text
scheduled_date
date_done
```

Use these for CDC/current-state selection and technical lineage:

```text
create_date
write_date
```

Do not use `write_date` as a substitute for warehouse completion time.

---

## 11. Iceberg Table Creation Requirements

Every dataset-specific module must provide an idempotent table-creation function.

Use:

```sql
CREATE NAMESPACE IF NOT EXISTS
CREATE TABLE IF NOT EXISTS
```

Do not replace or drop current tables during normal execution.

Use Iceberg format version 2.

Suggested Silver partitioning is based on business query patterns and data scale. For the current small synthetic data, avoid unnecessary high-cardinality partitioning.

Recommended current-state Silver table design:

```text
products:
unpartitioned or bucketed only if later justified

sales_orders:
PARTITIONED BY months(date_order)

inventory_quants:
unpartitioned initially

inventory_pickings:
PARTITIONED BY months(scheduled_date)
```

However, inspect and follow the existing project convention. If current-state MERGE behavior becomes simpler and more reliable with unpartitioned Silver tables at this scale, unpartitioned is acceptable. Document the choice.

Do not partition any table directly by high-cardinality IDs such as product ID, order ID, quant ID, or picking ID.

---

## 12. Rejected-Record Contracts

Every dataset needs a rejected-record schema containing:

- all original Bronze business fields;
- raw relation strings;
- any successfully parsed relation IDs and names;
- `failed_rule_names ARRAY<STRING>`;
- `rejection_reasons ARRAY<STRING>`;
- source batch and file lineage;
- stream batch ID;
- pipeline run ID;
- rejected timestamp.

Rejected records must be written to the dataset-specific Iceberg quarantine table.

The existing append-based retry limitation must be acknowledged. If a micro-batch writes rejected rows and later fails before checkpoint commit, a retry can duplicate rejected records.

Prefer the existing project strategy. Do not redesign retry idempotency silently. If improving rejected-write idempotency, provide regression evidence and preserve order-lines behavior.

---

## 13. Exploration Requirements

Run data exploration for each static `foreachBatch` DataFrame before transformations.

Use:

```text
processing_stage = BRONZE
profile_scope = BATCH
```

Profile all business fields and mark required columns appropriately.

At minimum capture:

```text
row count
null count
non-null count
distinct count
min/max for numeric and timestamp columns
```

Use the existing exploration module and table. Do not implement a new exploration framework.

---

## 14. Audit and Reconciliation Requirements

Every micro-batch must write an audit record to:

```text
datalake.operations.silver_ingestion_audit
```

Required reconciliation:

```text
source_count
=
valid_before_dedup_count + rejected_count
```

```text
valid_before_dedup_count
=
valid_after_dedup_count + duplicate_count
```

```text
valid_after_dedup_count
=
incoming_keys_found_in_silver
```

Also verify target version satisfaction:

```text
target.write_date >= incoming.write_date
```

for every deduplicated incoming key after MERGE.

A successful pipeline may contain rejected records. `SUCCESS` means all source records were deterministically accepted or quarantined and reconciliation passed.

---

## 15. Deterministic Iceberg MERGE Requirements

Use the existing staging-table pattern from the validated order-lines implementation.

Required sequence:

1. create a unique staging-table name from dataset, pipeline run ID, and stream batch ID;
2. drop any leftover retry table with that exact name;
3. validate no null merge keys remain;
4. validate only one incoming row per merge key;
5. materialize the deduplicated DataFrame into the staging Iceberg table;
6. reconcile staging count;
7. MERGE the staging table into Silver;
8. update only when incoming `write_date` is newer;
9. insert absent keys;
10. preserve target `silver_created_at` during updates;
11. update `silver_updated_at` for updated rows;
12. drop the staging table in `finally`;
13. refresh target table metadata;
14. verify incoming keys and versions.

This staging materialization is required because the previous order-lines implementation encountered an Iceberg analysis failure when a non-deterministic source expression such as `current_timestamp()` remained in the MERGE logical plan.

---

## 16. Notebook Requirements

Every notebook should be thin and readable. It should import reusable production modules rather than redefine complete pipelines.

Recommended notebook sequence:

```text
1. Project path and imports
2. Simulated notebook arguments
3. Logging configuration
4. Typed runtime configuration
5. Spark/Iceberg session
6. Load and display schema
7. Verify source or Bronze path
8. Create required namespaces/tables
9. Import dataset pipeline functions
10. Optional static transformation validation
11. Create stream
12. Start availableNow query
13. Validate Silver, rejected, audit, exploration, and GX results
14. Rerun with same checkpoint and verify zero new rows
```

Do not leave large copied function definitions in notebooks after modules are created.

---

## 17. Executable Job Requirements

Each job must:

- define `main()`;
- parse arguments;
- configure logging;
- construct typed configuration;
- create Spark session;
- load explicit schema;
- create required tables;
- construct dataset callback;
- start the stream or batch ingestion;
- await completion;
- inspect query exception;
- log success/failure;
- stop Spark in `finally`.

Use:

```python
if __name__ == "__main__":
    main()
```

Importing a job or pipeline module must not automatically start Spark or execute the pipeline.

---

## 18. `spark-submit` Examples

The agent must include exact commands adapted to the project paths.

Example Bronze-to-Silver pattern:

```bash
spark-submit \
  /path/to/product_analysis/jobs/products_bronze_to_silver.py \
  --dataset-name products \
  --bronze-path hdfs://localhost:9000/capstone/warehouse/bronze/products \
  --checkpoint-path hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/products \
  --silver-table datalake.silver.products \
  --rejected-table datalake.quarantine.products_rejected \
  --audit-table datalake.operations.silver_ingestion_audit \
  --schema-file /path/to/product_analysis/schemas/products_schema.json \
  --trigger-type availableNow \
  --max-files-per-trigger 5 \
  --log-level INFO
```

Provide equivalent commands for all datasets and both stages.

---

## 19. Airflow Compatibility

Do not build the Airflow DAG in this task unless explicitly requested later.

Jobs must nevertheless be ready for Airflow to invoke through `spark-submit`.

Future flow:

```text
Airflow DAG params
-> BashOperator or Spark submit operator
-> command-line arguments
-> argparse
-> typed pipeline configuration
```

`pipeline_run_id` must accept an Airflow run ID later.

Environment-level values should be passable through an operator environment mapping.

---

## 20. Required Testing

## 20.1 Schema tests

For every dataset:

- compare CSV header with schema JSON;
- validate required columns;
- validate source timestamp parsing;
- validate Bronze timestamp parsing;
- ensure framework columns are not written into business CSV output.

## 20.2 Landing-to-Bronze tests

Verify:

```text
source_count = good_count + bad_count
written_good_count = good_count
written_bad_count = bad_count
```

Verify Bronze partition output and audit records.

## 20.3 Static Silver transformation tests

Before starting each stream:

- read Bronze statically;
- add source lineage;
- parse every relation;
- apply validation;
- reconcile valid and rejected counts;
- inspect sample accepted and rejected rows;
- validate exact target column order.

## 20.4 Deduplication tests

Verify:

```text
valid_before_dedup
=
valid_after_dedup + duplicate_count
```

Verify no duplicate primary keys remain.

## 20.5 MERGE tests

Test:

- new key inserts;
- newer `write_date` updates;
- equal `write_date` does not unnecessarily update;
- older `write_date` does not replace newer target state;
- `silver_created_at` remains unchanged on update;
- staging table is removed after success and failure.

## 20.6 Checkpoint idempotency tests

Run each stream again with the same checkpoint and no new files.

Verify:

```text
input rows processed = 0
Silver count unchanged
rejected count unchanged
audit count unchanged
```

## 20.7 Relation tests

Test:

- normal Odoo array relation;
- missing required relation;
- malformed relation;
- optional `False` relation;
- empty string;
- null.

## 20.8 State tests

For sales orders and pickings:

- known state preserved and standardized;
- unknown non-empty state mapped to `OTHER` without row rejection;
- null/blank mandatory state rejected.

---

## 21. Dataset-Specific Acceptance Criteria

## 21.1 Products

- one Silver row per product ID;
- latest `write_date` wins;
- category, UOM, and template relations parsed;
- inactive products retained;
- usable stock preserved as `qty_available`;
- projected stock preserved as `virtual_available`;
- no false interpretation of `list_price` as cost;
- future Gold SCD2 tracked attributes documented.

## 21.2 Sales orders

- one Silver row per sales order ID;
- latest `write_date` wins;
- all relations parsed;
- all valid states retained;
- unknown non-empty states mapped to `OTHER`;
- business timestamps distinguished from technical timestamps;
- unmet demand and closed flags preserved independently.

## 21.3 Inventory quants

- one Silver row per current quant ID;
- product and location relations parsed;
- optional `company_id=False` accepted;
- negative quantities retained and profiled;
- location-level `available_quantity` derived;
- no indiscriminate use of quant totals as usable product stock.

## 21.4 Inventory pickings

- one Silver row per picking ID;
- latest `write_date` wins;
- all required and optional relations handled correctly;
- `sale_id=False` and `purchase_id=False` accepted;
- scheduled and completion timestamps preserved;
- warehouse stage and handling seconds validated;
- unknown non-empty states retained as `OTHER`.

---

## 22. Cross-Dataset Validation

After all four Silver tables exist, perform referential-quality reports. These should normally be warnings/audits, not destructive row filtering.

Check:

```text
sales_orders.customer_id is parseable
inventory_quants.product_id exists in silver.products
inventory_pickings.sales_order_id exists in silver.sales_orders when present
inventory_pickings product linkage is not directly available in the supplied header
sales_order_lines.sales_order_id exists in silver.sales_orders
sales_order_lines.product_id exists in silver.products
```

Report orphan counts and sample keys.

Do not reject a structurally valid source record merely because a related dataset has not yet arrived in the current processing order.

---

## 23. Important Business Decisions to Preserve

1. `products.qty_available` is the approved usable-stock quantity for initial stock-cover KPIs.
2. `products.virtual_available` is projected inventory and is not current sellable stock.
3. `list_price` is selling/reference price, not inventory cost.
4. Future retail valuation may use:

```text
inventory_retail_value = qty_available * list_price
```

5. `inventory_quants` supplies location-level balance detail but must not be summed blindly across vendor, input, transit, and internal locations.
6. Sales Gold KPIs must use business timestamps such as `ops_order_at` or `date_order`, not `write_date`.
7. Product Gold dimension will be SCD Type 2.
8. Silver products remains current-state.
9. Product SCD2 tracks descriptive attributes, not live inventory measures.
10. Future Gold inventory history will be captured in a product inventory snapshot fact.

---

## 24. Out of Scope

Do not implement in this task:

```text
datalake.gold.dim_product
datalake.gold.fact_sales_order_line
datalake.gold.fact_product_inventory_snapshot
datalake.gold.product_inventory_kpi_snapshot
Sales Velocity
Days of Stock Cover
ABC Classification
Inventory Health Classification
Airflow DAGs
```

The current task ends when the four datasets are reliably available in Silver and all notebooks/jobs/tests pass.

---

## 25. Final Agent Response Requirements

The assigned agent must return a concise implementation report containing:

1. files created and modified;
2. project structure after changes;
3. source schemas used;
4. source and target paths;
5. table schemas and partitioning;
6. validation rules per dataset;
7. primary keys and MERGE rules;
8. counts from every pipeline stage;
9. GX and exploration outcomes;
10. checkpoint rerun results;
11. command examples;
12. known limitations;
13. deviations from this specification and reasons;
14. confirmation that the existing order-lines pipeline still imports and runs correctly.

The agent must not claim completion without execution evidence for all eight notebooks and all executable jobs, or a clear explanation of any environmental blocker.
