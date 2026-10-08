# Sales Performance and Product History Analytics Platform

## Revised End-to-End Blueprint for the New Incremental Dataset Structure

**Project type:** Batch-first Lakehouse and Snowflake analytics pipeline  
**Core technologies:** Airflow, PySpark, HDFS or object storage, Apache Iceberg, Snowflake, Python, SQL  
**Primary business scope:** Sales performance, product performance, product-price history, product-category history, product-version analysis, discounts, order status, fulfilment, and invoicing progress  
**Primary fact grain:** One valid sales-order line linked to the product version effective at the order time

---

# 1. Executive Summary

The project keeps the original business requirements and KPI framework, but redesigns the data flow for the new source structure.

The original design assumed a small set of large, stable JSONL files. The revised source is organized as ordered API batches, for example:

```text
landing-zone/
├── api-00003/
│   └── jsonl/
│       ├── sales/orders/<timestamp>.jsonl
│       ├── sales/order-lines/<timestamp>.jsonl
│       ├── inventory/pickings/<timestamp>.jsonl
│       ├── invoicing/invoices/<timestamp>.jsonl
│       └── invoicing/invoice-lines/<timestamp>.jsonl
├── api-00004/
├── api-00005/
├── ...
├── api-00033/
└── contacts-00036/
    └── jsonl/contact-details/customers/<timestamp>.jsonl
```

The latest published business state is reconstructed by applying batches in numeric order, beginning with the baseline and then applying later inserts, updates, and explicit deletions. The new architecture therefore treats the landing zone as an immutable sequence of published batches rather than as a single replaceable snapshot.

The revised pipeline is:

```text
Landing batch discovery and validation
              |
              v
Immutable Bronze append and corrupt-record isolation
              |
              v
Silver event normalization + ordered CDC application
              |
              v
Silver current-state and historical entity tables
              |
              v
Gold dimensions, SCD history, sales fact, operational facts, and aggregates
              |
              v
Controlled Snowflake publication and dashboards
```

---

# 2. Business Requirements That Remain Unchanged

The revised architecture must continue to answer the original business questions.

## 2.1 Sales Manager requirements

- What is total gross and net sales revenue?
- How many orders were created, confirmed, completed, or cancelled?
- What is total quantity sold?
- What is average order value?
- What is weighted average selling price?
- Which products and categories generate the most revenue?
- Which products receive large discounts?
- How are sales changing month over month?
- What is the cancellation rate?

## 2.2 Product Manager requirements

- Which product prices changed?
- When did a price or category change become effective?
- What was product performance before and after the change?
- Which products changed categories?
- Which historical product version was valid when a sale occurred?
- Which active products have no sales?
- Which products are underperforming or discount-dependent?

## 2.3 Operations requirements

- What quantity was ordered, delivered, invoiced, and still pending?
- Which orders completed picking, packing, and dispatch?
- Which orders are delayed?
- Which orders are cancelled source orders versus synthetic unmet demand?
- Which invoices are posted, paid, partially paid, or not paid?

## 2.4 Data Engineering and Data Quality requirements

- Process source batches in deterministic numeric order.
- Preserve raw source evidence and replayability.
- Detect malformed JSON, schema drift, missing files, and duplicate files.
- Support initial baseline loads and later incremental batches.
- Apply inserts, updates, and explicit deletes idempotently.
- Quarantine invalid records without silently losing them.
- Reconcile counts and amounts between layers.
- Rerun a selected batch from a selected stage.
- Prevent overlapping publication attempts for the same batch.
- Retain useful execution, quality, lineage, and publication logs.

---

# 3. Revised Understanding of the Dataset

## 3.1 Published API batches

The source is a series of published API batches such as `api-00003`, `api-00004`, and `api-00033`. Each batch may contain only the entities or record IDs changed or published in that batch.

Therefore:

- The latest incremental file is not expected to contain every order.
- An entity's latest state may be found in an earlier batch if no later change exists.
- Batches must be applied in numeric order.
- A baseline is required before applying later increments.
- Batch order is part of the state-reconstruction logic.

## 3.2 Separate contact batches

Customer details can arrive through separately numbered contact batches such as:

```text
contacts-00036/jsonl/contact-details/customers/<timestamp>.jsonl
```

Customer data must therefore have an independent ingestion watermark and control state. A sales batch must not be marked corrupt simply because the matching customer update arrives through a different source family.

## 3.3 Two timestamp families

The source contains two conceptually different clocks.

### Business-operation timestamps

Examples:

```text
ops_order_at
scheduled_date
date_done
ops_issued_at
ops_paid_at
```

These describe simulated business activity and should be used for business KPIs such as order-to-dispatch time, fulfilment time, invoice issuance, and payment timing.

### Replay and extraction timestamps

Examples:

```text
create_date
write_date
file timestamp
ingestion_timestamp
batch publication timestamp
```

These describe publication, replay, or source-system execution and should be used for CDC sequencing, audit, and technical freshness.

Business KPIs must not substitute `write_date` for an operational event timestamp.

## 3.4 Available trace relationships

The operational trace is:

```text
Source InvoiceNo
    -> source_invoice / ref
    -> sale.order.client_order_ref
    -> sales order ID
    -> sales order-line order_id
    -> picking sale_id
    -> invoice-line sale_line_ids
    -> invoice move_id
    -> invoice ID
```

Customer tracing is:

```text
sales order partner_id
    -> contact ID
    -> contact ref = CUST-<CustomerID>
    -> source customer
```

## 3.5 Important semantic distinctions

- A source cancellation is a native cancelled quotation or order path. It must not automatically be interpreted as a refund.
- Synthetic unmet demand is explicitly identified and follows its own cancellation policy.
- A normal delayed order is not the same as unmet demand.
- Absence of an exported invoice does not by itself prove a stock shortage.
- Invoice payment state and residual are direct evidence. Exact payment allocation requires a trustworthy journal-line bridge.
- WAL is an incremental technical change stream, not a complete order snapshot.

---

# 4. Revised End-to-End Architecture

```text
                     SOURCE PRODUCERS
       API batches | contact batches | optional WAL stream
                            |
                            v
+------------------------------------------------------------+
| LANDING ZONE                                               |
| Immutable batch folders, source manifests, received files  |
+------------------------------------------------------------+
                            |
                  Airflow batch registration
                            |
                            v
+------------------------------------------------------------+
| BRONZE                                                     |
| Raw append-only Iceberg/Parquet tables                     |
| Valid parse rows + corrupt rows + schema observations       |
+------------------------------------------------------------+
                            |
                  Ordered batch application
                            |
                            v
+------------------------------------------------------------+
| SILVER                                                     |
| Normalized events, deduplicated changes, current entities, |
| entity history, relationship bridges, quality quarantine   |
+------------------------------------------------------------+
                            |
                  Business transformations
                            |
                            v
+------------------------------------------------------------+
| GOLD                                                       |
| Dimensions, SCD2 product history, sales line fact,         |
| optional fulfilment/invoice facts, KPI aggregates          |
+------------------------------------------------------------+
                            |
               Reconciliation + publication lock
                            |
                            v
+------------------------------------------------------------+
| SNOWFLAKE ANALYTICAL SERVING                               |
| Reporting schemas, secure views, dashboards                |
+------------------------------------------------------------+
```

> Snowflake is the analytical serving warehouse in this design. It should not be labelled as the OLTP source.

---

# 5. Landing Zone Blueprint

## 5.1 Landing-zone purpose

The landing zone is a file-arrival area and immutable evidence boundary. It is not a curated business layer.

## 5.2 Recommended layout

```text
/landing-zone/
├── api-00003/
│   ├── _manifest/manifest.json
│   ├── jsonl/sales/orders/<timestamp>.jsonl
│   ├── jsonl/sales/order-lines/<timestamp>.jsonl
│   ├── jsonl/products/products/<timestamp>.jsonl
│   ├── jsonl/products/categories/<timestamp>.jsonl
│   ├── jsonl/inventory/pickings/<timestamp>.jsonl
│   ├── jsonl/invoicing/invoices/<timestamp>.jsonl
│   └── jsonl/invoicing/invoice-lines/<timestamp>.jsonl
├── api-00004/
├── ...
└── contacts-00036/
    ├── _manifest/manifest.json
    └── jsonl/contact-details/customers/<timestamp>.jsonl
```

## 5.3 Do not immediately move source files

The image suggests that Airflow may move files from landing to a processed directory. For replayability and evidence preservation, the recommended design is:

1. Keep the original landing batch immutable.
2. Register the file and checksum in the batch-control tables.
3. Copy or ingest the file into Bronze.
4. Mark the file as `BRONZE_COMMITTED` in metadata.
5. Optionally archive the complete immutable batch after the retention period.

If operational policy requires moving files, move only after Bronze commit and retain the same batch-relative path in an immutable archive.

## 5.4 Landing validation

For every batch:

- Validate batch naming convention.
- Parse numeric batch sequence.
- Confirm expected folder structure.
- Validate manifest if present.
- Calculate file checksum and size.
- Reject zero-byte or nonregular files.
- Detect duplicate file checksum and duplicate source path.
- Confirm file extension and compression type.
- Record source family: `api`, `contacts`, or `wal`.
- Record discovered entities.
- Do not require every entity in every incremental batch.

## 5.5 Landing statuses

```text
DISCOVERED
REGISTERED
VALIDATED
READY_FOR_BRONZE
BRONZE_COMMITTED
REJECTED
ARCHIVED
```

## 5.6 Bad batch handling

A batch is considered structurally bad when, for example:

- The manifest is invalid.
- A required baseline file is missing.
- Two files claim the same identity with different checksums.
- A batch sequence is duplicated incorrectly.
- Files are copied from an unauthorized QA or staging location.
- The source folder is incomplete at the time of discovery.

Bad batches are moved logically, not destructively, into:

```text
/quarantine/batches/<source_family>/<batch_id>/
```

The original source evidence remains retained.

---

# 6. Batch and File Control Model

## 6.1 `audit.batch_control`

```text
source_family
batch_id
batch_sequence
batch_type
previous_published_batch_id
status
attempt_number
discovered_at
started_at
completed_at
failed_at
published_at
error_code
error_message
```

### Batch types

```text
BASELINE
INCREMENTAL
CONTACT_INCREMENTAL
WAL_INCREMENTAL
REPROCESS
```

## 6.2 `audit.file_control`

```text
source_family
batch_id
entity_name
source_relative_path
source_file_name
file_timestamp
file_size_bytes
file_checksum
file_format
discovered_at
bronze_table
bronze_record_count
corrupt_record_count
schema_fingerprint
status
```

## 6.3 Processing order

- Process the baseline first.
- Process API batches in numeric order.
- Process contact batches independently by their own sequence.
- Do not publish batch `N+1` state before batch `N` if both affect the same source family and entity state.
- Allow entity-level parallelism inside a batch after file validation.

---

# 7. Landing-to-Bronze Blueprint

## 7.1 Processing mode

The recommended mode is Airflow-triggered Spark Structured Streaming with `availableNow` or a deterministic Spark batch job.

### Recommended default

```text
Airflow scheduled batch
    -> Spark availableNow
    -> process all newly registered files
    -> maintain checkpoint
    -> stop after available input is committed
```

This provides file-discovery convenience without requiring an always-running cluster.

### Alternative modes

1. Standard Spark batch for a selected `batch_id`.
2. Always-running file stream only if low-latency arrival is a genuine requirement.
3. Kafka or WAL streaming as a separate technical ingestion path, not mixed with batch file state without a declared precedence rule.

## 7.2 Bronze table strategy

Use one Bronze table per source entity, not one giant table.

```text
bronze.sales_orders_raw
bronze.sales_order_lines_raw
bronze.products_raw
bronze.product_categories_raw
bronze.customers_raw
bronze.inventory_pickings_raw
bronze.invoices_raw
bronze.invoice_lines_raw
bronze.wal_events_raw              # optional
bronze.corrupt_records
bronze.schema_observations
```

## 7.3 Bronze storage format

Preferred:

```text
Apache Iceberg tables with append-only writes
```

Acceptable alternative:

```text
Partitioned Parquet plus external metadata catalog
```

An Iceberg table should represent one logical entity. Do not combine unrelated orders, products, pickings, and invoices into one broad table.

## 7.4 Bronze organization

If Iceberg is used, physical partition choices should be limited and stable, for example:

```text
source_family
batch_date or days(ingestion_timestamp)
```

Keep high-cardinality values such as `batch_id` as columns rather than creating excessive small directory partitions unless the actual engine benefits from that layout.

If plain Parquet is used:

```text
/bronze/<entity>/ingestion_year=2026/ingestion_month=10/ingestion_day=08/
```

## 7.5 Bronze record metadata

```text
source_family
batch_id
batch_sequence
source_relative_path
source_file_name
source_file_timestamp
source_row_number
source_entity
source_record_id
source_create_date
source_write_date
source_deleted_flag
ingested_at
run_id
record_hash
schema_fingerprint
raw_payload
```

## 7.6 Valid and invalid parse handling

### Valid parse rows

Valid JSON or CSV records are appended to the relevant entity Bronze table.

### Broken rows

Malformed rows are appended to:

```text
bronze.corrupt_records
```

with:

```text
batch_id
source_file
source_row_number
raw_text
parse_error
expected_schema_version
observed_at
```

A malformed row does not automatically fail the entire batch. Failure thresholds are configurable.

## 7.7 Schema drift handling

Classify schema changes as:

```text
ADDITIVE_COMPATIBLE
TYPE_WIDENING_REVIEW
BREAKING_TYPE_CHANGE
MISSING_REQUIRED_FIELD
UNKNOWN_NESTED_STRUCTURE
```

Actions:

- Additive optional columns: accept into raw payload and register the new schema.
- Type widening: accept only through an approved compatibility rule.
- Required-column removal or breaking type change: quarantine the affected file or fail the entity task.
- Never silently drop unknown fields from Bronze.

## 7.8 Landing-to-Bronze reconciliation

For each file:

```text
input_record_count
=
bronze_valid_parse_count
+
bronze_corrupt_count
```

The following file metrics are required:

```text
input_records_count
valid_parse_records
invalid_parse_records
processed_time
source_file_name
source_checksum
batch_id
run_id
```

---

# 8. Bronze-to-Silver Blueprint

## 8.1 Silver architectural pattern

Silver contains three distinct types of tables:

1. Normalized change tables
2. Reconstructed current-state tables
3. Historical or relationship bridge tables

This separation prevents raw event logic from becoming mixed with business-ready current state.

## 8.2 Normalized change tables

```text
silver_change.sales_orders_changes
silver_change.sales_order_lines_changes
silver_change.products_changes
silver_change.product_categories_changes
silver_change.customers_changes
silver_change.inventory_pickings_changes
silver_change.invoices_changes
silver_change.invoice_lines_changes
```

Each row represents a normalized source observation from a particular batch.

Required metadata:

```text
entity_business_key
batch_id
batch_sequence
source_write_date
source_deleted_flag
normalized_record_hash
change_type
change_applied_at
```

## 8.3 Current-state tables

```text
silver_current.sales_orders
silver_current.sales_order_lines
silver_current.products
silver_current.product_categories
silver_current.customers
silver_current.inventory_pickings
silver_current.invoices
silver_current.invoice_lines
```

For each business key, the current-state table contains the latest valid record after ordered batch application.

## 8.4 Historical and bridge tables

```text
silver_history.products_history
silver_bridge.invoice_line_sale_line
silver_bridge.order_picking
silver_bridge.order_customer
silver_bridge.order_invoice
```

### `invoice_line_sale_line` grain

One row per invoice-line ID and sale-line ID extracted from `sale_line_ids`.

```text
invoice_line_id
sale_line_id
invoice_move_id
batch_id
source_write_date
```

### `order_picking` grain

One row per sales order and picking event.

```text
order_id
picking_id
picking_name
stage
state
scheduled_date
date_done
batch_id
```

## 8.5 Deduplication order

For duplicates of the same business key inside or across batches, use deterministic ordering:

```text
1. Higher batch_sequence
2. Later source_write_date
3. Later source_file_timestamp
4. Higher source_row_number or deterministic source tie-breaker
```

Do not use ingestion arrival time as the primary business ordering when source sequencing is available.

## 8.6 CDC application

### Initial baseline

Insert the latest valid record per business key from the baseline.

### Later incremental batches

Classify records:

```text
INSERT
UPDATE
DELETE
NO_CHANGE
LATE_ARRIVING_UPDATE
DUPLICATE_OBSERVATION
INVALID_CHANGE
```

### Explicit delete

Apply deletion only when the source provides a trusted delete indicator or WAL delete event.

### Missing from incremental file

Do nothing. Incremental files do not contain all current records.

## 8.7 Silver standardization

- Convert IDs to stable numeric or string types.
- Normalize booleans and false-like values.
- Standardize decimal precision.
- Normalize timestamps while retaining source timezone interpretation.
- Preserve both business timestamps and technical timestamps.
- Standardize order states and payment states.
- Explode array relationships such as `sale_line_ids` into bridge rows.
- Retain original source status alongside standardized status.

## 8.8 Silver business rules

### Orders

- `order_id` is non-null.
- `ops_order_at` is valid where expected.
- Order state belongs to an accepted domain.
- `ops_unmet` and source cancellation are distinguishable.
- Closed-state logic is internally consistent.

### Order lines

- `order_line_id`, `order_id`, and `product_id` are non-null.
- Ordered quantity is positive for normal sales lines.
- Delivered and invoiced quantities are nonnegative unless an approved signed transaction rule applies.
- The referenced order exists or is temporarily marked as an unresolved late parent.

### Products and categories

- The product business key is unique in current state.
- Product category references resolve or use a controlled unknown record.
- List price is nonnegative.
- Active status is normalized.

### Pickings

- Picking stage is standardized to values such as `picking`, `packing`, and `dispatch`.
- Completed timestamp is required for `done` events.
- Cancelled pickings are not treated as completed.

### Invoices

- Invoice state and payment state are standardized.
- Residual amount is nonnegative unless approved accounting rules allow otherwise.
- Invoice issue time is a business timestamp.

## 8.9 Silver quarantine

```text
silver_quarantine.invalid_records
silver_quarantine.unresolved_relationships
silver_quarantine.schema_drift
silver_quarantine.bad_batches
```

Required fields:

```text
batch_id
entity_name
business_key
rule_id
severity
failure_reason
failed_value
source_file
source_row_number
raw_payload
quarantined_at
resolution_status
```

## 8.10 Bronze-to-Silver reconciliation

```text
Bronze valid parse rows
=
Silver accepted observations
+
Silver duplicates
+
Silver quarantined records
```

Current-state counts are not expected to equal raw Bronze counts because multiple source observations can update the same business key.

---

# 9. Product and Category History Strategy

## 9.1 Category SCD Type 1

Use SCD Type 1 for descriptive category corrections.

```text
gold.dim_category
```

Tracked fields:

```text
category_name
parent_category_id
complete_category_name
active_flag
```

A corrected category description overwrites the current Gold row.

## 9.2 Product SCD Type 2

Use SCD Type 2 to preserve historically meaningful product changes.

```text
gold.dim_product
```

Tracked fields:

```text
product_name
category_id
unit_of_measure_id
list_price
active_flag
```

Version fields:

```text
product_key
product_id
effective_start_timestamp
effective_end_timestamp
is_current
version_number
record_hash
created_batch_id
updated_batch_id
```

## 9.3 Effective timestamp selection

Preferred order:

1. Trusted business-effective timestamp, if provided.
2. Source `write_date`.
3. Batch publication timestamp.

The chosen rule must be consistent and documented. Technical ingestion time should be used only as a fallback.

## 9.4 Late-arriving product changes

If an older product change arrives after a newer product version:

- Insert the historical version into the correct effective interval.
- Split or adjust neighboring intervals.
- Re-key affected fact rows only if the business-effective timestamp proves that the previous product-key assignment was wrong.
- Record the repair batch and affected-row count.

## 9.5 SCD validations

- Exactly one current version per active product.
- No overlapping effective intervals.
- No zero-length or negative intervals.
- Every sales fact resolves to one and only one effective product version.

---

# 10. Revised Gold Model

The original sales and product-history star schema remains the primary Gold model. The new operational entities are added as optional supporting facts and bridges rather than forcing everything into one table.

## 10.1 Core dimensions

```text
gold.dim_date
gold.dim_product
gold.dim_category
gold.dim_order_status
gold.dim_company
```

Optional dimensions when enabled:

```text
gold.dim_customer
gold.dim_picking_stage
gold.dim_invoice_status
gold.dim_payment_status
```

## 10.2 Core sales fact

```text
gold.fact_sales
```

### Grain

> One row per valid sales-order line linked to the product version effective at `ops_order_at` or the approved order business timestamp.

### Keys

```text
sales_fact_key
order_id
order_line_id
order_date_key
product_key
category_key
order_status_key
company_key
customer_key              # optional
```

### Measures

```text
ordered_quantity
delivered_quantity
invoiced_quantity
remaining_quantity
unit_price
historical_list_price
discount_percentage
gross_sales_amount
discount_amount
net_sales_amount
source_subtotal_amount
subtotal_difference
```

### Flags

```text
order_count_flag
cancelled_order_flag
confirmed_order_flag
closed_order_flag
unmet_order_flag
fully_delivered_flag
partially_delivered_flag
fully_invoiced_flag
```

### Lineage

```text
source_batch_id
source_batch_sequence
source_file
source_write_date
record_hash
published_batch_id
created_at
updated_at
```

## 10.3 Optional fulfilment fact

```text
gold.fact_order_fulfilment
```

### Grain

One row per order and operational fulfilment stage.

```text
order_id
picking_id
stage_key
scheduled_date_key
completed_date_key
scheduled_timestamp
completed_timestamp
stage_duration_minutes
stage_state
is_delayed
source_batch_id
```

This fact supports picking, packing, dispatch, completion, and delay analysis without changing the grain of `fact_sales`.

## 10.4 Optional invoice fact

```text
gold.fact_invoice
```

### Grain

One row per invoice.

```text
invoice_id
invoice_number
customer_key
invoice_date_key
due_date_key
invoice_status_key
payment_status_key
amount_total
amount_residual
collected_amount
issued_timestamp
paid_timestamp
source_batch_id
```

## 10.5 Relationship bridge

```text
gold.bridge_sale_line_invoice
```

### Grain

One row per sale-line and invoice-line relationship.

```text
sale_line_id
invoice_line_id
invoice_id
allocation_method
source_batch_id
```

Do not infer exact payment IDs from customer and amount alone.

## 10.6 Gold aggregate tables

```text
gold.agg_daily_sales
gold.agg_monthly_product_sales
gold.agg_monthly_category_sales
gold.agg_order_status_daily
gold.agg_product_change_impact
```

Optional:

```text
gold.agg_fulfilment_daily
gold.agg_invoice_status_daily
```

---

# 11. Gold KPI Preservation

The original KPIs remain unchanged.

## 11.1 Primary sales calculations

```text
Gross Sales Amount = Ordered Quantity * Unit Price
```

```text
Discount Amount = Gross Sales Amount * Discount Percentage / 100
```

```text
Net Sales Amount = Gross Sales Amount - Discount Amount
```

```text
Remaining Quantity = Ordered Quantity - Delivered Quantity
```

## 11.2 Main dashboard KPIs

1. Net Sales Revenue
2. Total Orders
3. Total Quantity Sold
4. Average Order Value
5. Average Selling Price
6. Month-over-Month Sales Growth
7. Top 10 Products by Revenue
8. Revenue by Category
9. Cancellation Rate
10. Products with Price Changes

## 11.3 Product-history KPIs

- Product change count
- Products with price changes
- Average price-change percentage
- Revenue by product version
- Current and historical product-version counts
- Products that changed category
- Sales before and after a product change
- Historical list-price variance

## 11.4 Operational KPIs enabled by the new structure

These supplement, but do not replace, the original KPIs.

```text
Order-to-dispatch duration
Picking duration
Packing duration
Dispatch completion rate
Fulfilment rate
Pending delivery quantity
Invoicing rate
Outstanding invoice amount
Paid / partially paid / unpaid invoice counts
Unmet demand count
Unmet demand expiry count
```

## 11.5 KPI timestamp rules

- Sales trends use `ops_order_at` or approved order business date.
- Stage durations use `scheduled_date` and `date_done`.
- Invoice issuance uses `ops_issued_at`.
- Payment timing uses `ops_paid_at` only when available and trusted.
- Technical freshness uses file timestamp, source `write_date`, and ingestion timestamp.

---

# 12. Order Lifecycle and Traceability

## 12.1 Completed order path

```text
Order created
    -> order confirmed
    -> stock reserved
    -> picking completed
    -> packing completed
    -> dispatch completed
    -> customer invoice posted
    -> payment reconciled
    -> ops_closed = true
```

## 12.2 Source cancellation path

```text
Source cancellation
    -> cancelled quotation/order
    -> no delivery expected
    -> no customer invoice required
```

Do not infer a refund without verified linkage to an earlier sale and accounting evidence.

## 12.3 Unmet demand path

```text
Synthetic unmet order
    -> reservation unavailable
    -> cancelled after policy window
    -> no delivery
    -> no customer invoice
    -> reservations released
```

## 12.4 Pending order interpretation

A pending order can have warehouse completion but still remain open because an invoice is not paid. Distinguish:

- Warehouse pending
- Invoice pending
- Payment pending
- Operational closure pending

Do not classify all open orders as fulfilment failures.

## 12.5 Traceability table

Create:

```text
gold.order_trace
```

### Grain

One row per order with the latest summarized lifecycle state.

```text
order_id
order_number
source_invoice_reference
customer_id
order_timestamp
order_state
is_closed
is_unmet
ordered_quantity
delivered_quantity
invoiced_quantity
pick_completed_at
pack_completed_at
dispatch_completed_at
invoice_id
invoice_status
payment_status
amount_residual
trace_completeness_status
last_source_batch_id
```

This is a convenience table or view. It does not replace the atomic facts and bridges.

---

# 13. Data Quality Framework

## 13.1 File-level checks

- File exists and is readable.
- File checksum matches registration.
- File is not unexpectedly empty.
- Source path matches the allowed source family and entity.
- Batch sequence is valid.
- File has not already been committed with the same checksum.

## 13.2 Record-level checks

- Business key is present.
- Required timestamps are parseable.
- Numeric fields use valid precision.
- Array relationships are syntactically valid.
- Source deletion flag is normalized.

## 13.3 Referential checks

- Order line resolves to an order.
- Order line resolves to a product.
- Product resolves to a category or controlled unknown category.
- Picking resolves to a sales order when `sale_id` is expected.
- Invoice-line `sale_line_ids` resolve to sales lines or enter unresolved relationship quarantine.
- Invoice-line `move_id` resolves to an invoice.

## 13.4 Business checks

- Ordered quantity is positive for normal sales.
- Delivered quantity does not exceed approved business tolerance.
- Invoiced quantity does not exceed approved business tolerance.
- Cancelled source orders are not counted as realized revenue.
- Synthetic unmet orders have no delivered or invoiced quantity.
- A `done` picking has a completion timestamp.
- Posted invoice state is consistent with required invoice fields.
- Payment residual is consistent with payment state.

## 13.5 SCD checks

- One current product version per product.
- No overlapping intervals.
- Every sales fact resolves to one product version.
- Late repairs are fully auditable.

## 13.6 Quality severity

```text
INFO
WARNING
QUARANTINE_RECORD
FAIL_ENTITY
FAIL_BATCH
BLOCK_PUBLICATION
```

---

# 14. Reconciliation Framework

## 14.1 Landing to Bronze

```text
Landing input rows
=
Bronze valid parse rows
+
Bronze corrupt rows
```

## 14.2 Bronze to Silver observations

```text
Bronze valid parse rows
=
Silver accepted observations
+
Duplicate observations
+
Quarantined observations
```

## 14.3 Silver current state

For each entity:

```text
Current active keys
=
Baseline keys
+
Applied inserts
-
Applied explicit deletes
```

Updates change state but not the number of business keys unless the business key itself is invalidly changed.

## 14.4 Silver to Gold sales

```text
Eligible Silver order lines
=
Gold fact_sales rows
+
Documented business exclusions
+
Unresolved-key quarantine
```

## 14.5 Amount reconciliation

```text
SUM(Silver calculated net sales for eligible statuses)
=
SUM(Gold fact_sales net sales)
```

## 14.6 Quantity reconciliation

```text
SUM(ordered quantity)
SUM(delivered quantity)
SUM(invoiced quantity)
```

must reconcile between eligible Silver lines and Gold facts.

## 14.7 Lifecycle reconciliation

- Source cancellation orders must not have realized delivery or invoice facts unless explicitly justified.
- Synthetic unmet orders must follow the expected cancellation path.
- Closed completed sales should have consistent delivery and invoice evidence according to the business simulation rules.

---

# 15. Airflow DAG Blueprint

## 15.1 Parent batch DAG

```text
register_batch
    -> validate_batch_structure
    -> register_files
    -> validate_file_checksums
    -> choose_processing_mode
    -> ingest_bronze_entities
    -> validate_bronze_reconciliation
    -> build_silver_changes
    -> apply_silver_current_state
    -> build_relationship_bridges
    -> run_silver_quality
    -> apply_category_scd1
    -> apply_product_scd2
    -> build_gold_dimensions
    -> build_fact_sales
    -> build_optional_operational_facts
    -> build_gold_aggregates
    -> run_gold_quality
    -> reconcile_batch
    -> acquire_publication_lock
    -> publish_to_snowflake
    -> validate_snowflake_counts
    -> mark_batch_published
    -> release_publication_lock
    -> archive_or_mark_landing_batch
    -> publish_run_summary
```

## 15.2 Dynamic entity tasks

Bronze and Silver entity processing can run in parallel after batch validation:

```text
orders
order_lines
products
categories
customers
pickings
invoices
invoice_lines
```

Gold fact creation waits for the required dependencies.

## 15.3 Schedule

Recommended:

```text
Dataset-aware schedule after a complete source batch is registered
```

Fallback:

```text
Every 15 minutes or hourly batch discovery
```

Do not process a directory while files are still being written. Require an atomic manifest, completion marker, or stable-size waiting period.

## 15.4 Retry settings

```text
Retries: 2 or 3
Retry delay: 5 minutes
Exponential backoff: enabled
Task timeouts: entity-specific
max_active_runs: configurable by source family
publication max_active_runs: 1
```

## 15.5 Failure logs

```text
pipeline_name
source_family
batch_id
batch_sequence
run_id
task_id
attempt_number
entity_name
source_file
processing_layer
exception_type
error_code
error_message
rule_id
business_key
records_read
records_written
records_quarantined
started_at
failed_at
duration_seconds
```

---

# 16. Rerun and Idempotency Blueprint

## 16.1 Example rerun

```bash
airflow dags trigger sales_product_history_pipeline \
  --conf '{
    "source_family": "api",
    "batch_id": "api-00030",
    "rerun_from": "silver",
    "entities": ["sales_orders", "sales_order_lines", "inventory_pickings"]
  }'
```

## 16.2 Rerun rules

- Reuse the original logical batch ID.
- Create a new run ID and attempt number.
- Read the same registered source checksums.
- Fail if the source file changed after registration unless an approved replacement process is used.
- Delete or overwrite only batch-owned intermediate outputs.
- Use deterministic merges for current state and Gold.
- Preserve all previous attempts in audit tables.
- Recompute dependent bridges, facts, and aggregates when required.

## 16.3 Idempotency keys

```text
Bronze observation:
source_family + batch_id + source_file + source_row_number

Silver current entity:
entity_name + business_key

Product SCD2 version:
product_id + effective_start_timestamp + record_hash

Gold sales fact:
order_line_id
or company_id + order_id + order_line_id

Publication:
pipeline_name + source_family + batch_id + target_environment
```

## 16.4 Publication lock

The publication-control table must enforce:

```text
UNIQUE (
  pipeline_name,
  source_family,
  batch_id,
  target_environment
)
```

Statuses:

```text
READY
PUBLISHING
PUBLISHED
FAILED
SKIPPED_ALREADY_PUBLISHED
```

---

# 17. Snowflake Serving Blueprint

## 17.1 Database schemas

```text
ANALYTICS_CONTROL
ANALYTICS_DIM
ANALYTICS_FACT
ANALYTICS_AGG
ANALYTICS_SECURE
```

## 17.2 Publication pattern

1. Load batch data into a transient stage table.
2. Validate row counts and required keys.
3. Merge dimensions.
4. Merge facts using stable business keys.
5. Rebuild or merge affected aggregate partitions.
6. Record publication metrics.
7. Commit the publication status.

## 17.3 Snowflake is not the source OLTP

The operational source is the published API or database replay. Snowflake is the analytical serving layer for reporting, dashboards, and controlled analytical queries.

---

# 18. Dashboard and Business Outputs

## 18.1 Executive overview

- Net Sales Revenue
- Total Orders
- Total Quantity Sold
- Average Order Value
- Average Selling Price
- Total Discount
- Month-over-Month Growth
- Cancellation Rate

## 18.2 Product performance

- Top and bottom products by revenue
- Quantity by product
- Product revenue contribution
- Weighted discounts
- Historical list-price variance
- Active products without sales

## 18.3 Product history

- Product price timeline
- Product category timeline
- Revenue by product version
- Sales before and after product changes
- Current and expired product versions

## 18.4 Category performance

- Revenue and quantity by category
- Category contribution
- Product count by category
- Top-product share in category
- Monthly category trend

## 18.5 Operational lifecycle

- Orders by state
- Source cancellation versus unmet cancellation
- Picking, packing, and dispatch completion
- Order-to-dispatch duration
- Fulfilment and invoicing rates
- Paid, partially paid, and unpaid invoices

## 18.6 Pipeline quality

- Batch status and sequence
- Files discovered and committed
- Valid and corrupt parse counts
- CDC insert, update, delete, duplicate, and late-arrival counts
- Quarantine counts by rule
- Reconciliation differences
- Publication attempts and lock outcomes

---

# 19. Directory and Table Summary

## 19.1 Physical directories

```text
/landing-zone/<source_family_batch>/...
/archive/<source_family>/<batch_id>/...
/checkpoints/<pipeline>/<entity>/...
/quarantine/batches/<source_family>/<batch_id>/...
/quarantine/records/<entity>/...
```

If using plain Parquet:

```text
/bronze/<entity>/ingestion_year=YYYY/ingestion_month=MM/ingestion_day=DD/
/silver_change/<entity>/...
/silver_current/<entity>/...
/gold/<table>/...
```

## 19.2 Logical tables

### Bronze

```text
sales_orders_raw
sales_order_lines_raw
products_raw
product_categories_raw
customers_raw
inventory_pickings_raw
invoices_raw
invoice_lines_raw
corrupt_records
schema_observations
```

### Silver

```text
sales_orders_changes
sales_order_lines_changes
products_changes
product_categories_changes
sales_orders_current
sales_order_lines_current
products_current
product_categories_current
customers_current
inventory_pickings_current
invoices_current
invoice_lines_current
invoice_line_sale_line
order_picking
order_invoice
invalid_records
unresolved_relationships
```

### Gold

```text
dim_date
dim_product
dim_category
dim_order_status
dim_company
fact_sales
fact_order_fulfilment              # optional
fact_invoice                        # optional
bridge_sale_line_invoice            # optional
order_trace                         # convenience view/table
agg_daily_sales
agg_monthly_product_sales
agg_monthly_category_sales
agg_product_change_impact
```

---

# 20. Key Design Decisions and Corrections

1. **Landing remains immutable.** Do not destructively move files before Bronze commit.
2. **Batch sequence matters.** Apply API batches in numeric order.
3. **Missing from an incremental batch is not deletion.** Apply only explicit deletion evidence.
4. **One Iceberg table per logical entity.** Do not use one giant Bronze or Gold table.
5. **Separate raw observations from current state.** Bronze is append-only; Silver reconstructs current entities.
6. **Separate business and technical clocks.** Use operational timestamps for KPIs and replay timestamps for CDC.
7. **Keep the original fact grain.** `fact_sales` remains one row per order line.
8. **Add operational facts without changing sales grain.** Pickings and invoices become separate facts or bridges.
9. **Snowflake is OLAP serving.** It is not the source OLTP system.
10. **Use available-now streaming only as a processing mechanism.** The business semantics remain ordered micro-batch processing.
11. **Do not infer refunds or payments without trustworthy joins.** Preserve evidence boundaries.
12. **Do not infer stock shortage from missing invoice evidence.** Use warehouse and invoice states separately.

---

# 21. Implementation Phases

## Phase 1: Source profiling

- Inventory each new entity path.
- Confirm fields, data types, arrays, and deletion markers.
- Confirm baseline and incremental batch semantics.
- Produce schema registry and source-to-target mapping.

## Phase 2: Landing and control framework

- Implement batch discovery.
- Implement file registration and checksum logic.
- Implement completion detection.
- Implement bad-batch quarantine.

## Phase 3: Bronze ingestion

- Create entity-specific Iceberg or Parquet tables.
- Implement valid and corrupt parse handling.
- Implement schema drift observations.
- Implement file-level reconciliation.

## Phase 4: Silver reconstruction

- Normalize entity changes.
- Implement deterministic deduplication.
- Build current-state tables.
- Build invoice-line and picking bridges.
- Implement record-level quarantine.

## Phase 5: SCD and Gold sales

- Implement category SCD1.
- Implement product SCD2.
- Build dimensions.
- Build `fact_sales` with historical product lookup.
- Preserve existing KPIs.

## Phase 6: Operational extensions

- Build fulfilment fact.
- Build invoice fact.
- Build order trace view.
- Add delay, invoice, and payment-status KPIs where evidence is sufficient.

## Phase 7: Airflow and publication

- Implement dataset-aware dependencies.
- Configure retries and failure logs.
- Implement selected-batch reruns.
- Implement publication lock.
- Publish to Snowflake.

## Phase 8: Validation and demonstration

Demonstrate:

- Baseline load
- One later incremental update
- Explicit deletion
- Duplicate file detection
- Malformed record quarantine
- Schema drift handling
- Late-arriving update
- Product SCD2 version creation
- Selected-batch rerun
- Prevented overlapping publication
- End-to-end order trace

---

# 22. Acceptance Criteria

The redesigned project is complete when:

- The new batch folder structure is correctly discovered and registered.
- The earliest baseline and later API batches are applied deterministically.
- Contact batches are independently tracked.
- Landing files remain replayable.
- Bronze stores all valid parse records and corrupt rows with lineage.
- Schema drift is detected and classified.
- Silver change tables preserve observations.
- Silver current tables represent the latest published state.
- Missing records in an incremental file are not treated as deletions.
- Product SCD2 history is valid and nonoverlapping.
- Category SCD1 updates are idempotent.
- `fact_sales` preserves the original order-line grain.
- Each sale resolves to the historically correct product version.
- Original sales and product-history KPIs remain available.
- Operational facts do not change the sales fact grain.
- Counts, quantities, and amounts reconcile across layers.
- A selected batch can be rerun safely.
- The same batch cannot be published concurrently twice.
- Snowflake contains validated dimensions, facts, aggregates, and audit metrics.

---

# 23. Mentor Presentation Summary

> The project keeps the original Sales Performance and Product History business requirements, KPIs, and order-line fact grain, but redesigns the pipeline for the new incremental batch structure. Source data now arrives in ordered API folders such as `api-00003` and `api-00033`, with separate contact batches. The landing zone remains immutable. Airflow registers complete batches and triggers entity-specific PySpark ingestion. Bronze stores append-only raw observations, corrupt records, and schema observations. Silver normalizes changes, applies deterministic batch ordering, reconstructs current state, and builds relationship bridges for pickings and invoices. Category corrections use SCD Type 1, while product price and category history use SCD Type 2. Gold retains the original sales star schema and adds optional fulfilment and invoice facts without changing the sales fact grain. Data quality, reconciliation, idempotent reruns, and batch-level publication locking ensure that only validated data is published to Snowflake.

---

# 24. 60-Second Explanation

> The source is no longer a few large snapshot files. It is now a sequence of published API batches, and each batch may contain only changed entities or records. Therefore, I keep the Landing files immutable, append every raw observation into entity-specific Bronze tables, and apply batches in numeric order in Silver to reconstruct the latest state. Missing records in an incremental file are not treated as deletes; only explicit deletion evidence is applied. The original category SCD Type 1, product SCD Type 2, order-line sales fact, and KPIs remain unchanged. The new picking and invoice datasets are added through separate facts and bridges so the sales grain remains stable. Airflow handles complete-batch detection, retries, selected-batch reruns, and publication locking, while Snowflake remains the analytical serving layer.
