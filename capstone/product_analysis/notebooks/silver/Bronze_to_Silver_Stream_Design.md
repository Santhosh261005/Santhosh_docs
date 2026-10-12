# Bronze to Silver Streaming Pipeline Design

## Processing Flow

```text
Bronze HDFS CSV
       |
       v
Spark Structured Streaming
       |
       v
foreachBatch(batch_df, batch_id)
       |
       +--> Cache and materialize batch
       |
       +--> Data exploration
       |      └── datalake.operations.explorations
       |
       +--> Great Expectations checkpoint
       |      └── datalake.operations.expectation_profiles
       |
       +--> Parse Odoo relation columns
       |
       +--> Apply deterministic row-level validation rules
       |
       +--> Split valid and rejected records
       |           |
       |           +--> Rejected Iceberg table
       |
       +--> Deduplicate valid records within micro-batch
       |
       +--> MERGE valid records into Silver Iceberg table
       |
       +--> Reconciliation and Silver audit
       |
       v
Unpersist batch
```

---

## Great Expectations Configuration

Configured **12 validation rules** covering:

- Primary-key null checks
- Odoo relationship-column null checks
- Quantity null checks
- Price null checks
- Timestamp null checks
- Discount range validation (0 to 100)

---

## Validation Outcome

### Failed Validation

When Spark validation fails:

```text
Record → sales_order_lines_rejected
```

### Passed Validation

When Spark validation passes:

```text
Record → Deduplication → Silver MERGE
```

---

## Profiling Tables

### Exploration Metrics

```text
datalake.operations.explorations
```

Stores data exploration and profiling metrics generated for each micro-batch.

### Great Expectations Results

```text
datalake.operations.expectation_profiles
```

Stores Great Expectations validation results and checkpoint outputs.

---

## Audit Table

### datalake.operations.silver_ingestion_audit

Stores one processing summary per micro-batch, including:

- Batch metadata
- Input record counts
- Valid record counts
- Rejected record counts
- Silver write statistics
- Processing timestamps

---

## Rejected Records Table

### datalake.quarantine.sales_order_lines_rejected

Stores:

- Original Bronze values
- Successfully parsed relationship fields
- All rejection reasons
- Failed validation rule names
- Source lineage information
- Stream lineage information

---

## Output Datasets

### Valid Records

Stored in:

```text
datalake.silver.sales_order_lines
```

Schema matches the final Silver data model.

### Rejected Records

Stored in:

```text
datalake.quarantine.sales_order_lines_rejected
```

Contains validation failures together with rejection metadata.

---

## Silver MERGE Strategy

This process upserts deduplicated valid records into the Silver Iceberg table.

### MERGE Behavior

| Scenario | Action |
|----------|---------|
| New ID | Insert |
| Newer incoming version | Update |
| Equal timestamp | No change |
| Older incoming version | No change |

---

## Deduplication Logic

Micro-batch deduplication reduces multiple records having the same ID to a single latest version before the Silver MERGE.

When duplicate records are encountered again in future stream executions, unnecessary updates are prevented using the condition:

```sql
source.write_date > target.write_date
```

This ensures:

- Latest version wins
- Older versions cannot overwrite newer Silver records
- Reprocessing remains idempotent

---

## ForeachBatch Processor Responsibilities

The foreachBatch callback performs:

1. Data exploration
2. Great Expectations profiling
3. Odoo relationship parsing
4. Deterministic row validation
5. Rejected-record table writes
6. Micro-batch deduplication
7. Iceberg MERGE into Silver
8. Reconciliation checks
9. Audit logging

The implementation uses the mentor-provided profiler APIs and templates for:

- Exploration profiling
- Great Expectations validation
- Data quality monitoring
- Operational audit generation
