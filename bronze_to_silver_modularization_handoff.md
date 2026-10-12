# Bronze-to-Silver Modularization Handoff Guide

## 1. Objective

Refactor the working `sales_order_lines` Bronze-to-Silver notebook into reusable Python modules without changing the validated business behavior.

The completed pipeline currently:

- reads partitioned Bronze CSV files from HDFS using Spark Structured Streaming;
- processes each micro-batch through `foreachBatch`;
- caches and materializes each static micro-batch;
- runs data exploration and Great Expectations profiling;
- parses Odoo relation columns;
- applies row-level Silver validation;
- writes rejected records to an Iceberg quarantine table;
- deduplicates valid records using `id`, with the latest `write_date` winning;
- stages deduplicated data in Iceberg to ensure a deterministic MERGE source;
- upserts valid data into the Silver Iceberg table;
- performs reconciliation and writes operational audit records;
- uses a checkpoint to prevent already processed files from being processed again.

The refactor must preserve all of this behavior while making the code reusable for future datasets.

---

## 2. Validated Current Behavior

The current `sales_order_lines` implementation has been functionally validated with these results:

```text
Bronze source records:       10,497
Valid before deduplication:  10,491
Rejected records:                 6
Duplicates removed:           6,994
Valid after deduplication:    3,497
Silver records:               3,497
Duplicate IDs in Silver:          0
```

The checkpoint rerun test produced:

```text
Input rows processed:             0
Silver count unchanged:        True
Rejected count unchanged:      True
Audit count unchanged:         True
```

These results are the regression baseline. The refactored implementation must reproduce the same outcomes when run against the same input and clean target/checkpoint state.

---

## 3. Target Project Structure

```text
product_analysis/
├── data_profilers/
│   ├── __init__.py
│   ├── exploration.py
│   ├── great_expectations.py
│   └── schema_drift.py
│
├── pipelines/
│   ├── __init__.py
│   ├── bronze_to_silver_common.py
│   └── sales_order_lines_silver.py
│
├── jobs/
│   ├── __init__.py
│   └── sales_order_lines_bronze_to_silver.py
│
├── schemas/
│   └── sales_order_lines_schema.json
│
└── notebooks/
    ├── sales_order_lines_landing_to_bronze.ipynb
    └── sales_order_lines_bronze_to_silver.ipynb
```

### Purpose of each area

- `data_profilers/`: mentor-provided or profiler-specific utilities. Do not mix ingestion and business transformation logic into these modules.
- `pipelines/bronze_to_silver_common.py`: dataset-independent Bronze-to-Silver utilities.
- `pipelines/sales_order_lines_silver.py`: transformations, validations, deduplication, table definitions, and callback logic specific to sales order lines.
- `jobs/sales_order_lines_bronze_to_silver.py`: executable entry point used by `spark-submit` and later by Airflow.
- `notebooks/`: development, testing, inspection, and demonstration only. The notebook should import production functions rather than duplicate them.

---

## 4. Refactoring Principles

### 4.1 Preserve behavior before improving design

Do not combine refactoring with unrelated business-rule changes. First move the already validated logic into modules, run regression tests, and only then make enhancements.

### 4.2 Keep common code dataset-independent

The common module must not contain hardcoded references to:

```text
sales_order_lines
sales_order_line_id
order_id
product_id
specific table names
specific validation rules
```

Pass such values as function arguments or keep them in the dataset-specific module.

### 4.3 Keep DataFrame transformations lazy

Transformation functions should generally return DataFrames without calling actions such as:

```python
count()
collect()
show()
writeTo()
spark.sql("MERGE ...")
```

Actions and writes should be coordinated in the `foreachBatch` processor or explicit writer functions.

### 4.4 Make side effects obvious

Function names should make writes and table mutations clear, for example:

```text
write_rejected_records
write_silver_audit
merge_sales_order_lines_to_silver
create_sales_order_lines_tables
```

### 4.5 Do not use driver collection for business data

Do not call `collect()` or `toLocalIterator()` on normal or large business datasets. Small configuration or test-only collections are acceptable, but pipeline writes and validations should remain distributed.

### 4.6 Cache ownership must be explicit

The `foreachBatch` callback owns the micro-batch caches it creates and must release them in `finally`. Helper functions should not silently unpersist a DataFrame cached by the caller.

---

## 5. `bronze_to_silver_common.py`

This module contains reusable utilities that can support products, sales orders, inventory, purchase lines, and other future datasets.

### 5.1 Required functions

Move these functions into this module:

```text
write_rejected_records
create_silver_audit_row
write_silver_audit
evaluate_micro_batch_reconciliation
count_incoming_keys_found_in_silver
count_incoming_versions_satisfied_in_silver
```

### 5.2 Recommended additional helpers

Add these reusable helpers if they reduce duplication:

```text
configure_logging
extract_hive_path_value
build_source_lineage_columns
validate_dataframe_columns
validate_table_exists
quote_iceberg_table_name
```

### 5.3 Function requirements

#### `write_rejected_records`

Responsibilities:

- accept an already shaped rejected DataFrame;
- optionally accept a known `rejected_count` to avoid another action;
- skip the write when count is zero;
- validate expected column order when an expected schema or column list is supplied;
- append records to the given Iceberg table;
- return the number of records written;
- log the action instead of using `print()`.

Suggested generic signature:

```python
def write_rejected_records(
    rejected_df,
    *,
    rejected_table,
    expected_columns=None,
    rejected_count=None,
    logger=None,
):
    ...
```

#### `create_silver_audit_row`

Responsibilities:

- build a dictionary matching the common Silver audit-table schema;
- accept `dataset_name` and `processing_stage` as arguments;
- never refer directly to `sales_order_lines`;
- normalize Python numeric values to `int` where appropriate;
- allow nullable results when a failure occurs before all counts are available.

#### `write_silver_audit`

Responsibilities:

- create a one-row DataFrame using an explicitly supplied audit schema;
- append it to an Iceberg audit table;
- return the generated audit ID;
- log success or failure.

#### `evaluate_micro_batch_reconciliation`

Perform these checks:

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

Return structured details, not only a Boolean:

```python
{
    "source_split_passed": True,
    "deduplication_passed": True,
    "silver_merge_passed": True,
    "reconciliation_status": "RECONCILED",
}
```

#### `count_incoming_keys_found_in_silver`

Responsibilities:

- compare distinct incoming keys with target keys using a distributed `left_semi` join;
- accept the key column name as an argument;
- return the number of incoming keys present after MERGE.

Suggested generic signature:

```python
def count_incoming_keys_found_in_silver(
    spark_session,
    incoming_df,
    *,
    silver_table,
    key_column,
):
    ...
```

#### `count_incoming_versions_satisfied_in_silver`

Responsibilities:

- verify that the target contains the same or a newer version for each incoming business key;
- accept key and version-column names as arguments;
- use a distributed join;
- consider a record satisfied when:

```text
target.version_column >= incoming.version_column
```

### 5.4 No dataset-specific schemas in the common module

Do not import `sales_order_lines` table schemas into this module. Pass schemas or expected column lists from the dataset-specific module.

---

## 6. `sales_order_lines_silver.py`

This module owns everything that depends on the `sales_order_lines` structure or business rules.

### 6.1 Required contents

Move these functions into this module:

```text
transform_and_validate_sales_order_lines
create_valid_silver_df
create_rejected_records_df
deduplicate_valid_records
merge_sales_order_lines_to_silver
process_sales_order_lines_batch
```

Also move or define:

```text
silver_schema
rejected_schema
silver_audit_schema
Great Expectations rule builder
exploration column configuration
Iceberg table creation function
Bronze business-schema loading helper
```

### 6.2 Avoid module-level runtime state

Do not make `process_sales_order_lines_batch` depend on mutable globals such as:

```text
PIPELINE_RUN_ID
SILVER_TABLE
REJECTED_TABLE
SILVER_AUDIT_TABLE
EXPECTATION_TABLE
```

Instead, pass a typed configuration object or construct the callback through a factory.

Recommended pattern:

```python
def build_sales_order_lines_batch_processor(
    *,
    config,
    logger,
):
    def process_batch(batch_df, stream_batch_id):
        ...

    return process_batch
```

Then:

```python
batch_processor = build_sales_order_lines_batch_processor(
    config=config,
    logger=logger,
)
```

This allows `foreachBatch` to receive a two-argument callback while still giving it all runtime configuration.

### 6.3 `transform_and_validate_sales_order_lines`

Preserve these behaviors:

1. Rename raw relation columns:

```text
order_id   -> order_id_raw
product_id -> product_id_raw
```

2. Parse Odoo relationship arrays:

```text
parsed_sales_order_id
parsed_sales_order_name
parsed_product_id
parsed_product_display_name
```

3. Apply these row-level validations:

```text
id must not be null
order_id must not be null
order_id must parse to a numeric ID
product_id must not be null
product_id must parse to a numeric ID
product_uom_qty must not be null
price_unit must not be null
discount must not be null
discount must be between 0 and 100
price_subtotal must not be null
create_date must not be null
write_date must not be null
qty_delivered must not be null
qty_invoiced must not be null
```

4. Produce:

```text
failed_rule_names ARRAY<STRING>
rejection_reasons ARRAY<STRING>
record_status = VALID or REJECTED
```

5. Add:

```text
stream_batch_id
pipeline_run_id
processed_at
```

Replace `F.current_timestamp()` with a deterministic timestamp literal created once per callback, or materialize before MERGE. The current validated implementation uses an Iceberg staging table because Iceberg MERGE rejected a source logical plan containing non-deterministic expressions.

### 6.4 `create_valid_silver_df`

Return exactly the Silver table column order:

```text
sales_order_line_id
sales_order_id
sales_order_name
product_id
product_display_name
product_uom_qty
price_unit
discount
price_subtotal
create_date
write_date
qty_delivered
qty_invoiced
source_batch_id
source_file
stream_batch_id
pipeline_run_id
silver_created_at
silver_updated_at
```

Do not carry raw Odoo relation strings into Silver.

### 6.5 `create_rejected_records_df`

Return exactly the quarantine table column order. Preserve:

- raw Bronze values;
- any successfully parsed relationship values;
- all failed rules and reasons;
- source batch and file lineage;
- stream batch and pipeline run IDs;
- rejection timestamp.

### 6.6 `deduplicate_valid_records`

Business key:

```text
sales_order_line_id
```

Winner:

```text
latest write_date
```

Deterministic tie-breakers:

```text
source_batch_id descending
source_file descending
```

Return both:

```text
deduplicated_df
duplicate_records_df
```

Required reconciliation:

```text
valid_before_dedup_count
=
valid_after_dedup_count + duplicate_count
```

### 6.7 `merge_sales_order_lines_to_silver`

Preserve the materialized staging-table method.

Required sequence:

1. Build a unique temporary Iceberg staging-table name from `pipeline_run_id` and `stream_batch_id`.
2. Drop a leftover table with the same name to support retry.
3. Validate null and duplicate merge keys.
4. Write the deduplicated DataFrame to the staging table.
5. Reconcile staging count.
6. MERGE staging into the Silver table.
7. Update only when:

```sql
source.write_date > target.write_date
```

8. Insert records when the key is absent.
9. Preserve target `silver_created_at` during updates.
10. Refresh `silver_updated_at` during updates.
11. Drop the staging table in `finally`.

Do not revert to a temporary view as the MERGE source unless the non-deterministic expression issue has been conclusively removed and regression-tested.

### 6.8 `process_sales_order_lines_batch`

The callback must preserve this order:

```text
cache batch
  -> count/materialize
  -> add source lineage
  -> exploration profiling
  -> Great Expectations profiling
  -> transform and validate
  -> split valid/rejected
  -> reconcile split
  -> write rejected
  -> deduplicate valid
  -> reconcile deduplication
  -> stage and MERGE Silver
  -> verify keys and versions
  -> final reconciliation
  -> write batch audit
  -> unpersist in finally
```

Profiler failures should be logged and recorded but should not block valid records. Core transformation, quarantine write, MERGE, and reconciliation failures should fail the micro-batch and be re-raised so Spark does not advance the checkpoint.

---

## 7. Configuration Model

Create a typed configuration object in either the dataset-specific module or a small dedicated configuration module.

Recommended dataclass:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class SalesOrderLinesSilverConfig:
    dataset_name: str
    bronze_path: str
    checkpoint_path: str
    silver_table: str
    rejected_table: str
    audit_table: str
    exploration_table: str
    expectation_table: str
    schema_file: str
    pipeline_run_id: str
    max_files_per_trigger: int
    trigger_type: str
    catalog: str = "datalake"
```

Benefits:

- removes mutable global constants;
- makes dependencies explicit;
- makes callbacks easier to test;
- prevents accidental use of the wrong dataset path or table;
- enables future jobs to follow the same configuration pattern.

---

## 8. Argument Parser and Environment Variables

The executable job should use command-line arguments with environment-variable defaults.

Priority:

```text
explicit command-line argument
    -> environment-variable default
    -> safe code default
```

Example parser responsibilities:

```text
--dataset-name
--bronze-path
--checkpoint-path
--silver-table
--rejected-table
--audit-table
--exploration-table
--expectation-table
--schema-file
--pipeline-run-id
--max-files-per-trigger
--trigger-type
--log-level
```

Environment-level values:

```text
SPARK_MASTER
HDFS_DEFAULT_FS
ICEBERG_WAREHOUSE
ICEBERG_SPARK_RUNTIME_JAR
LOG_LEVEL
ENVIRONMENT
```

Notebook usage may use `parse_known_args()` or an explicit `NOTEBOOK_ARGS` list because Jupyter injects its own kernel arguments. The executable job must use normal `parse_args()`.

---

## 9. Logging Requirements

Replace pipeline `print()` calls with a standard module logger.

Use:

```python
logger = logging.getLogger(__name__)
```

Configure logging only in the executable job, not repeatedly inside imported modules.

Recommended message context:

```text
dataset_name
pipeline_run_id
stream_batch_id
processing_stage
source_count
valid_count
rejected_count
duplicate_count
```

Level guidance:

- `DEBUG`: schema, configuration internals, detailed test information.
- `INFO`: start/end of pipeline, counts, writes, MERGE, reconciliation success.
- `WARNING`: non-fatal profiler failure, empty batch, failed GX rules that do not block valid rows.
- `ERROR`: reconciliation failure or failed required write.
- `EXCEPTION`: inside `except` blocks to include the stack trace.

Do not log secrets or full credential-bearing connection strings.

---

## 10. `jobs/sales_order_lines_bronze_to_silver.py`

This file is the orchestration entry point for local `spark-submit` and later Airflow.

It should contain only startup and orchestration logic:

```text
parse arguments
  -> finalize pipeline_run_id
  -> configure logging
  -> create Spark session
  -> load schema
  -> create namespaces/tables
  -> build GX expectations
  -> create streaming DataFrame
  -> build foreachBatch callback
  -> start availableNow query
  -> await termination
  -> log final status
  -> stop Spark
```

Recommended outline:

```python
def main():
    args = parse_arguments()
    logger = configure_logging(args.log_level)
    config = build_config(args)
    spark = create_spark_session(config)

    try:
        create_sales_order_lines_tables(spark, config)

        stream_df = create_sales_order_lines_bronze_stream(
            spark,
            config,
        )

        batch_processor = build_sales_order_lines_batch_processor(
            config=config,
            logger=logger,
        )

        query = start_sales_order_lines_stream(
            stream_df,
            batch_processor,
            config,
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(query.exception())

    except Exception:
        logger.exception(
            "Sales-order-lines Bronze-to-Silver job failed"
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## 11. Notebook After Refactoring

The notebook should become a thin development and validation layer.

Suggested cells:

```text
1. Project path and imports
2. Simulated notebook arguments
3. Logging and configuration construction
4. Spark-session creation
5. Schema/table initialization
6. Import and inspect modular functions
7. Optional static transformation test
8. Create streaming DataFrame
9. Start availableNow query
10. Validate Silver/quarantine/audit tables
11. Verify checkpoint idempotency
```

The notebook must not contain duplicate copies of production functions. If notebook experimentation changes a function, apply the change to the module and reload it.

---

## 12. Table Contracts to Preserve

### Silver table

```text
datalake.silver.sales_order_lines
```

Partition transform:

```text
days(write_date)
```

Primary business key:

```text
sales_order_line_id
```

Version field:

```text
write_date
```

### Rejected table

```text
datalake.quarantine.sales_order_lines_rejected
```

Partition transform:

```text
days(rejected_at)
```

### Processing audit table

```text
datalake.operations.silver_ingestion_audit
```

### Exploration table

```text
datalake.operations.explorations
```

### Great Expectations audit table

```text
datalake.operations.expectation_profiles
```

---

## 13. Streaming Contract to Preserve

Source:

```text
hdfs:///capstone/warehouse/bronze/sales_order_lines
```

Checkpoint:

```text
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/sales_order_lines
```

Trigger:

```python
.trigger(availableNow=True)
```

Reader requirements:

```text
format = csv
explicit business schema
header = true
quote = "
escape = "
timestampFormat = yyyy-MM-dd'T'HH:mm:ss.SSSX
recursiveFileLookup = true
multiLine = false
maxFilesPerTrigger = configurable, currently 5
```

Path metadata must still be derived through `input_file_name()` because ordinary directories such as `file_00000` occur between Hive-style path components.

Extract:

```text
batch_id=([^/]+)
write_date_partition=([^/]+)
```

---

## 14. Retry and Idempotency Requirements

### Checkpoint behavior

- Successful micro-batches advance the checkpoint.
- Failed micro-batches must re-raise the exception so the checkpoint is not advanced.
- Reusing the same checkpoint with no new files must process zero rows.

### Silver idempotency

- Deduplicate inside each micro-batch.
- Use Iceberg MERGE by `sales_order_line_id`.
- Update only when incoming `write_date` is newer.
- Equal or older versions must not modify the target.

### Rejected-record retry concern

The current rejected-table write is append-based. If rejected records are written and the same micro-batch later fails before checkpoint commit, retry can duplicate rejected rows.

The assigned agent should either:

1. preserve the current behavior and document the limitation, or preferably;
2. make rejected writes idempotent using a deterministic rejection key and Iceberg MERGE/delete-before-insert strategy.

Suggested deterministic rejection key components:

```text
dataset_name
source_file
source_batch_id
stream_batch_id
id
failed_rule_names
```

Do not silently introduce this change without regression testing.

### Profiler retry concern

Exploration and GX audit rows can also repeat when a micro-batch fails after profiler writes. Preserve current behavior initially, but document this as a future idempotency enhancement.

---

## 15. Testing Requirements

### 15.1 Import test

Verify all modules import without starting Spark jobs automatically.

```python
from pipelines.bronze_to_silver_common import (
    evaluate_micro_batch_reconciliation,
)

from pipelines.sales_order_lines_silver import (
    transform_and_validate_sales_order_lines,
)
```

### 15.2 Static transformation test

Use the current Bronze static read and verify:

```text
source = valid + rejected
valid before dedup = valid after dedup + duplicates
no duplicate keys after dedup
```

### 15.3 Table-schema test

Confirm exact DataFrame column order matches the Iceberg table contracts.

### 15.4 Clean integration test

Use a clean temporary checkpoint and empty target tables, then reproduce:

```text
source_count                 = 10,497
valid_before_dedup_count     = 10,491
rejected_count               = 6
duplicate_count              = 6,994
valid_after_dedup_count      = 3,497
Silver distinct key count    = 3,497
```

### 15.5 Checkpoint rerun test

Run the same query again with the same checkpoint and no new source files. Verify:

```text
input rows = 0
Silver count unchanged
rejected count unchanged
audit count unchanged
```

### 15.6 Newer-version MERGE test

Create a controlled record with an existing `sales_order_line_id` and newer `write_date`. Verify business fields update and `silver_created_at` remains unchanged.

### 15.7 Older/equal-version MERGE test

Verify equal or older `write_date` does not update the Silver row.

### 15.8 Failure test

Force a failure after cache creation and verify all callback-owned DataFrames are unpersisted in `finally`.

---

## 16. Acceptance Criteria

The modularization is complete only when all of the following are true:

- [ ] Notebook functions have been moved into the intended modules.
- [ ] The notebook imports functions rather than redefining them.
- [ ] Common utilities contain no sales-order-lines-specific constants.
- [ ] Dataset-specific logic is isolated in `sales_order_lines_silver.py`.
- [ ] Runtime values are supplied through a configuration object.
- [ ] The executable job uses `argparse` with environment-variable defaults.
- [ ] Modules use logging instead of `print()`.
- [ ] The job runs through `spark-submit`.
- [ ] The same validated record counts are reproduced.
- [ ] Silver contains one record per `sales_order_line_id`.
- [ ] Latest `write_date` wins.
- [ ] Rejected records preserve reasons and lineage.
- [ ] Staging-table MERGE remains deterministic.
- [ ] Checkpoint rerun processes zero records.
- [ ] No staging table remains after success or failure.
- [ ] Importing a module does not start a Spark session or stream.

---

## 17. Deliverables Expected from the Assigned Agent

The assigned agent should return:

1. `pipelines/bronze_to_silver_common.py`
2. `pipelines/sales_order_lines_silver.py`
3. `jobs/sales_order_lines_bronze_to_silver.py`
4. Updated `pipelines/__init__.py` and `jobs/__init__.py`
5. An updated thin notebook that imports the modules
6. A concise README section showing:
   - notebook execution;
   - `spark-submit` execution;
   - environment variables;
   - expected tables and checkpoint;
7. Regression-test evidence with counts
8. A list of any intentional behavior changes
9. A list of known retry/idempotency limitations

---

## 18. Out of Scope for This Refactor

Do not begin the Gold layer inside this modularization task.

Do not change the validated business rules unless explicitly requested.

Do not redesign the mentor-provided exploration or Great Expectations profiler implementations.

Do not remove the Iceberg staging-table MERGE workaround without proving that the replacement handles deterministic MERGE analysis.

Do not introduce a generic framework so abstract that the existing pipeline becomes difficult to understand. Reuse should reduce duplication while keeping dataset behavior explicit.

---

## 19. Recommended Next Work After Modularization

Once the agent completes and validates this refactor, development can proceed independently on higher-value work:

1. design Gold-layer dimensional and aggregate models;
2. build product-performance metrics;
3. build sales velocity and days-of-stock-cover measures;
4. implement ABC product classification;
5. implement inventory-health classification;
6. orchestrate the modular jobs with Airflow;
7. extend the reusable Bronze-to-Silver pattern to sales orders, products, inventory, procurement, and supplier datasets.
