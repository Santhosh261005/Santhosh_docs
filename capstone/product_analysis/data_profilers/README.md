# Data profilers: student guide

Use these profilers to inspect Spark data and write findings to Iceberg audit tables.
They do not repair records, split good/bad business data, or write Bronze/quarantine
outputs. Enable any profiler independently. Great Expectations (GX) takes a
DataFrame and a list of expectation objects; you do not pass a runner or checkpoint.

## Package and environment

Copy the entire `data_profilers` directory beside your notebook or Python script:

```text
project/
  notebook.ipynb
  data_profilers/
    __init__.py
    _audit.py
    schema_drift.py
    exploration.py
    great_expectations.py
```

Spark must have an Iceberg catalog named `datalake` configured. The existing
three notebooks show this configuration. Tested versions are Spark 3.5.7,
Iceberg 1.9.2 and Great Expectations 1.24.0. Only GX examples require GX:

```bash
python -m pip install great_expectations==1.24.0
```

The examples below assume an existing configured `spark` session. Set the source
path to your own directory. Local file URIs, HDFS and configured S3A are supported
by the schema profiler's Hadoop file listing; remote connectors and credentials
must already be configured. Source paths are folders or individual files, not
Python `Path.glob` patterns.

## Which function should I use?

| Question | Function | Input | Return |
| --- | --- | --- | --- |
| Which records did Spark's parser reject? | `profile_corrupt_records` | Static DataFrame containing `_corrupt_record` | `None`, after audit writes |
| Does the original file schema differ from my expected schema? | `profile_schema_drift` | Spark session, source path, expected `StructType`, format | `None`, after audit writes |
| What are the counts, nulls and other statistics? | `explore_data` | Spark session and DataFrame or table name | Lazy measures DataFrame |
| Store exploration measures | `write_exploration_to_iceberg` | Spark session and measures DataFrame | Destination table name |
| Does this data meet my business rules? | `profile_expectations` | Static DataFrame and GX expectation objects | `None`, after audit writes |

See [API.md](API.md) for complete function signatures.

## Audit tables

| Default table | Contents | Row granularity |
| --- | --- | --- |
| `datalake.operations.corrupt_records` | Parser summary and original rejected text | One summary per call plus one row per corrupt record; or one capture-unavailable row |
| `datalake.operations.schema_drift_profiles` | File schemas, differences, counts, source metadata and inspection errors | One row per inspected file; missing paths/no matching files produce a status row |
| `datalake.operations.explorations` | Counts and statistical measures | One row per measure/column/scope |
| `datalake.operations.expectation_profiles` | GX checkpoint summary and every expectation result | One overall result plus one row per evaluated rule |

The package creates missing namespaces/tables and appends results. Repeated
calls append new profiles; writes are not deduplicated automatically. Supply a
shared `run_id` to correlate outputs, and a new ID for each distinct run.

### Common audit columns

These columns apply to corrupt-record, schema-drift and GX tables. Exploration
uses the separate measure schema shown below.

| Column | Spark type | Meaning |
| --- | --- | --- |
| `run_id` | STRING | Caller-supplied ID or generated UUID |
| `dataset_name` | STRING | Optional dataset label |
| `profile_kind` | STRING | `CORRUPT_RECORDS`, `SCHEMA_DRIFT` or `GREAT_EXPECTATIONS` |
| `source_file` | STRING | File path when available; null for summaries and GX results |
| `status` | STRING | Result category from the table below |
| `details_json` | STRING | JSON payload containing the profiler's details |
| `profiled_at` | TIMESTAMP | Audit timestamp generated in UTC |

| Profiler | Status | Meaning / details |
| --- | --- | --- |
| Corrupt | `SUMMARY` | `row_count` and `corrupt_count` for the selected DataFrame |
| Corrupt | `PARSER_REJECTED_RECORD` | `raw_record` and caller reason, or a generic parser reason |
| Corrupt | `CAPTURE_UNAVAILABLE` | Corrupt column absent; `corrupt_count` is null, not zero |
| Schema | `SCHEMA_MATCH` | No inferred physical field/type differences |
| Schema | `DRIFT_FOUND` | Added/missing/type differences; not a rejection decision |
| Schema | `INSPECTION_FAILED` | Path/file inspection failed; error text is recorded |
| Schema | `NO_MATCHING_FILES` | No files match the requested format |
| GX | `PASSED` / `FAILED` | Overall checkpoint result |
| GX | `RULE_PASSED` / `RULE_FAILED` | Individual expectation result and GX details |

Schema details include `actual_schema`, `expected_schema`, `differences`,
`row_count`, `corrupt_count`, `corrupt_capture`, `source_size_bytes` and
`source_modified_epoch_ms` on successful inspection. Differences identify
`ADDED_FIELD`, `MISSING_FIELD` or `TYPE_DIFFERENCE`. A parser-corrupt count can be
unknown when the reader does not expose capture; consult `corrupt_capture`.

### Exploration columns

| Columns | Meaning |
| --- | --- |
| `profile_id`, `run_id`, `batch_id` | Individual profile and caller correlation IDs |
| `dataset_name`, `source_table` | Dataset label and optional source table/view name |
| `profile_scope`, `processing_stage`, `profile_level` | BATCH/FILE, LANDING/BRONZE/QUARANTINE, FULL/LIGHTWEIGHT |
| `source_file_path` | Source path for FILE scope |
| `column_name`, `column_data_type` | Profiled column and Spark type |
| `measure_name`, `measure_value`, `measure_value_type` | Measure name, value stored as text and value type |
| `profiled_at` | Profile timestamp |

## Example 1: read JSONL and profile parser corruption

Include a nullable string `_corrupt_record` in your read schema. Preserve the
source filename explicitly at read time. It is not automatically a normal
DataFrame column.

```python
import uuid
from pyspark.sql import functions as F, types as T
from data_profilers.schema_drift import profile_corrupt_records

run_id = str(uuid.uuid4())
source_path = "file:///home/student/data/orders-jsonl"

expected_schema = T.StructType(
    [
        T.StructField("order_id", T.LongType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("amount", T.DoubleType(), True),
    ]
)
read_schema = T.StructType(
    expected_schema.fields
    + [
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)

df = (
    spark.read.schema(read_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(source_path)
    .withColumn("_source_file", F.input_file_name())
)

profile_corrupt_records(df, dataset_name="orders", run_id=run_id)
```

Spark supplies rejected text, not the exact parser error. The profiler logs a
generic explanation unless you provide your own `reason_column`. Some typed-read
conversion failures are parser corruption; missing required business values are
not automatically parser failures. A nullable read schema is intentional:
required-value rules belong in GX or your own validation.

Profile a subset, or use a reason column you have already populated:

```python
profile_corrupt_records(
    df,
    filter_condition=F.col("_source_file").contains("batch00001"),
    dataset_name="orders_batch00001",
    run_id=str(uuid.uuid4()),
)

# If df_with_reasons already contains your own parser_reason column:
profile_corrupt_records(
    df_with_reasons,
    reason_column="parser_reason",
    audit_table="datalake.operations.orders_parser_audit",
)
```

## Example 2: read CSV with corruption capture

For a CSV whose header matches the business columns in `expected_schema`:

```python
csv_df = (
    spark.read.schema(read_schema)
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("escape", '"')  # RFC-style doubled quotes in our Odoo CSVs.
    .csv("file:///home/student/data/orders-csv")
    .withColumn("_source_file", F.input_file_name())
)

profile_corrupt_records(csv_df, dataset_name="orders_csv", run_id=run_id)
```

CSV reads with all-string columns do not detect numeric conversion failures.
Spark can null-fill missing CSV fields or discard surplus fields without marking
the row corrupt. Enable `multiLine=True` when the source actually contains quoted
multiline cells. For files with different headers, read each header separately
and union by name, as shown in the CSV teaching notebook.

## Example 3: inspect original schemas independently

Pass the source path and expected schema. This function infers each file before
applying the expected schema; it does not use a projected or already-cast DF.

```python
from data_profilers.schema_drift import profile_schema_drift

profile_schema_drift(
    spark,
    source_path,
    expected_schema,
    source_format="jsonl",
    dataset_name="orders",
    run_id=run_id,
)

profile_schema_drift(
    spark,
    "hdfs:///training/orders-csv",
    expected_schema,
    source_format="csv",
    reader_options={"sep": ";"},
    dataset_name="orders_csv",
    run_id=run_id,
)

profile_schema_drift(
    spark,
    "s3a://training-bucket/orders-parquet/",
    expected_schema,
    source_format="parquet",
    dataset_name="orders_parquet",
    run_id=run_id,
    audit_table="datalake.operations.orders_schema_audit",
)
```

Empty arrays and all-null fields provide weak evidence of intended types.
Odoo relationships such as `[58, "Customer"]` or `false` differ physically from
a desired struct. Those differences are logged; they do not make a record
automatically invalid. Mixed `[number, name]` arrays may infer as arrays of
strings. CSV inference can interpret formatted identifiers as numbers.
The profiler never rewrites the original data to resolve these differences.

Per-file inspection errors are audited and later files continue. An audit sink
failure still raises an exception. Unsupported formats and invalid schema
arguments also raise configuration errors.

## Example 4: exploration only

```python
from data_profilers.exploration import explore_data, write_exploration_to_iceberg

measurements = explore_data(
    spark,
    df,
    dataset_name="orders",
    run_id=run_id,
    batch_id="batch00001",
    columns=["order_id", "customer_id", "quantity", "amount"],
    required_columns=["order_id", "customer_id"],
    processing_stage="LANDING",
    profile_level="FULL",
)
measurements.show(truncate=False)
table_name = write_exploration_to_iceberg(spark, measurements)
```

Use a table/view instead of a DataFrame, or profile each source file:

```python
table_measures = explore_data(
    spark,
    "datalake.bronze.orders",
    processing_stage="BRONZE",
    profile_level="LIGHTWEIGHT",
    run_id=run_id,
)
write_exploration_to_iceberg(spark, table_measures)

file_measures = explore_data(
    spark,
    df,
    dataset_name="orders",
    run_id=run_id,
    profile_scope="FILE",
    source_file_column="_source_file",
    columns=["order_id", "quantity", "amount"],
)
write_exploration_to_iceberg(
    spark,
    file_measures,
    table_name="orders_file_explorations",
)
```

FILE scope requires a non-null source-file column. Complex columns are profiled
according to type; the module avoids distinct/hash operations on complex values
that Spark cannot handle safely. Exploration returns lazy measures: write a
profile once to avoid duplicate audit rows.

## Example 5: Great Expectations rules, not runners

This small dataset has a missing order ID, a negative quantity and a negative
amount. GX evaluates the batch and the profiler audits the failures.

```python
import great_expectations as gx
from data_profilers.great_expectations import profile_expectations

example_df = spark.createDataFrame(
    [
        (101, 1, 2, 20.0),
        (102, 2, 1, 10.0),
        (None, 3, -2, -5.0),
    ],
    expected_schema,
)

rules = [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1),
    gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0),
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
]

profile_expectations(
    example_df,
    rules,
    dataset_name="orders_demo",
    run_id=run_id,
)
```

The function creates an ephemeral GX context, Spark DataFrame asset, suite,
validation definition and actual checkpoint internally. It copies rules before
adding them to the suite, so you can reuse the same rule objects:

```python
# This selection is a teaching example, not a general quarantine mechanism.
clean_example = example_df.filter(F.col("order_id").isNotNull())
profile_expectations(
    clean_example,
    rules,
    dataset_name="orders_demo_clean",
    run_id=str(uuid.uuid4()),
    audit_table="datalake.operations.orders_quality_audit",
)
```

| GX question | Current behavior |
| --- | --- |
| Are all failed rules audited? | Yes, every evaluated failed expectation gets `RULE_FAILED`. Passed rules are recorded too. |
| Is overall success recorded? | Yes, one `PASSED` or `FAILED` row per call. |
| Is every offending source row stored? | No. GX uses `SUMMARY`; unexpected-value samples can be limited. |
| Are failure counts available? | Many column rules include `unexpected_count` and percentages; available fields depend on the rule. |
| Are records quarantined? | No. GX evaluates a batch; individual-row routing is a separate operation. |
| Does a failed rule crash the profiler? | No, it is an audited quality finding. |
| Are engine/configuration/sink failures audited as failed rules? | No, those exceptions propagate. |

## Example 6: choose any combination

Each enabled block is independent. Set flags or comment out an entire block.
Imports of GX occur only when GX is enabled.

```python
RUN_CORRUPT = True
RUN_SCHEMA = True
RUN_EXPLORATION = False
RUN_GX = True

if RUN_CORRUPT:
    from data_profilers.schema_drift import profile_corrupt_records

    profile_corrupt_records(df, dataset_name="orders", run_id=run_id)

if RUN_SCHEMA:
    from data_profilers.schema_drift import profile_schema_drift

    profile_schema_drift(
        spark,
        source_path,
        expected_schema,
        source_format="jsonl",
        dataset_name="orders",
        run_id=run_id,
    )

if RUN_EXPLORATION:
    from data_profilers.exploration import explore_data, write_exploration_to_iceberg

    measures = explore_data(spark, df, dataset_name="orders", run_id=run_id)
    write_exploration_to_iceberg(spark, measures)

if RUN_GX:
    import great_expectations as gx
    from data_profilers.great_expectations import profile_expectations

    required_id = [gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")]
    profile_expectations(df, required_id, dataset_name="orders", run_id=run_id)
```

Corrupt/GX profilers preserve caller-owned caches and release their own temporary
caches in `finally`. No context manager is needed. Exploration remains lazy;
cache management for its returned measures belongs to the caller.

## Example 7: streaming with foreachBatch

The DataFrame profilers run on the static microbatch passed to `foreachBatch`.
Use `batch_df.sparkSession` for exploration and its writer inside the callback.
No file path is needed for corrupt-record profiling, exploration or GX. The
path-based schema profiler is a separate source inspection step outside this callback.

This example uses `read_schema`, `source_path`, `spark` and the GX `rules` from
previous examples. Turn each flag on/off independently:

```python
RUN_CORRUPT = True
RUN_EXPLORATION = True
RUN_GX = True
STREAM_RUN_NAME = "orders-stream-v1"  # Keep stable with the same checkpoint.

stream_df = (
    spark.readStream.schema(read_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(source_path)
    .withColumn("_source_file", F.input_file_name())
)


def profile_batch(batch_df, batch_id):
    # This callback receives a static DataFrame, not a streaming DataFrame.
    # Cache once for the three profilers and release when the callback finishes.
    batch_df.cache()
    try:
        if batch_df.count() == 0:
            return
        batch_spark = batch_df.sparkSession
        run_id = f"{STREAM_RUN_NAME}-{batch_id}"

        if RUN_CORRUPT:
            from data_profilers.schema_drift import profile_corrupt_records

            profile_corrupt_records(
                batch_df, dataset_name="orders_stream", run_id=run_id
            )

        if RUN_EXPLORATION:
            from data_profilers.exploration import (
                explore_data,
                write_exploration_to_iceberg,
            )

            measures = explore_data(
                batch_spark,
                batch_df,
                dataset_name="orders_stream",
                run_id=run_id,
                batch_id=str(batch_id),
                columns=["order_id", "quantity", "amount"],
                required_columns=["order_id"],
            )
            write_exploration_to_iceberg(batch_spark, measures)

        if RUN_GX:
            from data_profilers.great_expectations import profile_expectations

            profile_expectations(
                batch_df, rules, dataset_name="orders_stream", run_id=run_id
            )
    finally:
        batch_df.unpersist()


query = (
    stream_df.writeStream.foreachBatch(profile_batch)
    .option("checkpointLocation", "file:///home/student/checkpoints/orders")
    .trigger(once=True)
    .start()
)
query.awaitTermination()

# Refresh when reading audits from this outer session after callback writes.
for table in ("corrupt_records", "explorations", "expectation_profiles"):
    if spark.catalog.tableExists("datalake.operations." + table):
        spark.catalog.refreshTable("datalake.operations." + table)
```

For a continuous query, replace `.trigger(once=True)` with
`.trigger(processingTime="30 seconds")`. There is no Spark `processOnce` option;
`trigger(once=True)` requests a one-shot query.

The streaming checkpoint tracks Spark processing progress. It differs from the
GX validation checkpoint. `foreachBatch` can retry; append-only audits may contain
duplicates even with the same `run_id`. This example does not implement exactly-once
audit writes. Avoid profiling the entire source directory on every microbatch;
run path-based schema profiling separately.

### Runnable streaming example

[stream_foreachbatch_example.py](../stream_foreachbatch_example.py) creates isolated
good, business-invalid and malformed JSONL inputs, runs all three DataFrame
profilers, and checks the resulting Iceberg audits. It also restarts the query
with the same checkpoint, first with no new files and then with one new file.
It never accesses the original landing-zone data.

This runnable example passed on aadhi: batch 0 processed three records, a restart
without new files processed no batch, and batch 1 processed only the new good
record. The parser audit contained one malformed record, the first GX checkpoint
had three failed rules, and the new batch passed GX. Evidence is in
`../validation/stream-profiler-verification.json`.

Run from the parent project directory in the configured Spark/Iceberg/GX environment:

```bash
STREAM_EXAMPLE_ROOT=./student-output/stream-example python stream_foreachbatch_example.py
```

The script adds a unique subdirectory per invocation. Its expected checks are:

| Invocation | Input processed | Expected result |
| --- | --- | --- |
| First one-shot query | Three records: good, business-invalid and malformed JSON | One parser-corrupt record, three failed GX rules, exploration measures written |
| Restart, no new files | No new batches | Previous input is not processed again |
| Restart, one new file | Only the new good record | GX passes for the new batch |

Inspect `verification.json` in the run directory and the four audit tables as
applicable. This example uses the three DataFrame audit tables; it does not write
schema-drift profiles because no file-based inspection is performed in the callback.

## Example 8: query the findings

In Python, inspect one run:

```python
spark.table("datalake.operations.corrupt_records").where(
    F.col("run_id") == run_id
).select("status", "source_file", "details_json").show(truncate=False)
```

These SQL examples use a dataset filter. Add `run_id = 'your-run-id'` to scope
them to one run.

```sql
-- Original parser-rejected text.
SELECT run_id, source_file,
       get_json_object(details_json, '$.raw_record') AS raw_record,
       get_json_object(details_json, '$.reason') AS reason
FROM datalake.operations.corrupt_records
WHERE status = 'PARSER_REJECTED_RECORD' AND dataset_name = 'orders';

-- Schema differences and source inspection failures.
SELECT run_id, source_file, status,
       get_json_object(details_json, '$.differences') AS differences,
       get_json_object(details_json, '$.error') AS inspection_error
FROM datalake.operations.schema_drift_profiles
WHERE status IN ('DRIFT_FOUND', 'INSPECTION_FAILED', 'NO_MATCHING_FILES');

-- Every failed GX expectation, with rule-dependent failure counts.
SELECT run_id, dataset_name,
       get_json_object(details_json, '$.expectation_config.type') AS rule_type,
       get_json_object(details_json, '$.expectation_config.kwargs.column') AS column_name,
       get_json_object(details_json, '$.result.unexpected_count') AS unexpected_count,
       details_json
FROM datalake.operations.expectation_profiles
WHERE status = 'RULE_FAILED';

-- Overall GX checkpoint outcomes.
SELECT run_id, dataset_name, status, profiled_at
FROM datalake.operations.expectation_profiles
WHERE status IN ('PASSED', 'FAILED');

-- Exploration measures are text values with an accompanying type.
SELECT run_id, column_name, measure_name, measure_value, measure_value_type
FROM datalake.operations.explorations
WHERE dataset_name = 'orders';
```

Inspect `details_json` when a GX rule has no `column` or `unexpected_count`, such
as a table-level rule. A schema match alone does not establish business validity;
a GX pass covers only the rules you supplied.

## Included notebooks and verification

| Notebook in the parent directory | Example |
| --- | --- |
| `schema_drifting_csv_orders.ipynb` | Olist/Odoo orders, relationships and altered CSV values |
| `schema_drifting_jsonl_order_lines.ipynb` | Complex relationships, type changes and broken JSON |
| `schema_drifting_parquet_purchases.ipynb` | Missing supplier field and missing required ID |

All ten notebook runs passed on aadhi: all/none across the three formats and one
run with each profiler enabled alone. Additional API checks verified malformed
complex JSON, cache ownership, filtered profiling, reusable GX rules, successful
and failed quality results, audit error propagation and unchanged source data.
Evidence is in `../validation/profiler-verification-summary.json` and executed
notebooks in `../validation/profiler-notebooks-final/`.

The original landing/release data and existing student ZIP remain unchanged.
