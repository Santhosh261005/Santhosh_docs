# Profiler function signatures

Reference signatures; imports and usage examples are in README.md.

## schema_drift

```python
def profile_corrupt_records(
    df,
    *,
    corrupt_column="_corrupt_record",
    source_file_column="_source_file",
    reason_column=None,
    filter_condition=None,
    dataset_name=None,
    run_id=None,
    audit_table="datalake.operations.corrupt_records"
): ...
def profile_schema_drift(
    spark,
    input_path,
    expected_schema,
    *,
    source_format="jsonl",
    reader_options=None,
    dataset_name=None,
    run_id=None,
    audit_table="datalake.operations.schema_drift_profiles"
): ...
```

## exploration

```python
def explore_data(
    spark,
    dataframe_or_table,
    dataset_name=None,
    run_id=None,
    batch_id=None,
    profile_scope="BATCH",
    processing_stage="LANDING",
    profile_level="FULL",
    required_columns=None,
    source_file_column=None,
    columns=None,
    approximate_distinct=True,
    relative_sd=0.05,
): ...
def write_exploration_to_iceberg(
    spark,
    exploration_results,
    catalog_name="datalake",
    namespace_name="operations",
    table_name="explorations",
): ...
```

## great_expectations

```python
def profile_expectations(
    df,
    expectations,
    *,
    dataset_name=None,
    run_id=None,
    audit_table="datalake.operations.expectation_profiles"
): ...
```
