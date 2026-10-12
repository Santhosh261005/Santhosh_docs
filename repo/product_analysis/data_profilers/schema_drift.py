"""Landing-to-Bronze CSV ingestion with schema drift, corrupt-row capture,
HDFS writes, Iceberg audit logging, and reconciliation.

Assumptions
-----------
* Input files are CSV.
* Good records are written to HDFS as CSV, using the explicit expected schema
  when supplied. Framework columns are not included in the Bronze output.
* Corrupt rows and operational events are appended to one Iceberg audit table.
* Schema drift is informational; parser-corrupt rows alone are rejected.
"""

import json
import uuid
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

CORRUPT_COLUMN = "_corrupt_record"
SOURCE_FILE_COLUMN = "_source_file"

AUDIT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("batch_id", T.StringType(), False),
    T.StructField("dataset_name", T.StringType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("source_path", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("target_path", T.StringType(), True),
    T.StructField("expected_schema_json", T.StringType(), True),
    T.StructField("actual_schema_json", T.StringType(), True),
    T.StructField("drift_details_json", T.StringType(), True),
    T.StructField("raw_record", T.StringType(), True),
    T.StructField("reason", T.StringType(), True),
    T.StructField("source_count", T.LongType(), True),
    T.StructField("good_count", T.LongType(), True),
    T.StructField("bad_count", T.LongType(), True),
    T.StructField("written_good_count", T.LongType(), True),
    T.StructField("written_bad_count", T.LongType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), False),
])


def _quoted_table(table_name):
    return ".".join(f"`{part.replace('`', '``')}`" for part in table_name.split("."))


def _hadoop_path(spark, path):
    return spark._jvm.org.apache.hadoop.fs.Path(str(path))


def _path_exists(spark, path):
    hpath = _hadoop_path(spark, path)
    return hpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hpath)


def _list_csv_files(spark, landing_path):
    root = _hadoop_path(spark, landing_path)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(root):
        raise FileNotFoundError(landing_path)

    iterator = fs.listFiles(root, True)
    files = []
    while iterator.hasNext():
        path = iterator.next().getPath()
        name = path.getName()
        if name.lower().endswith(".csv") and not name.startswith(("_", ".")):
            files.append(path.toString())
    return sorted(files)


def _schema_without_framework_columns(schema):
    return T.StructType([
        field for field in schema
        if field.name not in {CORRUPT_COLUMN, SOURCE_FILE_COLUMN}
    ])


def compare_schemas(expected_schema, actual_schema):
    """
    Compare top-level column names and Spark datatypes.

    Compatible type widening is recorded separately and does not cause
    schema drift. For example, an actual integer column is compatible
    with an expected long column.
    """

    expected = {
        field.name: field.dataType.simpleString()
        for field in expected_schema
    }

    actual = {
        field.name: field.dataType.simpleString()
        for field in actual_schema
    }

    added_columns = sorted(
        actual.keys() - expected.keys()
    )

    missing_columns = sorted(
        expected.keys() - actual.keys()
    )

    # actual_type -> expected_type
    compatible_widening = {
        ("tinyint", "smallint"),
        ("tinyint", "int"),
        ("tinyint", "bigint"),
        ("smallint", "int"),
        ("smallint", "bigint"),
        ("int", "bigint"),
        ("float", "double"),
        ("date", "timestamp"),
    }

    incompatible_type_changes = []
    compatible_type_changes = []

    common_columns = sorted(
        expected.keys() & actual.keys()
    )

    for column_name in common_columns:
        expected_type = expected[column_name]
        actual_type = actual[column_name]

        if expected_type == actual_type:
            continue

        type_change = {
            "column": column_name,
            "expected": expected_type,
            "actual": actual_type,
        }

        if (
            actual_type,
            expected_type
        ) in compatible_widening:
            compatible_type_changes.append(
                type_change
            )
        else:
            incompatible_type_changes.append(
                type_change
            )

    drift_detected = bool(
        added_columns
        or missing_columns
        or incompatible_type_changes
    )

    return {
        "drift_detected": drift_detected,
        "added_columns": added_columns,
        "missing_columns": missing_columns,

        # Breaking datatype differences
        "type_changes": incompatible_type_changes,

        # Informational, non-breaking differences
        "compatible_type_changes": compatible_type_changes,
    }


def ensure_audit_table(spark, audit_table):
    """Create the Iceberg namespace/table if absent."""
    parts = audit_table.split(".")
    if len(parts) != 3:
        raise ValueError("audit_table must be catalog.namespace.table")
    catalog, namespace, _ = parts
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")
    empty = spark.createDataFrame([], AUDIT_SCHEMA)
    empty.writeTo(_quoted_table(audit_table)).using("iceberg").createOrReplace() \
        if not spark.catalog.tableExists(audit_table) else None


def append_audit_rows(spark, audit_table, rows):
    if not rows:
        return
    spark.createDataFrame(rows, AUDIT_SCHEMA).writeTo(
        _quoted_table(audit_table)
    ).append()


def _base_event(run_id, batch_id, dataset_name, source_path, target_path):
    return {
        "event_id": str(uuid.uuid4()),
        "run_id": run_id,
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "event_type": "SUMMARY",
        "status": "STARTED",
        "source_path": source_path,
        "source_file": None,
        "target_path": target_path,
        "expected_schema_json": None,
        "actual_schema_json": None,
        "drift_details_json": None,
        "raw_record": None,
        "reason": None,
        "source_count": None,
        "good_count": None,
        "bad_count": None,
        "written_good_count": None,
        "written_bad_count": None,
        "error_message": None,
        "created_at": datetime.utcnow(),
    }


def _read_csv_with_corrupt_capture(spark, file_path, reader_options, expected_schema=None, string_columns=()):
    """Infer the file schema, then reread with an explicit corrupt-record field."""
    options = {
        "header": "true",
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": CORRUPT_COLUMN,
        "inferSchema": "true",
        "samplingRatio": "1.0",
        "multiLine": "false",
        "escape": '"',
    }
    options.update({str(k): str(v) for k, v in (reader_options or {}).items()})
    options["mode"] = "PERMISSIVE"
    options["columnNameOfCorruptRecord"] = CORRUPT_COLUMN

    inferred = spark.read.options(**options).csv(file_path).schema
    inferred = _schema_without_framework_columns(inferred)
    capture_base_schema = expected_schema or inferred
    if expected_schema is not None and string_columns:
        capture_base_schema = T.StructType([
            T.StructField(field.name, T.StringType(), field.nullable, field.metadata)
            if field.name in set(string_columns) else field
            for field in expected_schema.fields
        ])
    capture_schema = T.StructType(
        list(capture_base_schema.fields)
        + [T.StructField(CORRUPT_COLUMN, T.StringType(), True)]
    )

    # Disabling CSV column pruning ensures Spark materializes malformed-row data
    # even when a later query selects only a subset of columns.
    spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
    frame = spark.read.schema(capture_schema).options(**options).csv(file_path)
    return frame, inferred


def process_landing_csv(
    spark,
    *,
    landing_path,
    bronze_path,
    expected_schema,
    dataset_name,
    audit_table="datalake.operations.schema_drift",
    batch_id=None,
    reader_options=None,
    write_mode="append",
    expected_manifest_rows=None,
    source_files=None,
    timestamp_false_columns=(),
):
    """Process every CSV file under a landing path.

    Returns a batch summary dictionary. Schema drift is logged but does not, by
    itself, reject a row. Rows where `_corrupt_record` is non-null are written
    to the Iceberg audit table; all other rows are written to HDFS as CSV.
    """
    if not isinstance(expected_schema, T.StructType):
        raise TypeError("expected_schema must be pyspark.sql.types.StructType")
    if write_mode not in {"append", "overwrite"}:
        raise ValueError("write_mode must be 'append' or 'overwrite'")

    ensure_audit_table(spark, audit_table)
    files = list(source_files) if source_files is not None else _list_csv_files(spark, landing_path)
    if source_files is not None:
        for source_file in files:
            if not _path_exists(spark, source_file):
                raise FileNotFoundError(source_file)
    if not files:
        raise ValueError(f"No CSV files found under {landing_path}")

    run_id = str(uuid.uuid4())
    batch_id = batch_id or f"{dataset_name}_{uuid.uuid4().hex}"
    batch_source = batch_good = batch_bad = 0
    file_summaries = []

    for index, file_path in enumerate(files):
        output_path = f"{bronze_path.rstrip('/')}/batch_id={batch_id}/file_{index:05d}"
        base = _base_event(
            run_id, batch_id, dataset_name, landing_path, output_path
        )
        base["source_file"] = file_path
        base["expected_schema_json"] = expected_schema.json()
        frame = None

        try:
            frame, inferred_schema = _read_csv_with_corrupt_capture(
                spark, file_path, reader_options, expected_schema=expected_schema,
                string_columns=timestamp_false_columns,
            )
            frame = frame.withColumn(SOURCE_FILE_COLUMN, F.input_file_name())
            for column in timestamp_false_columns:
                raw_value = F.trim(F.col(column))
                frame = frame.withColumn(
                    column,
                    F.when(F.lower(raw_value).isin("false", "null", "none", ""), F.lit(None).cast("timestamp"))
                    .otherwise(F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")),
                )
            frame.persist(StorageLevel.MEMORY_AND_DISK)

            source_count = frame.count()
            actual_schema = inferred_schema
            drift = compare_schemas(expected_schema, actual_schema)

            good = frame.where(F.col(CORRUPT_COLUMN).isNull()).drop(
                CORRUPT_COLUMN, SOURCE_FILE_COLUMN
            )
            bad = frame.where(F.col(CORRUPT_COLUMN).isNotNull())
            good_count = good.count()
            bad_count = bad.count()

            drift_event = dict(base)
            drift_event.update(
                event_id=str(uuid.uuid4()),
                event_type="SCHEMA_DRIFT",
                status="DRIFT_FOUND" if drift["drift_detected"] else "SCHEMA_MATCH",
                actual_schema_json=actual_schema.json(),
                drift_details_json=json.dumps(drift, sort_keys=True),
                source_count=source_count,
                good_count=good_count,
                bad_count=bad_count,
                created_at=datetime.utcnow(),
            )
            append_audit_rows(spark, audit_table, [drift_event])

            # Write each rejected raw record directly from Spark to Iceberg.
            # This avoids collecting malformed records into Python driver memory.
            corrupt_audit = bad.select(
                F.expr("uuid()").alias("event_id"),
                F.lit(run_id).alias("run_id"),
                F.lit(batch_id).alias("batch_id"),
                F.lit(dataset_name).alias("dataset_name"),
                F.lit("CORRUPT_RECORD").alias("event_type"),
                F.lit("PARSER_REJECTED").alias("status"),
                F.lit(landing_path).alias("source_path"),
                F.col(SOURCE_FILE_COLUMN).alias("source_file"),
                F.lit(output_path).alias("target_path"),
                F.lit(expected_schema.json()).alias("expected_schema_json"),
                F.lit(actual_schema.json()).alias("actual_schema_json"),
                F.lit(None).cast("string").alias("drift_details_json"),
                F.col(CORRUPT_COLUMN).alias("raw_record"),
                F.lit(
                    "Spark CSV parser rejected this record; exact parser reason is unavailable"
                ).alias("reason"),
                F.lit(None).cast("long").alias("source_count"),
                F.lit(None).cast("long").alias("good_count"),
                F.lit(None).cast("long").alias("bad_count"),
                F.lit(None).cast("long").alias("written_good_count"),
                F.lit(None).cast("long").alias("written_bad_count"),
                F.lit(None).cast("string").alias("error_message"),
                F.current_timestamp().alias("created_at"),
            )
            if bad_count:
                corrupt_audit.writeTo(_quoted_table(audit_table)).append()

            if good_count:
                good_to_write = (
                    good
                    .withColumn(
                        "write_date_partition",
                        F.to_date(
                            F.to_timestamp(
                                F.col("write_date"),
                                "yyyy-MM-dd HH:mm:ss"
                            )
                        )
                    )
                )
                invalid_partition_count = (
                    good_to_write
                    .filter(
                        F.col("write_date_partition").isNull()
                    )
                    .count()
                )
                if invalid_partition_count > 0:
                    raise ValueError(
                        f"{invalid_partition_count} good records have an "
                        "invalid or null write_date and cannot be "
                        "partitioned safely."
                    )
                (
                    good_to_write.write
                    .mode(write_mode)
                    .option("header", "true")
                    .option("quote", '"')
                    .option("escape", '"')
                    .partitionBy("write_date_partition")
                    .csv(output_path)
                )

            written_good_count = (
                spark.read
                .option("header", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(output_path)
                .count()
                if good_count and _path_exists(
                    spark,
                    output_path
                )
                else 0
            )
            written_bad_count = (
                spark.table(audit_table)
                .where(
                    (F.col("run_id") == run_id)
                    & (F.col("batch_id") == batch_id)
                    & (F.col("target_path") == output_path)
                    & (F.col("event_type") == "CORRUPT_RECORD")
                )
                .count()
            )

            reconciled = (
                source_count == good_count + bad_count
                and written_good_count == good_count
                and written_bad_count == bad_count
            )
            reconciliation = dict(base)
            reconciliation.update(
                event_id=str(uuid.uuid4()),
                event_type="RECONCILIATION",
                status="RECONCILED" if reconciled else "FAILED",
                actual_schema_json=actual_schema.json(),
                drift_details_json=json.dumps(drift, sort_keys=True),
                source_count=source_count,
                good_count=good_count,
                bad_count=bad_count,
                written_good_count=written_good_count,
                written_bad_count=written_bad_count,
                error_message=None if reconciled else "Persisted counts do not match the source split",
                created_at=datetime.utcnow(),
            )
            append_audit_rows(spark, audit_table, [reconciliation])

            if expected_manifest_rows is not None and len(files) == 1 and source_count != int(expected_manifest_rows):
                raise ValueError(
                    f"Manifest/source row count mismatch for {file_path}: "
                    f"manifest={expected_manifest_rows}, actual={source_count}"
                )

            batch_source += source_count
            batch_good += good_count
            batch_bad += bad_count
            file_summaries.append({
                "source_file": file_path,
                "source_count": source_count,
                "good_count": good_count,
                "bad_count": bad_count,
                "drift_detected": drift["drift_detected"],
                "reconciliation_status": reconciliation["status"],
            })

        except Exception as exc:
            failed = dict(base)
            failed.update(
                event_id=str(uuid.uuid4()),
                event_type="PROCESSING_ERROR",
                status="FAILED",
                error_message=f"{type(exc).__name__}: {exc}",
                created_at=datetime.utcnow(),
            )
            append_audit_rows(spark, audit_table, [failed])
            raise
        finally:
            if frame is not None:
                frame.unpersist()

    batch_ok = all(
        item["reconciliation_status"] == "RECONCILED"
        for item in file_summaries
    )
    summary = _base_event(
        run_id, batch_id, dataset_name, landing_path, bronze_path
    )
    summary.update(
        event_id=str(uuid.uuid4()),
        event_type="BATCH_SUMMARY",
        status="SUCCESS" if batch_ok else "FAILED",
        expected_schema_json=expected_schema.json(),
        source_count=batch_source,
        good_count=batch_good,
        bad_count=batch_bad,
        written_good_count=batch_good if batch_ok else None,
        written_bad_count=batch_bad if batch_ok else None,
        created_at=datetime.utcnow(),
    )
    append_audit_rows(spark, audit_table, [summary])

    return {
        "run_id": run_id,
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "status": summary["status"],
        "source_count": batch_source,
        "good_count": batch_good,
        "bad_count": batch_bad,
        "files": file_summaries,
    }
