"""Dataset-independent helpers for Iceberg Bronze-to-Silver pipelines."""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from pyspark.sql import functions as F, types as T


def configure_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger("bronze_to_silver")


def extract_hive_path_value(path_column: str, key: str):
    """Return a Spark expression extracting ``key=value`` from a file path."""
    return F.regexp_extract(F.col(path_column), rf"(?:^|/){re.escape(key)}=([^/]+)", 1)


def build_source_lineage_columns(df):
    """Attach the source filename and path-derived Bronze partition metadata."""
    return (
        df.withColumn("_bronze_source_file", F.input_file_name())
        .withColumn("batch_id", extract_hive_path_value("_bronze_source_file", "batch_id"))
        .withColumn(
            "write_date_partition",
            F.to_date(extract_hive_path_value("_bronze_source_file", "write_date_partition"), "yyyy-MM-dd"),
        )
    )


def validate_dataframe_columns(df, expected_columns: Sequence[str], *, context: str = "DataFrame") -> None:
    if list(df.columns) != list(expected_columns):
        raise ValueError(f"{context} columns mismatch: expected {list(expected_columns)}, got {list(df.columns)}")


def quote_iceberg_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError(f"Invalid Iceberg table identifier: {table_name!r}")
    return ".".join(f"`{p}`" for p in parts)


def validate_table_exists(spark, table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required Iceberg table does not exist: {table_name}")


def write_rejected_records(
    rejected_df,
    *,
    rejected_table: str,
    expected_columns: Sequence[str] | None = None,
    rejected_count: int | None = None,
    logger: logging.Logger | None = None,
) -> int:
    logger = logger or logging.getLogger(__name__)
    count = rejected_df.count() if rejected_count is None else int(rejected_count)
    if count == 0:
        logger.info("No rejected records; quarantine write skipped table=%s", rejected_table)
        return 0
    if expected_columns is not None:
        validate_dataframe_columns(rejected_df, expected_columns, context="Rejected DataFrame")
    rejected_df.writeTo(rejected_table).append()
    logger.info("Rejected records appended table=%s count=%d", rejected_table, count)
    return count


def create_silver_audit_row(
    *, dataset_name: str, processing_stage: str = "BRONZE_TO_SILVER",
    pipeline_run_id: str, stream_batch_id: int, status: str,
    source_count: int | None = None, valid_before_dedup_count: int | None = None,
    rejected_count: int | None = None, duplicate_count: int | None = None,
    valid_after_dedup_count: int | None = None,
    incoming_keys_found_in_silver: int | None = None,
    reconciliation_status: str | None = None, exploration_status: str | None = None,
    expectation_status: str | None = None, error_message: str | None = None,
    started_at: datetime | None = None, completed_at: datetime | None = None,
) -> dict[str, Any]:
    def as_int(value):
        return None if value is None else int(value)

    return {
        "audit_id": str(uuid.uuid4()), "dataset_name": dataset_name,
        "processing_stage": processing_stage, "pipeline_run_id": pipeline_run_id,
        "stream_batch_id": int(stream_batch_id), "status": status,
        "source_count": as_int(source_count),
        "valid_before_dedup_count": as_int(valid_before_dedup_count),
        "rejected_count": as_int(rejected_count), "duplicate_count": as_int(duplicate_count),
        "valid_after_dedup_count": as_int(valid_after_dedup_count),
        "incoming_keys_found_in_silver": as_int(incoming_keys_found_in_silver),
        "reconciliation_status": reconciliation_status,
        "exploration_status": exploration_status, "expectation_status": expectation_status,
        "error_message": error_message, "started_at": started_at,
        "completed_at": completed_at or datetime.now(timezone.utc),
    }


def write_silver_audit(spark, audit_row: Mapping[str, Any], *, audit_table: str, audit_schema, logger=None) -> str:
    logger = logger or logging.getLogger(__name__)
    audit_id = str(audit_row["audit_id"])
    try:
        spark.createDataFrame([dict(audit_row)], schema=audit_schema).writeTo(audit_table).append()
        logger.info("Silver audit appended table=%s audit_id=%s", audit_table, audit_id)
        return audit_id
    except Exception:
        logger.exception("Silver audit write failed table=%s audit_id=%s", audit_table, audit_id)
        raise


def evaluate_micro_batch_reconciliation(*, source_count, valid_before_dedup_count, rejected_count,
                                        duplicate_count, valid_after_dedup_count,
                                        incoming_keys_found_in_silver):
    checks = {
        "source_split_passed": int(source_count) == int(valid_before_dedup_count) + int(rejected_count),
        "deduplication_passed": int(valid_before_dedup_count) == int(valid_after_dedup_count) + int(duplicate_count),
        "silver_merge_passed": int(valid_after_dedup_count) == int(incoming_keys_found_in_silver),
    }
    checks["reconciliation_status"] = "RECONCILED" if all(checks.values()) else "FAILED"
    return checks


def count_incoming_keys_found_in_silver(spark_session, incoming_df, *, silver_table, key_column):
    incoming = incoming_df.select(key_column).where(F.col(key_column).isNotNull()).distinct().alias("incoming")
    target = spark_session.table(silver_table).select(key_column).where(F.col(key_column).isNotNull()).distinct().alias("target")
    return incoming.join(target, on=key_column, how="left_semi").count()


def count_incoming_versions_satisfied_in_silver(spark_session, incoming_df, *, silver_table, key_column, version_column):
    incoming = incoming_df.select(key_column, version_column).alias("incoming")
    target = spark_session.table(silver_table).select(key_column, version_column).alias("target")
    return incoming.join(
        target,
        (F.col(f"incoming.{key_column}") == F.col(f"target.{key_column}"))
        & (F.col(f"target.{version_column}") >= F.col(f"incoming.{version_column}")),
        "left_semi",
    ).count()


def audit_schema():
    return T.StructType([
        T.StructField("audit_id", T.StringType(), False), T.StructField("dataset_name", T.StringType(), False),
        T.StructField("processing_stage", T.StringType(), False), T.StructField("pipeline_run_id", T.StringType(), False),
        T.StructField("stream_batch_id", T.LongType(), False), T.StructField("status", T.StringType(), False),
        *[T.StructField(c, T.LongType(), True) for c in ("source_count", "valid_before_dedup_count", "rejected_count", "duplicate_count", "valid_after_dedup_count", "incoming_keys_found_in_silver")],
        *[T.StructField(c, T.StringType(), True) for c in ("reconciliation_status", "exploration_status", "expectation_status", "error_message")],
        T.StructField("started_at", T.TimestampType(), True), T.StructField("completed_at", T.TimestampType(), True),
    ])
