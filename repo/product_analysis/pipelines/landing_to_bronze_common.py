"""Reusable manifest-aware Landing-to-Bronze orchestration."""
from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path

from data_profilers.schema_drift import process_landing_csv


def manifest_dataset(manifest_file, dataset_key, logger):
    """Return the declared metadata for a dataset when a manifest is available."""
    if not manifest_file:
        logger.warning("No source manifest supplied; using runtime source arguments")
        return None
    path = Path(manifest_file)
    if not path.is_file():
        raise FileNotFoundError(f"Source manifest does not exist: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    dataset = manifest.get("datasets", {}).get(dataset_key)
    if dataset is None:
        logger.warning("Manifest has no dataset entry %s", dataset_key)
        return None
    return {"manifest": manifest, "dataset": dataset, "path": path}


def run_landing_to_bronze(*, spark, dataset_name, dataset_key, source_file,
        bronze_path, schema_file, batch_id=None, manifest_file=None,
        audit_table="datalake.operations.schema_drift", logger=None,
        timestamp_false_columns=()):
    """Read exactly the selected CSV with expected types and reconcile Bronze/audit writes."""
    logger = logger or logging.getLogger(__name__)
    source = Path(source_file)
    if not source.is_file():
        raise FileNotFoundError(f"Landing CSV does not exist: {source}")
    if not Path(schema_file).is_file():
        raise FileNotFoundError(f"Schema JSON does not exist: {schema_file}")
    with open(schema_file, encoding="utf-8") as handle:
        type_map = json.load(handle)
    from pyspark.sql import types as T
    spark_types = {"string": T.StringType, "long": T.LongType, "integer": T.IntegerType,
        "int": T.IntegerType, "double": T.DoubleType, "float": T.FloatType,
        "boolean": T.BooleanType, "timestamp": T.TimestampType, "date": T.DateType}
    fields = []
    for name, type_name in type_map.items():
        ctor = spark_types.get(str(type_name).lower())
        if ctor is None:
            raise ValueError(f"Unsupported schema type {type_name!r} for field {name!r}")
        fields.append(T.StructField(name, ctor(), True))
    expected_schema = T.StructType(fields)
    manifest_info = manifest_dataset(manifest_file, dataset_key, logger)
    manifest_rows = None
    manifest_batch = None
    manifest_source_matches = False
    if manifest_info:
        manifest_rows = manifest_info["dataset"].get("rows")
        manifest_batch = manifest_info["manifest"].get("batch_id")
        declared_columns = None
        for item in manifest_info["manifest"].get("files", []):
            if item.get("dataset") == dataset_key and item.get("path", "").lower().endswith(".csv"):
                declared_columns = item.get("columns")
                if Path(item.get("path", "")).name == source.name:
                    manifest_source_matches = True
                break
        if declared_columns and declared_columns != expected_schema.fieldNames():
            raise ValueError(f"Manifest columns do not match {schema_file}: {declared_columns}")
        if batch_id is None and manifest_batch and manifest_source_matches:
            batch_id = manifest_batch
        elif batch_id is None and manifest_batch:
            logger.warning("Manifest path does not identify selected source file; batch_id must be supplied or generated for this source")
        else:
            logger.info("Manifest batch=%s; runtime batch=%s", manifest_batch, batch_id)
    batch_id = batch_id or f"{dataset_name}-{uuid.uuid4().hex[:12]}"
    logger.info("Landing ingestion started dataset=%s batch=%s source=%s", dataset_name, batch_id, source)
    result = process_landing_csv(
        spark, landing_path=str(source.resolve().parent), source_files=[source.resolve().as_uri()],
        bronze_path=bronze_path, expected_schema=expected_schema,
        dataset_name=dataset_name, audit_table=audit_table, batch_id=batch_id,
        reader_options={"mode": "PERMISSIVE", "header": "true", "escape": '"',
            "multiLine": "false", "timestampFormat": "yyyy-MM-dd HH:mm:ss"},
        timestamp_false_columns=timestamp_false_columns,
    )
    actual_count = int(result["source_count"])
    if manifest_rows is not None and int(manifest_rows) != actual_count:
        logger.warning("Manifest row count differs from selected source dataset=%s manifest=%s actual=%s; manifest batch/path may not describe this file", dataset_name, manifest_rows, actual_count)
    logger.info("Landing ingestion completed dataset=%s source=%d good=%d corrupt=%d", dataset_name, result["source_count"], result["good_count"], result["bad_count"])
    return result


def landing_parser(dataset_name, source_default, schema_default):
    import argparse
    parser = argparse.ArgumentParser(description=f"Landing-to-Bronze job for {dataset_name}")
    parser.add_argument("--source-file", default=os.getenv("SOURCE_FILE", str(source_default)))
    default_manifest = Path(__file__).resolve().parents[3] / "data" / "source-manifest.json"
    parser.add_argument("--manifest-file", default=os.getenv("MANIFEST_FILE", str(default_manifest)))
    parser.add_argument("--bronze-path", default=os.getenv("BRONZE_PATH", f"hdfs://localhost:9000/capstone/warehouse/bronze/{dataset_name}"))
    parser.add_argument("--schema-file", default=os.getenv("SCHEMA_FILE", str(schema_default)))
    parser.add_argument("--batch-id", default=os.getenv("BATCH_ID"))
    parser.add_argument("--audit-table", default=os.getenv("LANDING_AUDIT_TABLE", "datalake.operations.schema_drift"))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser


def start_landing_spark(app_name, catalog="datalake"):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", os.getenv("ICEBERG_WAREHOUSE", "hdfs:///capstone/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_DEFAULT_FS", "hdfs://localhost:9000")))
    jar = os.getenv("ICEBERG_SPARK_RUNTIME_JAR")
    if jar:
        builder = builder.config("spark.jars", jar)
    return builder.getOrCreate()


def run_landing_job(dataset_name, dataset_key, default_source, default_schema, argv=None):
    """Shared executable-job lifecycle used by the four dataset wrappers."""
    from pipelines.bronze_to_silver_common import configure_logging
    args = landing_parser(dataset_name, default_source, default_schema).parse_args(argv)
    logger = configure_logging(args.log_level)
    spark = start_landing_spark(f"{dataset_name}_landing_to_bronze")
    try:
        return run_landing_to_bronze(
            spark=spark, dataset_name=dataset_name, dataset_key=dataset_key,
            source_file=args.source_file, bronze_path=args.bronze_path,
            schema_file=args.schema_file, batch_id=args.batch_id,
            manifest_file=args.manifest_file or None, audit_table=args.audit_table,
            logger=logger, timestamp_false_columns=("ops_confirmed_at",) if dataset_name == "sales_orders" else (),
        )
    except Exception:
        logger.exception("Landing-to-Bronze job failed dataset=%s", dataset_name)
        raise
    finally:
        spark.stop()
