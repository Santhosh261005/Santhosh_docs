"""Command-line entry point for the sales-order-lines Bronze-to-Silver job."""
from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.bronze_to_silver_common import configure_logging
from pipelines.sales_order_lines_silver import (
    SalesOrderLinesSilverConfig,
    build_sales_order_lines_batch_processor,
    build_sales_order_lines_bronze_stream,
    create_sales_order_lines_tables,
    load_bronze_business_schema,
)


def _env(name, default):
    return os.environ.get(name, default)


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-name", default=_env("DATASET_NAME", "sales_order_lines"))
    parser.add_argument("--bronze-path", default=_env("BRONZE_PATH", "hdfs:///capstone/warehouse/bronze/sales_order_lines"))
    parser.add_argument("--checkpoint-path", default=_env("CHECKPOINT_PATH", "hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/sales_order_lines"))
    parser.add_argument("--silver-table", default=_env("SILVER_TABLE", "datalake.silver.sales_order_lines"))
    parser.add_argument("--rejected-table", default=_env("REJECTED_TABLE", "datalake.quarantine.sales_order_lines_rejected"))
    parser.add_argument("--audit-table", default=_env("AUDIT_TABLE", "datalake.operations.silver_ingestion_audit"))
    parser.add_argument("--exploration-table", default=_env("EXPLORATION_TABLE", "datalake.operations.explorations"))
    parser.add_argument("--expectation-table", default=_env("EXPECTATION_TABLE", "datalake.operations.expectation_profiles"))
    default_schema = Path(__file__).resolve().parents[1] / "schemas" / "sales_order_lines_schema.json"
    parser.add_argument("--schema-file", default=_env("SCHEMA_FILE", str(default_schema)))
    parser.add_argument("--pipeline-run-id", default=_env("PIPELINE_RUN_ID", f"sales-order-lines-{uuid.uuid4()}"))
    parser.add_argument("--max-files-per-trigger", type=int, default=int(_env("MAX_FILES_PER_TRIGGER", "5")))
    parser.add_argument("--trigger-type", choices=("availableNow",), default=_env("TRIGGER_TYPE", "availableNow"))
    parser.add_argument("--log-level", default=_env("LOG_LEVEL", "INFO"))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_arguments(argv)
    logger = configure_logging(args.log_level)
    config = SalesOrderLinesSilverConfig(
        dataset_name=args.dataset_name, bronze_path=args.bronze_path,
        checkpoint_path=args.checkpoint_path, silver_table=args.silver_table,
        rejected_table=args.rejected_table, audit_table=args.audit_table,
        exploration_table=args.exploration_table, expectation_table=args.expectation_table,
        schema_file=args.schema_file, pipeline_run_id=args.pipeline_run_id,
        max_files_per_trigger=args.max_files_per_trigger, trigger_type=args.trigger_type,
    )
    builder = (SparkSession.builder.appName("sales_order_lines_bronze_to_silver")
        .master(_env("SPARK_MASTER", "local[2]") )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{config.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{config.catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", _env("ICEBERG_WAREHOUSE", "hdfs:///capstone/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", _env("HDFS_DEFAULT_FS", "hdfs://localhost:9000"))
        .config("spark.sql.session.timeZone", "UTC").config("spark.sql.caseSensitive", "true"))
    runtime_jar = os.environ.get("ICEBERG_SPARK_RUNTIME_JAR")
    if runtime_jar:
        builder = builder.config("spark.jars", runtime_jar)
    spark = builder.getOrCreate()
    try:
        business_schema = load_bronze_business_schema(config.schema_file)
        create_sales_order_lines_tables(spark, config)
        stream = build_sales_order_lines_bronze_stream(spark, config, business_schema)
        callback = build_sales_order_lines_batch_processor(config=config, logger=logger)
        query = (stream.writeStream.queryName("sales_order_lines_bronze_to_silver")
            .foreachBatch(callback).option("checkpointLocation", config.checkpoint_path)
            .trigger(availableNow=True).start())
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(query.exception())
        logger.info("Sales-order-lines pipeline completed run_id=%s", config.pipeline_run_id)
    except Exception:
        logger.exception("Sales-order-lines Bronze-to-Silver job failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
