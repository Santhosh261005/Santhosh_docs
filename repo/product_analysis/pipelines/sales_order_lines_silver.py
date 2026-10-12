"""Sales-order-lines-specific Bronze-to-Silver transformations."""
from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from pipelines.bronze_to_silver_common import (
    build_source_lineage_columns,
    count_incoming_keys_found_in_silver,
    count_incoming_versions_satisfied_in_silver,
    create_silver_audit_row,
    evaluate_micro_batch_reconciliation,
    write_rejected_records,
    write_silver_audit,
)


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
    max_files_per_trigger: int = 5
    trigger_type: str = "availableNow"
    catalog: str = "datalake"


silver_schema = T.StructType([
    T.StructField("sales_order_line_id", T.LongType(), False),
    T.StructField("sales_order_id", T.LongType(), False),
    T.StructField("sales_order_name", T.StringType(), True),
    T.StructField("product_id", T.LongType(), False),
    T.StructField("product_display_name", T.StringType(), True),
    *[T.StructField(c, T.DoubleType(), False) for c in ("product_uom_qty", "price_unit", "discount", "price_subtotal")],
    *[T.StructField(c, T.TimestampType(), False) for c in ("create_date", "write_date")],
    *[T.StructField(c, T.DoubleType(), False) for c in ("qty_delivered", "qty_invoiced")],
    T.StructField("source_batch_id", T.StringType(), False),
    T.StructField("source_file", T.StringType(), False),
    T.StructField("stream_batch_id", T.LongType(), False),
    T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("silver_created_at", T.TimestampType(), False),
    T.StructField("silver_updated_at", T.TimestampType(), False),
])

rejected_schema = T.StructType([
    T.StructField("id", T.LongType(), True),
    T.StructField("order_id_raw", T.StringType(), True),
    T.StructField("product_id_raw", T.StringType(), True),
    *[T.StructField(c, T.DoubleType(), True) for c in ("product_uom_qty", "price_unit", "discount", "price_subtotal")],
    *[T.StructField(c, T.TimestampType(), True) for c in ("create_date", "write_date")],
    *[T.StructField(c, T.DoubleType(), True) for c in ("qty_delivered", "qty_invoiced")],
    T.StructField("parsed_sales_order_id", T.LongType(), True),
    T.StructField("parsed_sales_order_name", T.StringType(), True),
    T.StructField("parsed_product_id", T.LongType(), True),
    T.StructField("parsed_product_display_name", T.StringType(), True),
    T.StructField("rejection_reasons", T.ArrayType(T.StringType(), False), False),
    T.StructField("failed_rule_names", T.ArrayType(T.StringType(), False), False),
    T.StructField("source_batch_id", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("stream_batch_id", T.LongType(), False),
    T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("rejected_at", T.TimestampType(), False),
])

silver_audit_schema = T.StructType([
    T.StructField("audit_id", T.StringType(), False), T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("stream_batch_id", T.LongType(), False), T.StructField("dataset_name", T.StringType(), False),
    T.StructField("processing_stage", T.StringType(), False), T.StructField("status", T.StringType(), False),
    *[T.StructField(c, T.LongType(), False) for c in ("source_count", "valid_before_dedup_count", "rejected_count", "duplicate_count", "valid_after_dedup_count")],
    T.StructField("incoming_keys_found_in_silver", T.LongType(), True), T.StructField("reconciliation_status", T.StringType(), False),
    T.StructField("exploration_status", T.StringType(), True), T.StructField("expectation_status", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True), T.StructField("started_at", T.TimestampType(), False), T.StructField("completed_at", T.TimestampType(), True),
])


def load_bronze_business_schema(schema_file):
    import json
    mapping = {"string": T.StringType, "integer": T.IntegerType, "int": T.IntegerType, "long": T.LongType,
               "double": T.DoubleType, "float": T.FloatType, "boolean": T.BooleanType,
               "date": T.DateType, "timestamp": T.TimestampType}
    with open(schema_file, encoding="utf-8") as f:
        definition = json.load(f)
    fields = []
    for name, kind in definition.items():
        constructor = mapping.get(str(kind).strip().lower())
        if constructor is None:
            raise ValueError(f"Unsupported datatype {kind!r} for {name!r}")
        fields.append(T.StructField(name, constructor(), True))
    schema = T.StructType(fields)
    required = {"id", "order_id", "product_id", "product_uom_qty", "price_unit", "discount", "price_subtotal", "create_date", "write_date", "qty_delivered", "qty_invoiced"}
    missing = required - set(schema.fieldNames())
    if missing:
        raise ValueError(f"Required source columns missing: {sorted(missing)}")
    return schema


def transform_and_validate_sales_order_lines(batch_df, *, stream_batch_id, pipeline_run_id, processed_at=None):
    """Parse Odoo relation arrays and attach row-level validation results."""
    df = batch_df.withColumnRenamed("order_id", "order_id_raw").withColumnRenamed("product_id", "product_id_raw")
    df = (df.withColumn("parsed_sales_order_id", F.get_json_object("order_id_raw", "$[0]").cast("long"))
        .withColumn("parsed_sales_order_name", F.get_json_object("order_id_raw", "$[1]"))
        .withColumn("parsed_product_id", F.get_json_object("product_id_raw", "$[0]").cast("long"))
        .withColumn("parsed_product_display_name", F.get_json_object("product_id_raw", "$[1]")))
    rules = [
        ("ID_NOT_NULL", "id must not be null", F.col("id").isNull()),
        ("ORDER_ID_NOT_NULL", "order_id must not be null", F.col("order_id_raw").isNull()),
        ("ORDER_ID_PARSEABLE", "order_id must contain a valid numeric Odoo relation ID", F.col("order_id_raw").isNotNull() & F.col("parsed_sales_order_id").isNull()),
        ("PRODUCT_ID_NOT_NULL", "product_id must not be null", F.col("product_id_raw").isNull()),
        ("PRODUCT_ID_PARSEABLE", "product_id must contain a valid numeric Odoo relation ID", F.col("product_id_raw").isNotNull() & F.col("parsed_product_id").isNull()),
        *[(f"{c.upper()}_NOT_NULL", f"{c} must not be null", F.col(c).isNull()) for c in ("product_uom_qty", "price_unit", "discount", "price_subtotal", "create_date", "write_date", "qty_delivered", "qty_invoiced")],
        ("DISCOUNT_VALID_RANGE", "discount must be between 0 and 100", F.col("discount").isNotNull() & ((F.col("discount") < 0) | (F.col("discount") > 100))),
    ]
    df = (df.withColumn("failed_rule_names", F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, _, cond in rules])))
        .withColumn("rejection_reasons", F.array_compact(F.array(*[F.when(cond, F.lit(reason)) for _, reason, cond in rules])))
        .withColumn("record_status", F.when(F.size("failed_rule_names") == 0, "VALID").otherwise("REJECTED"))
        .withColumn("stream_batch_id", F.lit(int(stream_batch_id)).cast("long"))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("processed_at", F.lit(processed_at).cast("timestamp") if processed_at is not None else F.current_timestamp()))
    return df


def create_valid_silver_df(df):
    return df.filter(F.col("record_status") == "VALID").select(
        F.col("id").cast("long").alias("sales_order_line_id"), F.col("parsed_sales_order_id").cast("long").alias("sales_order_id"),
        F.col("parsed_sales_order_name").cast("string").alias("sales_order_name"), F.col("parsed_product_id").cast("long").alias("product_id"),
        F.col("parsed_product_display_name").cast("string").alias("product_display_name"), *[F.col(c).cast("double").alias(c) for c in ("product_uom_qty", "price_unit", "discount", "price_subtotal")],
        *[F.col(c).cast("timestamp").alias(c) for c in ("create_date", "write_date")], *[F.col(c).cast("double").alias(c) for c in ("qty_delivered", "qty_invoiced")],
        F.col("batch_id").cast("string").alias("source_batch_id"), F.col("_bronze_source_file").cast("string").alias("source_file"),
        F.col("stream_batch_id").cast("long"), F.col("pipeline_run_id").cast("string"),
        F.col("processed_at").cast("timestamp").alias("silver_created_at"), F.col("processed_at").cast("timestamp").alias("silver_updated_at"))


def create_rejected_records_df(df):
    columns = ["id", "order_id_raw", "product_id_raw", "product_uom_qty", "price_unit", "discount", "price_subtotal", "create_date", "write_date", "qty_delivered", "qty_invoiced", "parsed_sales_order_id", "parsed_sales_order_name", "parsed_product_id", "parsed_product_display_name", "rejection_reasons", "failed_rule_names"]
    return (df.filter(F.col("record_status") == "REJECTED").select(*columns,
        F.col("batch_id").cast("string").alias("source_batch_id"), F.col("_bronze_source_file").cast("string").alias("source_file"),
        F.col("stream_batch_id").cast("long"), F.col("pipeline_run_id").cast("string"), F.col("processed_at").cast("timestamp").alias("rejected_at")))


def deduplicate_valid_records(valid_df):
    w = Window.partitionBy("sales_order_line_id").orderBy(F.col("write_date").desc(), F.col("source_batch_id").desc(), F.col("source_file").desc())
    ranked = valid_df.withColumn("_rank", F.row_number().over(w))
    return ranked.filter(F.col("_rank") == 1).drop("_rank"), ranked.filter(F.col("_rank") > 1).drop("_rank")


def merge_sales_order_lines_to_silver(spark, source_df, *, config, stream_batch_id, logger):
    """Stage a fixed micro-batch in Iceberg, then merge deterministically."""
    suffix = re.sub(r"[^A-Za-z0-9_]", "_", f"{config.pipeline_run_id}_{stream_batch_id}")
    staging = f"{config.catalog}.operations.sales_order_lines_staging_{suffix}"
    if source_df.filter(F.col("sales_order_line_id").isNull()).limit(1).count():
        raise ValueError("MERGE source contains a null sales_order_line_id")
    if source_df.groupBy("sales_order_line_id").count().filter("count > 1").limit(1).count():
        raise ValueError("MERGE source contains duplicate sales_order_line_id values")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    try:
        source_df.writeTo(staging).using("iceberg").create()
        staged_count = spark.table(staging).count()
        expected_count = source_df.count()
        if staged_count != expected_count:
            raise ValueError(f"Staging reconciliation failed: expected {expected_count}, got {staged_count}")
        spark.sql(f"""MERGE INTO {config.silver_table} target USING {staging} source
          ON target.sales_order_line_id = source.sales_order_line_id
          WHEN MATCHED AND source.write_date > target.write_date THEN UPDATE SET
          target.sales_order_id=source.sales_order_id, target.sales_order_name=source.sales_order_name,
          target.product_id=source.product_id, target.product_display_name=source.product_display_name,
          target.product_uom_qty=source.product_uom_qty, target.price_unit=source.price_unit,
          target.discount=source.discount, target.price_subtotal=source.price_subtotal,
          target.create_date=source.create_date, target.write_date=source.write_date,
          target.qty_delivered=source.qty_delivered, target.qty_invoiced=source.qty_invoiced,
          target.source_batch_id=source.source_batch_id, target.source_file=source.source_file,
          target.stream_batch_id=source.stream_batch_id, target.pipeline_run_id=source.pipeline_run_id,
          target.silver_updated_at=source.silver_updated_at
          WHEN NOT MATCHED THEN INSERT (
            sales_order_line_id, sales_order_id, sales_order_name, product_id, product_display_name,
            product_uom_qty, price_unit, discount, price_subtotal, create_date, write_date,
            qty_delivered, qty_invoiced, source_batch_id, source_file, stream_batch_id,
            pipeline_run_id, silver_created_at, silver_updated_at
          ) VALUES (
            source.sales_order_line_id, source.sales_order_id, source.sales_order_name, source.product_id,
            source.product_display_name, source.product_uom_qty, source.price_unit, source.discount,
            source.price_subtotal, source.create_date, source.write_date, source.qty_delivered,
            source.qty_invoiced, source.source_batch_id, source.source_file, source.stream_batch_id,
            source.pipeline_run_id, source.silver_created_at, source.silver_updated_at
          )""")
        spark.catalog.refreshTable(config.silver_table)
        logger.info("Silver MERGE committed from staging table %s", staging)
    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {staging}")
        except Exception:
            logger.exception("Unable to remove staging table %s", staging)


def create_sales_order_lines_tables(spark, config):
    """Create the three dataset tables, preserving the original partition contracts."""
    for ns in ("silver", "quarantine", "operations"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.{ns}")
    # Detailed table contracts use the schema above and original Bronze-to-Silver DDL.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.silver_table} (
      sales_order_line_id BIGINT NOT NULL, sales_order_id BIGINT NOT NULL, sales_order_name STRING,
      product_id BIGINT NOT NULL, product_display_name STRING, product_uom_qty DOUBLE NOT NULL,
      price_unit DOUBLE NOT NULL, discount DOUBLE NOT NULL, price_subtotal DOUBLE NOT NULL,
      create_date TIMESTAMP NOT NULL, write_date TIMESTAMP NOT NULL, qty_delivered DOUBLE NOT NULL,
      qty_invoiced DOUBLE NOT NULL, source_batch_id STRING NOT NULL, source_file STRING NOT NULL,
      stream_batch_id BIGINT NOT NULL, pipeline_run_id STRING NOT NULL, silver_created_at TIMESTAMP NOT NULL,
      silver_updated_at TIMESTAMP NOT NULL) USING iceberg PARTITIONED BY (days(write_date)) TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.rejected_table} (
      id BIGINT, order_id_raw STRING, product_id_raw STRING, product_uom_qty DOUBLE, price_unit DOUBLE,
      discount DOUBLE, price_subtotal DOUBLE, create_date TIMESTAMP, write_date TIMESTAMP, qty_delivered DOUBLE,
      qty_invoiced DOUBLE, parsed_sales_order_id BIGINT, parsed_sales_order_name STRING, parsed_product_id BIGINT,
      parsed_product_display_name STRING, rejection_reasons ARRAY<STRING> NOT NULL, failed_rule_names ARRAY<STRING> NOT NULL,
      source_batch_id STRING, source_file STRING, stream_batch_id BIGINT NOT NULL, pipeline_run_id STRING NOT NULL,
      rejected_at TIMESTAMP NOT NULL) USING iceberg PARTITIONED BY (days(rejected_at)) TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.audit_table} (
      audit_id STRING NOT NULL, pipeline_run_id STRING NOT NULL, stream_batch_id BIGINT NOT NULL,
      dataset_name STRING NOT NULL, processing_stage STRING NOT NULL, status STRING NOT NULL,
      source_count BIGINT NOT NULL, valid_before_dedup_count BIGINT NOT NULL, rejected_count BIGINT NOT NULL,
      duplicate_count BIGINT NOT NULL, valid_after_dedup_count BIGINT NOT NULL,
      incoming_keys_found_in_silver BIGINT, reconciliation_status STRING NOT NULL,
      exploration_status STRING, expectation_status STRING, error_message STRING,
      started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP) USING iceberg PARTITIONED BY (days(started_at)) TBLPROPERTIES ('format-version'='2')""")


def build_sales_order_lines_bronze_stream(spark, config, business_schema):
    return (spark.readStream.format("csv").schema(business_schema).option("header", "true")
        .option("quote", '"').option("escape", '"').option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        .option("recursiveFileLookup", "true").option("multiLine", "false")
        .option("maxFilesPerTrigger", config.max_files_per_trigger).load(config.bronze_path))


def build_sales_order_lines_expectations():
    import great_expectations as gx
    checks = [gx.expectations.ExpectColumnValuesToNotBeNull(column=c) for c in
        ("id", "order_id", "product_id", "product_uom_qty", "price_unit", "price_subtotal", "qty_delivered", "qty_invoiced", "discount", "create_date", "write_date")]
    checks.append(gx.expectations.ExpectColumnValuesToBeBetween(column="discount", min_value=0.0, max_value=100.0))
    return checks


def build_sales_order_lines_batch_processor(*, config, logger):
    """Bind configuration and profiler integrations into a foreachBatch callback."""
    from data_profilers.exploration import explore_data, write_exploration_to_iceberg
    from data_profilers.great_expectations import profile_expectations
    expectations = build_sales_order_lines_expectations()
    exploration_columns = ["id", "order_id", "product_id", "product_uom_qty", "price_unit",
        "discount", "price_subtotal", "create_date", "write_date", "qty_delivered", "qty_invoiced"]
    def process_sales_order_lines_batch(batch_df, stream_batch_id):
        spark = batch_df.sparkSession
        stream_batch_id = int(stream_batch_id)
        started = datetime.utcnow()
        processed_at = started
        owned = []
        counts = dict(source_count=0, valid_before_dedup_count=0, rejected_count=0, duplicate_count=0, valid_after_dedup_count=0, incoming_keys_found_in_silver=None)
        exploration_status = expectation_status = "SKIPPED"
        reconciliation = "NOT_EVALUATED"
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            counts["source_count"] = batch_df.count()
            if counts["source_count"] == 0:
                return
            enriched = build_source_lineage_columns(batch_df)
            run_id = f"{config.pipeline_run_id}-batch-{stream_batch_id}"
            try:
                measures = explore_data(spark, enriched, dataset_name=config.dataset_name, run_id=run_id,
                    batch_id=str(stream_batch_id), profile_scope="BATCH", processing_stage="BRONZE", profile_level="FULL",
                    columns=exploration_columns, required_columns=exploration_columns,
                    approximate_distinct=True, relative_sd=0.05)
                write_exploration_to_iceberg(spark, measures, catalog_name=config.catalog, namespace_name="operations", table_name="explorations")
                exploration_status = "COMPLETED"
            except Exception:
                exploration_status = "FAILED"
                logger.exception("Exploration profiler failed; continuing")
            try:
                profile_expectations(enriched, expectations, dataset_name=config.dataset_name, run_id=run_id, audit_table=config.expectation_table)
                expectation_status = "COMPLETED"
            except Exception:
                expectation_status = "FAILED"
                logger.exception("GX profiler failed; continuing")
            transformed = transform_and_validate_sales_order_lines(enriched, stream_batch_id=stream_batch_id, pipeline_run_id=config.pipeline_run_id, processed_at=processed_at).persist(StorageLevel.MEMORY_AND_DISK)
            owned.append(transformed)
            if transformed.count() != counts["source_count"]:
                raise ValueError("Transformation changed record count")
            valid = create_valid_silver_df(transformed).persist(StorageLevel.MEMORY_AND_DISK); owned.append(valid)
            rejected = create_rejected_records_df(transformed).persist(StorageLevel.MEMORY_AND_DISK); owned.append(rejected)
            counts["valid_before_dedup_count"], counts["rejected_count"] = valid.count(), rejected.count()
            if counts["source_count"] != counts["valid_before_dedup_count"] + counts["rejected_count"]:
                raise ValueError("Source split reconciliation failed")
            write_rejected_records(rejected, rejected_table=config.rejected_table, expected_columns=[f.name for f in rejected_schema.fields], rejected_count=counts["rejected_count"], logger=logger)
            deduped, duplicates = deduplicate_valid_records(valid)
            deduped = deduped.persist(StorageLevel.MEMORY_AND_DISK); owned.append(deduped)
            duplicates = duplicates.persist(StorageLevel.MEMORY_AND_DISK); owned.append(duplicates)
            counts["valid_after_dedup_count"], counts["duplicate_count"] = deduped.count(), duplicates.count()
            if counts["valid_before_dedup_count"] != counts["valid_after_dedup_count"] + counts["duplicate_count"]:
                raise ValueError("Deduplication reconciliation failed")
            if counts["valid_after_dedup_count"]:
                merge_sales_order_lines_to_silver(spark, deduped, config=config, stream_batch_id=stream_batch_id, logger=logger)
                counts["incoming_keys_found_in_silver"] = count_incoming_keys_found_in_silver(spark, deduped, silver_table=config.silver_table, key_column="sales_order_line_id")
                versions = count_incoming_versions_satisfied_in_silver(spark, deduped, silver_table=config.silver_table, key_column="sales_order_line_id", version_column="write_date")
            else:
                counts["incoming_keys_found_in_silver"], versions = 0, 0
            result = evaluate_micro_batch_reconciliation(**counts)
            reconciliation = result["reconciliation_status"] if versions == counts["valid_after_dedup_count"] else "FAILED"
            if reconciliation != "RECONCILED": raise ValueError(f"Reconciliation failed: {result}")
            row = create_silver_audit_row(dataset_name=config.dataset_name, processing_stage="BRONZE_TO_SILVER", pipeline_run_id=config.pipeline_run_id, stream_batch_id=stream_batch_id, status="SUCCESS", **counts, reconciliation_status=reconciliation, exploration_status=exploration_status, expectation_status=expectation_status, started_at=started)
            write_silver_audit(spark, row, audit_table=config.audit_table, audit_schema=silver_audit_schema, logger=logger)
            logger.info("Batch reconciled dataset=%s run=%s batch=%d source=%d", config.dataset_name, config.pipeline_run_id, stream_batch_id, counts["source_count"])
        except Exception as error:
            logger.exception("Micro-batch failed; Spark checkpoint must not advance")
            try:
                row = create_silver_audit_row(dataset_name=config.dataset_name, processing_stage="BRONZE_TO_SILVER", pipeline_run_id=config.pipeline_run_id, stream_batch_id=stream_batch_id, status="FAILED", **counts, reconciliation_status=reconciliation, exploration_status=exploration_status, expectation_status=expectation_status, error_message=f"{type(error).__name__}: {error}", started_at=started)
                write_silver_audit(spark, row, audit_table=config.audit_table, audit_schema=silver_audit_schema, logger=logger)
            except Exception:
                logger.exception("Failure audit write failed")
            raise
        finally:
            for df in reversed(owned): df.unpersist()
            batch_df.unpersist()
    return process_sales_order_lines_batch

