"""Dataset-configured Spark transformations and Iceberg micro-batch processing."""
from __future__ import annotations

import logging
import re
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.bronze_to_silver_common import (
    audit_schema, build_source_lineage_columns, count_incoming_keys_found_in_silver,
    count_incoming_versions_satisfied_in_silver, create_silver_audit_row,
    evaluate_micro_batch_reconciliation, quote_iceberg_table_name,
    write_rejected_records, write_silver_audit,
)


@dataclass(frozen=True)
class Relation:
    source: str
    output_id: str
    output_name: str
    required: bool = True


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    source_schema: str
    primary_key: str
    key_output: str
    silver_table: str
    rejected_table: str
    silver_fields: tuple  # (output, source/expression, Spark type)
    rejected_fields: tuple  # (column, Spark type)
    relations: tuple
    required: tuple
    extra_rules: tuple = ()  # (rule name, reason, Spark SQL boolean expression)
    state_source: str | None = None
    standardized_state_output: str | None = None
    known_states: tuple = ()
    partition_by: str | None = None
    gx_bounds: tuple = ()  # (column, minimum, maximum)


@dataclass(frozen=True)
class DatasetRuntimeConfig:
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
    catalog: str = "datalake"
    trigger_type: str = "availableNow"


def load_dataset_schema(schema_file):
    from pyspark.sql import types as T
    mapping = {"string": T.StringType, "long": T.LongType, "integer": T.IntegerType,
        "int": T.IntegerType, "double": T.DoubleType, "float": T.FloatType,
        "boolean": T.BooleanType, "timestamp": T.TimestampType, "date": T.DateType}
    definition = json.loads(Path(schema_file).read_text(encoding="utf-8"))
    fields = []
    for name, kind in definition.items():
        constructor = mapping.get(str(kind).lower())
        if constructor is None: raise ValueError(f"Unsupported field type {kind!r} for {name!r}")
        fields.append(T.StructField(name, constructor(), True))
    return T.StructType(fields)


def build_bronze_stream(spark, config, schema):
    return (spark.readStream.format("csv").schema(schema).option("header", "true")
        .option("quote", '"').option("escape", '"')
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSX")
        .option("recursiveFileLookup", "true").option("multiLine", "false")
        .option("maxFilesPerTrigger", config.max_files_per_trigger).load(config.bronze_path))


def silver_job_main(spec, argv=None):
    import argparse
    from pipelines.bronze_to_silver_common import configure_logging
    parser = argparse.ArgumentParser(description=f"Bronze-to-Silver job for {spec.name}")
    defaults = {
        "dataset_name": spec.name,
        "bronze_path": f"hdfs://localhost:9000/capstone/warehouse/bronze/{spec.name}",
        "checkpoint_path": f"hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/{spec.name}",
        "silver_table": f"datalake.silver.{spec.name}",
        "rejected_table": f"datalake.quarantine.{spec.name}_rejected",
        "audit_table": "datalake.operations.silver_ingestion_audit",
        "exploration_table": "datalake.operations.explorations",
        "expectation_table": "datalake.operations.expectation_profiles",
        "schema_file": str(Path(__file__).resolve().parents[1] / "schemas" / spec.source_schema),
        "pipeline_run_id": f"{spec.name}-{uuid.uuid4()}",
        "max_files_per_trigger": "5", "trigger_type": "availableNow", "log_level": "INFO",
    }
    for arg, default in defaults.items():
        parser.add_argument("--" + arg.replace("_", "-"), default=os.getenv(arg.upper(), default),
            type=int if arg == "max_files_per_trigger" else str,
            choices=("availableNow",) if arg == "trigger_type" else None)
    args = parser.parse_args(argv)
    logger = configure_logging(args.log_level)
    config = DatasetRuntimeConfig(**{k: getattr(args, k) for k in defaults if k != "log_level"})
    from pipelines.landing_to_bronze_common import start_landing_spark
    spark = start_landing_spark(f"{spec.name}_bronze_to_silver", config.catalog)
    try:
        schema = load_dataset_schema(config.schema_file)
        audit_table = create_dataset_tables(spark, spec=spec, config=config)
        # Use caller's audit override for all microbatch audit writes.
        config = DatasetRuntimeConfig(**{**config.__dict__, "audit_table": audit_table})
        stream = build_bronze_stream(spark, config, schema)
        callback = build_dataset_batch_processor(spec=spec, config=config, logger=logger)
        query = (stream.writeStream.queryName(f"{spec.name}_bronze_to_silver")
            .foreachBatch(callback).option("checkpointLocation", config.checkpoint_path)
            .trigger(availableNow=True).start())
        query.awaitTermination()
        if query.exception() is not None: raise RuntimeError(query.exception())
        logger.info("Job completed dataset=%s run=%s", spec.name, config.pipeline_run_id)
    except Exception:
        logger.exception("Bronze-to-Silver job failed dataset=%s", spec.name)
        raise
    finally: spark.stop()


def _q(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL column name: {name!r}")
    return f"`{name}`"


def _cast_expr(expression, spark_type):
    return expression.cast(spark_type) if spark_type else expression


def transform_dataset(df, *, spec: DatasetSpec, stream_batch_id, pipeline_run_id, processed_at=None):
    """Parse relation arrays and attach stable failure rule names and reasons."""
    out = df
    rules = []
    for col in spec.required:
        value = F.trim(F.col(col).cast("string"))
        condition = F.col(col).isNull() | (value == "")
        rules.append((f"{col.upper()}_REQUIRED", f"{col} must not be null or blank", condition))
    for relation in spec.relations:
        out = out.withColumn(f"_raw_{relation.source}", F.col(relation.source).cast("string"))
        raw_column = F.col(f"_raw_{relation.source}")
        raw = F.trim(raw_column)
        is_false = F.lower(raw).isin("false", "null", "none") | (raw == "")
        parsed_id = F.get_json_object(raw_column, "$[0]").cast("long")
        parsed_name = F.get_json_object(raw_column, "$[1]")
        out = (out.withColumn(relation.output_id, F.when(is_false, F.lit(None).cast("long")).otherwise(parsed_id))
            .withColumn(relation.output_name, F.when(is_false, F.lit(None).cast("string")).otherwise(parsed_name)))
        if relation.required:
            bad = F.col(relation.source).isNull() | (raw == "") | F.col(relation.output_id).isNull()
            rules.append((f"{relation.source.upper()}_RELATION_INVALID", f"{relation.source} must contain a numeric Odoo relation ID", bad))
        else:
            bad = (~is_false) & F.col(relation.output_id).isNull()
            rules.append((f"{relation.source.upper()}_RELATION_MALFORMED", f"{relation.source} must be a valid relation or False/null", bad))
    for rule, reason, sql in spec.extra_rules:
        rules.append((rule, reason, F.expr(sql)))
    if spec.state_source:
        state = F.lower(F.trim(F.col(spec.state_source)))
        known = [s.lower() for s in spec.known_states]
        out = out.withColumn(spec.standardized_state_output,
            F.when(state.isNull() | (state == ""), F.lit(None).cast("string"))
            .when(state.isin(*known), state).otherwise(F.lit("OTHER")))
        out = out.withColumn("_unknown_state", F.when(state.isNotNull() & (state != "") & (~state.isin(*known)), F.lit(True)).otherwise(F.lit(False)))
    failed = [F.when(cond, F.lit(rule)) for rule, _, cond in rules]
    reasons = [F.when(cond, F.lit(reason)) for _, reason, cond in rules]
    timestamp = F.lit(processed_at).cast("timestamp") if processed_at else F.current_timestamp()
    return (out.withColumn("failed_rule_names", F.array_compact(F.array(*failed)))
        .withColumn("rejection_reasons", F.array_compact(F.array(*reasons)))
        .withColumn("record_status", F.when(F.size("failed_rule_names") == 0, "VALID").otherwise("REJECTED"))
        .withColumn("stream_batch_id", F.lit(int(stream_batch_id)).cast("long"))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("_processed_at", timestamp))


def create_valid_df(transformed, spec: DatasetSpec):
    expressions = []
    for output, source, spark_type in spec.silver_fields:
        if source == "_processed_at": expression = F.col("_processed_at")
        elif source.startswith("expr:"): expression = F.expr(source[5:])
        else: expression = F.col(source)
        expressions.append(_cast_expr(expression, spark_type).alias(output))
    expressions.extend([
        F.col("batch_id").cast("string").alias("source_batch_id"),
        F.col("_bronze_source_file").cast("string").alias("source_file"),
        F.col("stream_batch_id").cast("long"), F.col("pipeline_run_id").cast("string"),
        F.col("_processed_at").cast("timestamp").alias("silver_created_at"),
        F.col("_processed_at").cast("timestamp").alias("silver_updated_at"),
    ])
    return transformed.filter(F.col("record_status") == "VALID").select(*expressions)


def create_rejected_df(transformed, spec: DatasetSpec):
    fields = [F.col(c).cast(t).alias(c) for c, t in spec.rejected_fields]
    for relation in spec.relations:
        fields.extend([F.col(f"_raw_{relation.source}").alias(f"{relation.source}_raw"), F.col(relation.output_id), F.col(relation.output_name)])
    fields.extend([F.col("failed_rule_names"), F.col("rejection_reasons"),
        F.col("batch_id").cast("string").alias("source_batch_id"),
        F.col("_bronze_source_file").cast("string").alias("source_file"),
        F.col("stream_batch_id").cast("long"), F.col("pipeline_run_id").cast("string"),
        F.col("_processed_at").cast("timestamp").alias("rejected_at")])
    return transformed.filter(F.col("record_status") == "REJECTED").select(*fields)


def rejected_column_order(spec: DatasetSpec):
    names = [name for name, _ in spec.rejected_fields]
    for relation in spec.relations:
        names.extend([f"{relation.source}_raw", relation.output_id, relation.output_name])
    names.extend(["failed_rule_names", "rejection_reasons", "source_batch_id", "source_file",
        "stream_batch_id", "pipeline_run_id", "rejected_at"])
    return names


def create_dataset_tables(spark, *, spec: DatasetSpec, config):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.silver")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.catalog}.quarantine")
    fields = list(spec.silver_fields) + [("source_batch_id", "", "string"), ("source_file", "", "string"),
        ("stream_batch_id", "", "long"), ("pipeline_run_id", "", "string"), ("silver_created_at", "", "timestamp"), ("silver_updated_at", "", "timestamp")]
    ddl = ",\n".join(f"{_q(name)} {typ.upper()}" + (" NOT NULL" if name in (spec.key_output, "stream_batch_id", "pipeline_run_id", "silver_created_at", "silver_updated_at") else "") for name, _, typ in fields)
    partition = f" PARTITIONED BY ({spec.partition_by})" if spec.partition_by else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {quote_iceberg_table_name(config.silver_table)} ({ddl}) USING iceberg{partition} TBLPROPERTIES ('format-version'='2')")
    reject_fields = list(spec.rejected_fields)
    for relation in spec.relations:
        reject_fields.extend([(f"{relation.source}_raw", "string"), (relation.output_id, "long"), (relation.output_name, "string")])
    reject_fields.extend([("failed_rule_names", "array<string>"), ("rejection_reasons", "array<string>"), ("source_batch_id", "string"), ("source_file", "string"), ("stream_batch_id", "long"), ("pipeline_run_id", "string"), ("rejected_at", "timestamp")])
    rddl = ",\n".join(f"{_q(name)} {typ.upper()}" + (" NOT NULL" if name in ("failed_rule_names", "rejection_reasons", "stream_batch_id", "pipeline_run_id", "rejected_at") else "") for name, typ in reject_fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {quote_iceberg_table_name(config.rejected_table)} ({rddl}) USING iceberg PARTITIONED BY (days(rejected_at)) TBLPROPERTIES ('format-version'='2')")
    # The common operations audit table is shared with sales_order_lines.
    audit = config.audit_table
    parts = audit.split(".")
    if len(parts) != 3: raise ValueError("audit_table must be catalog.namespace.table")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_q(parts[0])}.{_q(parts[1])}")
    if not spark.catalog.tableExists(audit):
        schema = audit_schema()
        cols = ", ".join(f"{_q(f.name)} {f.dataType.simpleString().upper()}" + (" NOT NULL" if not f.nullable else "") for f in schema.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {quote_iceberg_table_name(audit)} ({cols}) USING iceberg PARTITIONED BY (days(started_at)) TBLPROPERTIES ('format-version'='2')")
    return audit


def merge_dataset_to_silver(spark, source, *, spec, config, stream_batch_id, logger):
    key = spec.key_output
    if source.filter(F.col(key).isNull()).limit(1).count(): raise ValueError(f"Null merge keys in {spec.name}")
    if source.groupBy(key).count().filter("count > 1").limit(1).count(): raise ValueError(f"Duplicate merge keys in {spec.name}")
    suffix = re.sub(r"[^A-Za-z0-9_]", "_", f"{config.pipeline_run_id}_{stream_batch_id}")
    staging = f"{config.catalog}.operations.{spec.name}_staging_{suffix}"
    spark.sql(f"DROP TABLE IF EXISTS {quote_iceberg_table_name(staging)}")
    try:
        source.writeTo(quote_iceberg_table_name(staging)).using("iceberg").create()
        if spark.table(staging).count() != source.count(): raise ValueError("Staging table count does not reconcile")
        columns = source.columns
        updates = [c for c in columns if c not in (key, "silver_created_at")]
        assignments = ",\n".join(f"target.{_q(c)} = source.{_q(c)}" for c in updates)
        insert_cols = ", ".join(_q(c) for c in columns)
        insert_vals = ", ".join(f"source.{_q(c)}" for c in columns)
        spark.sql(f"""MERGE INTO {quote_iceberg_table_name(config.silver_table)} target
            USING {quote_iceberg_table_name(staging)} source ON target.{_q(key)}=source.{_q(key)}
            WHEN MATCHED AND source.write_date > target.write_date THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})""")
        spark.catalog.refreshTable(config.silver_table)
    finally:
        try: spark.sql(f"DROP TABLE IF EXISTS {quote_iceberg_table_name(staging)}")
        except Exception: logger.exception("Could not drop staging table %s", staging)


def build_dataset_batch_processor(*, spec, config, logger=None):
    """Return callback with caller-owned caches released and required failures re-raised."""
    logger = logger or logging.getLogger(__name__)
    from data_profilers.exploration import explore_data, write_exploration_to_iceberg
    from data_profilers.great_expectations import profile_expectations
    def callback(batch_df, stream_batch_id):
        spark = batch_df.sparkSession
        stream_batch_id = int(stream_batch_id)
        started = datetime.utcnow()
        cached = []
        counts = dict(source_count=0, valid_before_dedup_count=0, rejected_count=0,
            duplicate_count=0, valid_after_dedup_count=0, incoming_keys_found_in_silver=None)
        exploration_status = expectation_status = "SKIPPED"
        reconciliation = "NOT_EVALUATED"
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            counts["source_count"] = batch_df.count()
            if counts["source_count"] == 0:
                counts["incoming_keys_found_in_silver"] = 0
                row = create_silver_audit_row(dataset_name=spec.name, pipeline_run_id=config.pipeline_run_id,
                    stream_batch_id=stream_batch_id, status="SKIPPED_EMPTY_BATCH", **counts,
                    reconciliation_status="RECONCILED", exploration_status="SKIPPED", expectation_status="SKIPPED",
                    started_at=started)
                write_silver_audit(spark, row, audit_table=config.audit_table, audit_schema=audit_schema(), logger=logger)
                logger.warning("Empty micro-batch dataset=%s batch=%d", spec.name, stream_batch_id)
                return
            enriched = build_source_lineage_columns(batch_df)
            run_id = f"{config.pipeline_run_id}-batch-{stream_batch_id}"
            business_cols = [f.name for f in batch_df.schema.fields]
            try:
                measures = explore_data(spark, enriched, dataset_name=spec.name, run_id=run_id,
                    batch_id=str(stream_batch_id), profile_scope="BATCH", processing_stage="BRONZE",
                    profile_level="FULL", columns=business_cols, required_columns=list(spec.required),
                    approximate_distinct=True, relative_sd=0.05)
                exploration_parts = config.exploration_table.split(".")
                if len(exploration_parts) != 3: raise ValueError("exploration_table must be catalog.namespace.table")
                write_exploration_to_iceberg(spark, measures, catalog_name=exploration_parts[0],
                    namespace_name=exploration_parts[1], table_name=exploration_parts[2])
                exploration_status = "COMPLETED"
            except Exception:
                exploration_status = "FAILED"; logger.exception("Exploration failed; continuing dataset=%s", spec.name)
            # GX is best-effort monitoring; row-level Spark validation remains authoritative.
            try:
                import great_expectations as gx
                rules = [gx.expectations.ExpectColumnValuesToNotBeNull(column=c) for c in spec.required if c in business_cols]
                rules.extend(gx.expectations.ExpectColumnValuesToBeBetween(column=c, min_value=minimum, max_value=maximum)
                    for c, minimum, maximum in spec.gx_bounds)
                profile_expectations(enriched, rules, dataset_name=spec.name, run_id=run_id, audit_table=config.expectation_table)
                expectation_status = "COMPLETED"
            except Exception:
                expectation_status = "FAILED"; logger.exception("GX failed; continuing dataset=%s", spec.name)
            transformed = transform_dataset(enriched, spec=spec, stream_batch_id=stream_batch_id,
                pipeline_run_id=config.pipeline_run_id, processed_at=started).persist(StorageLevel.MEMORY_AND_DISK); cached.append(transformed)
            if transformed.count() != counts["source_count"]: raise ValueError("Transformation changed source row count")
            if spec.state_source:
                unknown_states = transformed.filter(F.col("_unknown_state") == F.lit(True)).count()
                if unknown_states:
                    logger.warning("Unknown non-empty states mapped to OTHER dataset=%s count=%d", spec.name, unknown_states)
            valid = create_valid_df(transformed, spec).persist(StorageLevel.MEMORY_AND_DISK); cached.append(valid)
            rejected = create_rejected_df(transformed, spec).persist(StorageLevel.MEMORY_AND_DISK); cached.append(rejected)
            counts["valid_before_dedup_count"], counts["rejected_count"] = valid.count(), rejected.count()
            if counts["source_count"] != counts["valid_before_dedup_count"] + counts["rejected_count"]: raise ValueError("Source split reconciliation failed")
            write_rejected_records(rejected, rejected_table=config.rejected_table,
                expected_columns=rejected_column_order(spec), rejected_count=counts["rejected_count"], logger=logger)
            w = Window.partitionBy(spec.key_output).orderBy(F.col("write_date").desc(), F.col("source_batch_id").desc(), F.col("source_file").desc())
            ranked = valid.withColumn("_row_rank", F.row_number().over(w))
            deduped = ranked.filter(F.col("_row_rank") == 1).drop("_row_rank").persist(StorageLevel.MEMORY_AND_DISK); cached.append(deduped)
            duplicates = ranked.filter(F.col("_row_rank") > 1).drop("_row_rank").persist(StorageLevel.MEMORY_AND_DISK); cached.append(duplicates)
            counts["valid_after_dedup_count"], counts["duplicate_count"] = deduped.count(), duplicates.count()
            if counts["valid_before_dedup_count"] != counts["valid_after_dedup_count"] + counts["duplicate_count"]: raise ValueError("Deduplication reconciliation failed")
            if counts["valid_after_dedup_count"]:
                merge_dataset_to_silver(spark, deduped, spec=spec, config=config, stream_batch_id=stream_batch_id, logger=logger)
                counts["incoming_keys_found_in_silver"] = count_incoming_keys_found_in_silver(spark, deduped,
                    silver_table=config.silver_table, key_column=spec.key_output)
                versions = count_incoming_versions_satisfied_in_silver(spark, deduped,
                    silver_table=config.silver_table, key_column=spec.key_output, version_column="write_date")
            else: counts["incoming_keys_found_in_silver"], versions = 0, 0
            result = evaluate_micro_batch_reconciliation(**counts)
            reconciliation = result["reconciliation_status"] if versions == counts["valid_after_dedup_count"] else "FAILED"
            if reconciliation != "RECONCILED": raise ValueError(f"Silver reconciliation failed: {result}")
            row = create_silver_audit_row(dataset_name=spec.name, pipeline_run_id=config.pipeline_run_id,
                stream_batch_id=stream_batch_id, status="SUCCESS", **counts, reconciliation_status=reconciliation,
                exploration_status=exploration_status, expectation_status=expectation_status, started_at=started)
            write_silver_audit(spark, row, audit_table=config.audit_table, audit_schema=audit_schema(), logger=logger)
            logger.info("Microbatch success dataset=%s run=%s batch=%d source=%d valid=%d rejected=%d duplicates=%d target=%s",
                spec.name, config.pipeline_run_id, stream_batch_id, counts["source_count"], counts["valid_before_dedup_count"], counts["rejected_count"], counts["duplicate_count"], config.silver_table)
        except Exception as error:
            logger.exception("Bronze-to-Silver failed; checkpoint must not advance dataset=%s batch=%d", spec.name, stream_batch_id)
            try:
                row = create_silver_audit_row(dataset_name=spec.name, pipeline_run_id=config.pipeline_run_id,
                    stream_batch_id=stream_batch_id, status="FAILED", **counts, reconciliation_status=reconciliation,
                    exploration_status=exploration_status, expectation_status=expectation_status,
                    error_message=f"{type(error).__name__}: {error}", started_at=started)
                write_silver_audit(spark, row, audit_table=config.audit_table, audit_schema=audit_schema(), logger=logger)
            except Exception: logger.exception("Failure audit write failed")
            raise
        finally:
            for frame in reversed(cached): frame.unpersist()
            batch_df.unpersist()
    return callback
