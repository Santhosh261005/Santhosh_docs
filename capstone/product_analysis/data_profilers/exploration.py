"""Independent data profiling; writes measurements to operations.explorations.

No schema_drift import or ingestion is required.
"""

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F, types as T

EXPLORATION_DDL = """
profile_id STRING,
run_id STRING,
batch_id STRING,
dataset_name STRING,
source_table STRING,
profile_scope STRING,
processing_stage STRING,
profile_level STRING,
source_file_path STRING,
column_name STRING,
column_data_type STRING,
measure_name STRING,
measure_value STRING,
measure_value_type STRING,
profiled_at TIMESTAMP
"""


def _ident(name):
    """Quote a top-level Spark SQL identifier."""
    return "`" + name.replace("`", "``") + "`"


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
):
    """Return a lazy Spark DataFrame with one row per column/measure/scope.

    A string input resolves through spark.table(), including temporary views.
    FILE scope requires a non-null source-file column; no paths are guessed.
    Numeric statistics exclude NaN; non_null_count includes NaN.
    Empty-input percentages/min/max/mean/stddev are null, while counts are zero.
    """
    source_table = dataframe_or_table if isinstance(dataframe_or_table, str) else None
    frame = (
        spark.table(source_table) if source_table is not None else dataframe_or_table
    )
    if not isinstance(frame, DataFrame):
        raise TypeError("Provide a Spark DataFrame or table/view name")
    if len(frame.columns) != len(set(frame.columns)):
        raise ValueError("Column names must be unique before profiling")
    stage = processing_stage.upper()
    level = profile_level.upper()
    if stage not in ("LANDING", "BRONZE", "QUARANTINE"):
        raise ValueError("processing_stage must be LANDING, BRONZE or QUARANTINE")
    if level not in ("FULL", "LIGHTWEIGHT"):
        raise ValueError("profile_level must be FULL or LIGHTWEIGHT")
    required_names = (
        set(required_columns)
        if required_columns is not None
        else {field.name for field in frame.schema if not field.nullable}
    )
    if not required_names.issubset(frame.columns):
        raise ValueError("required_columns must exist in the DataFrame")
    scope = profile_scope.upper()
    if scope not in ("BATCH", "FILE"):
        raise ValueError("profile_scope must be BATCH or FILE")
    if not 0 < relative_sd <= 0.39:
        raise ValueError("relative_sd must be greater than 0 and at most 0.39")
    if scope == "FILE":
        if source_file_column not in frame.columns:
            raise ValueError("FILE profiling requires an existing source_file_column")
        if not isinstance(frame.schema[source_file_column].dataType, T.StringType):
            raise ValueError("source_file_column must have string type")
        key = F.col(_ident(source_file_column))
        if frame.where(key.isNull() | (F.length(F.trim(key)) == 0)).limit(1).count():
            raise ValueError("FILE profiling requires non-null, nonblank source paths")

    names = (
        list(columns)
        if columns is not None
        else [
            name
            for name in frame.columns
            if scope != "FILE" or name != source_file_column
        ]
    )
    if len(names) != len(set(names)) or any(
        name not in frame.columns for name in names
    ):
        raise ValueError("Profiling columns must be unique existing column names")
    if scope == "FILE" and source_file_column in names:
        raise ValueError("The file-grouping column is metadata, not a profiled column")
    if not names:
        return spark.createDataFrame([], EXPLORATION_DDL)

    # Rename internally so business names cannot collide with aggregation aliases.
    selected = [
        F.col(_ident(name)).alias(f"_c{index}") for index, name in enumerate(names)
    ]
    if scope == "FILE":
        selected.append(F.col(_ident(source_file_column)).alias("_file"))
    selected_frame = frame.select(*selected)
    aggregations = [F.count(F.lit(1)).alias("_records")]
    measures = []

    def aggregate(expression):
        alias = f"_a{len(aggregations)}"
        aggregations.append(expression.alias(alias))
        return F.col(alias)

    def measure(name, dtype, measure_name, value, value_type=None):
        measures.append((name, dtype.simpleString(), measure_name, value, value_type))

    for index, name in enumerate(names):
        dtype = frame.schema[name].dataType
        column = F.col(f"_c{index}")
        records = F.col("_records")
        non_null = aggregate(F.count(column))
        nulls = records - non_null
        measure(name, dtype, "record_count", records, "bigint")
        measure(name, dtype, "null_count", nulls, "bigint")
        measure(name, dtype, "non_null_count", non_null, "bigint")
        percentage = F.when(records > 0, nulls.cast("double") * 100 / records)
        measure(name, dtype, "null_percentage", percentage, "double")

        if name in required_names:
            measure(name, dtype, "required_null_count", nulls, "bigint")
        if level == "LIGHTWEIGHT":
            continue

        # Spark cannot hash maps by default, including maps nested
        # inside other complex values.
        complex_type = isinstance(dtype, (T.StructType, T.ArrayType, T.MapType))
        if approximate_distinct and not complex_type:
            distinct = aggregate(F.approx_count_distinct(column, relative_sd))
            measure(name, dtype, "approximate_distinct_count", distinct, "bigint")

        if isinstance(dtype, T.NumericType):
            numeric = column
            if isinstance(dtype, (T.FloatType, T.DoubleType)):
                nan = aggregate(F.count(F.when(F.isnan(column), F.lit(1))))
                measure(name, dtype, "nan_count", nan, "bigint")
                numeric = F.when(~F.isnan(column), column)
            measure(name, dtype, "minimum", aggregate(F.min(numeric)))
            measure(name, dtype, "maximum", aggregate(F.max(numeric)))
            measure(name, dtype, "mean", aggregate(F.avg(numeric)))
            measure(
                name, dtype, "standard_deviation", aggregate(F.stddev_samp(numeric))
            )
        elif isinstance(dtype, T.StringType):
            blanks = aggregate(F.count(F.when(column.rlike(r"^\s*$"), F.lit(1))))
            measure(name, dtype, "blank_string_count", blanks, "bigint")
            measure(
                name, dtype, "minimum_length", aggregate(F.min(F.length(column))), "int"
            )
            measure(
                name, dtype, "maximum_length", aggregate(F.max(F.length(column))), "int"
            )
        elif isinstance(dtype, (T.DateType, T.TimestampType, T.TimestampNTZType)):
            measure(name, dtype, "minimum", aggregate(F.min(column)))
            measure(name, dtype, "maximum", aggregate(F.max(column)))
        elif isinstance(dtype, T.BooleanType):
            measure(
                name,
                dtype,
                "true_count",
                aggregate(F.count(F.when(column, F.lit(1)))),
                "bigint",
            )
            measure(
                name,
                dtype,
                "false_count",
                aggregate(F.count(F.when(~column, F.lit(1)))),
                "bigint",
            )
        elif isinstance(dtype, (T.ArrayType, T.MapType)):
            size = F.when(column.isNotNull(), F.size(column))
            measure(name, dtype, "minimum_size", aggregate(F.min(size)), "int")
            measure(name, dtype, "maximum_size", aggregate(F.max(size)), "int")

    aggregated = (
        selected_frame.groupBy("_file").agg(*aggregations)
        if scope == "FILE"
        else selected_frame.agg(*aggregations)
    )
    rows = []
    for name, dtype, measure_name, value, value_type in measures:
        if value_type is None:
            # Determine Spark's actual result type, including decimal mean precision.
            value_type = (
                aggregated.select(value.alias("value"))
                .schema["value"]
                .dataType.simpleString()
            )
        rows.append(
            F.struct(
                F.lit(name).alias("column_name"),
                F.lit(dtype).alias("column_data_type"),
                F.lit(measure_name).alias("measure_name"),
                value.cast("string").alias("measure_value"),
                F.lit(value_type).alias("measure_value_type"),
            )
        )
    projected = aggregated.select(
        (
            F.col("_file").alias("source_file_path")
            if scope == "FILE"
            else F.lit(None).cast("string").alias("source_file_path")
        ),
        F.explode(F.array(*rows)).alias("measure"),
    )
    profiled_at = datetime.now(timezone.utc)
    result = projected.select(
        F.lit(str(uuid.uuid4())).alias("profile_id"),
        F.lit(run_id or str(uuid.uuid4())).alias("run_id"),
        F.lit(batch_id or str(uuid.uuid4())).alias("batch_id"),
        F.lit(dataset_name or source_table or "dataframe").alias("dataset_name"),
        F.lit(source_table).cast("string").alias("source_table"),
        F.lit(scope).alias("profile_scope"),
        F.lit(stage).alias("processing_stage"),
        F.lit(level).alias("profile_level"),
        "source_file_path",
        "measure.*",
        F.lit(profiled_at).alias("profiled_at"),
    )
    return result


def write_exploration_to_iceberg(
    spark,
    exploration_results,
    catalog_name="datalake",
    namespace_name="operations",
    table_name="explorations",
):
    """Append profiling metadata to a Parquet-backed Iceberg table.

    Call once per profile_id. Appending the same results again creates duplicate
    measures; repeated profiling intentionally produces a new profile_id.
    """
    if not isinstance(exploration_results, DataFrame):
        raise TypeError("exploration_results must be a Spark DataFrame")
    required = spark.createDataFrame([], EXPLORATION_DDL).schema
    actual = {field.name: field.dataType for field in exploration_results.schema}
    for field in required:
        if actual.get(field.name) != field.dataType:
            raise ValueError(f"Missing or incompatible exploration field: {field.name}")
    namespace = f"{_ident(catalog_name)}.{_ident(namespace_name)}"
    target = f"{namespace}.{_ident(table_name)}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} ({EXPLORATION_DDL}) USING iceberg "
        "TBLPROPERTIES ('write.format.default'='parquet')"
    )
    stored = {field.name: field.dataType for field in spark.table(target).schema}
    if any(stored.get(field.name) != field.dataType for field in required):
        raise ValueError("Existing exploration table has an incompatible schema")
    if set(stored) != set(required.names):
        raise ValueError("Existing exploration table contains unexpected columns")
    exploration_results.select(*required.names).writeTo(target).append()
    # foreachBatch may supply a different Spark session. Refresh the caller
    # catalog after the DataFrame session commits its Iceberg snapshot.
    spark.catalog.refreshTable(target)
    return target
