"""Shared audit sink; source records are never written here."""

import json
from datetime import datetime, timezone
from pyspark.sql import types as T


def write_audit(spark, rows, table):
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("audit_table must be catalog.namespace.table")
    quoted = ["`" + part.replace("`", "``") + "`" for part in parts]
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + ".".join(quoted[:2]))
    schema = T.StructType(
        [
            T.StructField(name, dtype, True)
            for name, dtype in [
                ("run_id", T.StringType()),
                ("dataset_name", T.StringType()),
                ("profile_kind", T.StringType()),
                ("source_file", T.StringType()),
                ("status", T.StringType()),
                ("details_json", T.StringType()),
                ("profiled_at", T.TimestampType()),
            ]
        ]
    )
    values = [
        (
            r["run_id"],
            r.get("dataset_name"),
            r["profile_kind"],
            r.get("source_file"),
            r["status"],
            json.dumps(r["details"], default=str),
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
        for r in rows
    ]
    frame = spark.createDataFrame(values, schema)
    destination = ".".join(quoted)
    if spark.catalog.tableExists(table):
        frame.writeTo(destination).append()
    else:
        frame.writeTo(destination).using("iceberg").create()
