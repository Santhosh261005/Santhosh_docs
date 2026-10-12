# Landing to Bronze Processing Design

## Processing Flow

```text
Landing CSV files
        |
        v
Infer actual schema
        |
        +--> Compare with expected schema
        |         |
        |         +--> Log drift details to Iceberg
        |
        v
Read using:
  mode = PERMISSIVE
  columnNameOfCorruptRecord = _corrupt_record
        |
        v
Split records
   /             Good records     Corrupt records
   |                    |
   v                    v
HDFS as CSV      Iceberg schema_drift table
   |
   +--> Good records are partitioned by write_date
         (YYYY-MM-DD only, avoiding excessive tiny partitions)

           Reconciliation
              |
              v
        Audit summary
```

---

## Capabilities

This Landing-to-Bronze framework provides:

- Recursive discovery of CSV files from the Landing path
- PERMISSIVE parsing mode
- `_corrupt_record` capture
- Good/bad record splitting
- Schema drift detection
  - Added columns
  - Missing columns
  - Datatype changes
- Good records written to HDFS in CSV format
- Corrupt raw records written to the Iceberg audit table
- File-level reconciliation
- Batch-level audit summary
- Run ID and Batch ID tracking
- Processing error auditing
- No collection of all corrupt records to driver memory
- Cache cleanup after processing

---

## Schema Drift Detection

The schema comparison process identifies:

### Added Columns

Columns present in incoming data but not in the expected schema.

### Missing Columns

Columns expected by the platform but absent in incoming files.

### Datatype Changes

Cases where a column exists but its datatype differs from the expected schema.

All drift findings are recorded in the Iceberg schema drift audit table.

---

## Good and Corrupt Record Handling

### Good Records

- Parsed successfully
- Match expected schema requirements
- Written to Bronze HDFS storage as CSV

### Corrupt Records

- Captured through `_corrupt_record`
- Preserved exactly as received
- Logged into Iceberg audit tables
- Available for later investigation and replay

---

## Reconciliation

Reconciliation validates:

```text
Source Record Count
          =
Good Record Count
+
Corrupt Record Count
```

This ensures no records are lost during ingestion.

---

## Audit Summary

Each processing batch produces:

- Run ID
- Batch ID
- Source record count
- Good record count
- Corrupt record count
- Processing status
- Processing timestamp
- Reconciliation status

---

## Partitioning Strategy

Good Bronze records are partitioned by:

```text
write_date_partition = YYYY-MM-DD
```

Example:

```text
2026-10-07
2026-10-08
2026-10-09
```

Hour and minute level partitioning is intentionally avoided to prevent creation of large numbers of tiny files and partitions.

---

## HDFS Layout

```text
hdfs:///capstone/warehouse/bronze/sales_order_lines/
└── batch_id=api-00002/
    └── file_00000/
        └── write_date_partition=2026-10-07/
            ├── part-00000-....csv
            └── part-00001-....csv
```

### Partition Hierarchy

```text
Dataset
   └── Batch ID
         └── Source File
                └── Write Date Partition
                       └── CSV Data Files
```

---

## Operational Characteristics

- Streaming-friendly design
- Supports incremental batch arrivals
- Scales without collecting corrupt records to driver memory
- Supports auditing and lineage tracking
- Provides deterministic reconciliation checks
- Maintains Bronze layer as raw-but-validated data
