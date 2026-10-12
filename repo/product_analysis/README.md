# Product Analysis

## Sales order lines Bronze-to-Silver

The development notebook is `notebooks/bronze/bronze_to_silver.ipynb`; it imports the production modules. Run the job from the `product_analysis` directory with:

```bash
spark-submit --master local[2] jobs/sales_order_lines_bronze_to_silver.py
```

Command-line options override these environment defaults: `SPARK_MASTER`, `HDFS_DEFAULT_FS`, `ICEBERG_WAREHOUSE`, `ICEBERG_SPARK_RUNTIME_JAR`, `LOG_LEVEL`, `ENVIRONMENT`, `DATASET_NAME`, `BRONZE_PATH`, `CHECKPOINT_PATH`, `SILVER_TABLE`, `REJECTED_TABLE`, `AUDIT_TABLE`, `EXPLORATION_TABLE`, `EXPECTATION_TABLE`, `SCHEMA_FILE`, `PIPELINE_RUN_ID`, `MAX_FILES_PER_TRIGGER`, and `TRIGGER_TYPE`.

Defaults are Bronze `hdfs:///capstone/warehouse/bronze/sales_order_lines`, checkpoint `hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/sales_order_lines`, Silver `datalake.silver.sales_order_lines`, quarantine `datalake.quarantine.sales_order_lines_rejected`, ingestion audit `datalake.operations.silver_ingestion_audit`, exploration `datalake.operations.explorations`, and GX `datalake.operations.expectation_profiles`.

The materialized Iceberg staging-table MERGE remains in place. Quarantine, exploration, and GX writes append records, so a retry after those writes but before checkpoint advancement can create duplicate operational or rejected rows.


## Multi-dataset Landing-to-Bronze and Bronze-to-Silver

This implementation adds the four datasets specified in `products_sales_orders_inventory_pipeline_handoff.md`. Production logic lives in `pipelines/`; notebooks import those modules. No Gold models or centralized dataset configuration file are introduced.

### Project additions

```text
product_analysis/
├── data_profilers/schema_drift.py             # explicit-schema landing ingestion support
├── pipelines/
│   ├── landing_to_bronze_common.py
│   ├── multi_dataset_silver.py
│   ├── products_silver.py
│   ├── sales_orders_silver.py
│   ├── inventory_quants_silver.py
│   ├── inventory_pickings_silver.py
│   └── cross_dataset_quality.py
├── jobs/                                      # eight dataset job entry points
├── schemas/                                   # four new JSON source schemas
└── notebooks/{bronze,silver}/                 # four notebooks per stage
```

### Tables and merge keys

| Dataset | Silver table | Quarantine table | Key | Partition |
|---|---|---|---|---|
| products | `datalake.silver.products` | `datalake.quarantine.products_rejected` | `product_id` | Unpartitioned |
| sales_orders | `datalake.silver.sales_orders` | `datalake.quarantine.sales_orders_rejected` | `sales_order_id` | `months(date_order)` |
| inventory_quants | `datalake.silver.inventory_quants` | `datalake.quarantine.inventory_quants_rejected` | `quant_id` | Unpartitioned |
| inventory_pickings | `datalake.silver.inventory_pickings` | `datalake.quarantine.inventory_pickings_rejected` | `picking_id` | `months(scheduled_date)` |

All Silver tables use deterministic staging Iceberg MERGE. Rows are ordered by `write_date`, `source_batch_id`, and `source_file` within a batch. The target is updated only for a strictly newer `write_date`; `silver_created_at` is retained on updates. Bronze lineage comes from `input_file_name()` and the physical `batch_id=` path segment.

All four landing jobs write the shared parser/schema audit table `datalake.operations.schema_drift`; all four Silver jobs append the common `datalake.operations.silver_ingestion_audit` records and use the existing exploration and GX profiler tables. Failed GX or exploration runs are logged and do not stop otherwise-valid data. Required transformation, quarantine, MERGE, and reconciliation failures are re-raised to preserve checkpoint progress.

### Run jobs

Run commands from `repo/product_analysis` after Spark, HDFS, Iceberg, and Great Expectations are configured. Landing jobs take `--source-file`, `--manifest-file`, `--bronze-path`, `--schema-file`, `--batch-id`, `--audit-table`, and `--log-level`. Bronze-to-Silver jobs take `--dataset-name`, `--bronze-path`, `--checkpoint-path`, `--silver-table`, `--rejected-table`, `--audit-table`, `--exploration-table`, `--expectation-table`, `--schema-file`, `--pipeline-run-id`, `--max-files-per-trigger`, `--trigger-type`, and `--log-level`.

```bash
spark-submit jobs/products_landing_to_bronze.py --source-file /path/to/products.csv --manifest-file /path/to/manifest.json --batch-id products-run-001
spark-submit jobs/products_bronze_to_silver.py --pipeline-run-id products-run-001
spark-submit jobs/sales_orders_landing_to_bronze.py --source-file /path/to/sales_orders.csv --manifest-file /path/to/manifest.json --batch-id orders-run-001
spark-submit jobs/sales_orders_bronze_to_silver.py --pipeline-run-id orders-run-001
spark-submit jobs/inventory_quants_landing_to_bronze.py --source-file /path/to/inventory_quants.csv --manifest-file /path/to/manifest.json --batch-id quants-run-001
spark-submit jobs/inventory_quants_bronze_to_silver.py --pipeline-run-id quants-run-001
spark-submit jobs/inventory_pickings_landing_to_bronze.py --source-file /path/to/inventory_pickings.csv --manifest-file /path/to/manifest.json --batch-id pickings-run-001
spark-submit jobs/inventory_pickings_bronze_to_silver.py --pipeline-run-id pickings-run-001
```

For each option, the explicit CLI value takes precedence over the uppercase environment variable, then the safe code default. Environment settings include `SPARK_MASTER`, `HDFS_DEFAULT_FS`, `ICEBERG_WAREHOUSE`, `ICEBERG_SPARK_RUNTIME_JAR`, `LOG_LEVEL`, `ENVIRONMENT`, `SOURCE_FILE`, `MANIFEST_FILE`, `BRONZE_PATH`, `CHECKPOINT_PATH`, `SILVER_TABLE`, `REJECTED_TABLE`, `AUDIT_TABLE`, `EXPLORATION_TABLE`, `EXPECTATION_TABLE`, `SCHEMA_FILE`, `BATCH_ID`, `PIPELINE_RUN_ID`, `MAX_FILES_PER_TRIGGER`, and `TRIGGER_TYPE`. Do not put credentials in arguments or logs.

Notebook paths:

- `notebooks/bronze/products_landing_to_bronze.ipynb`
- `notebooks/bronze/sales_orders_landing_to_bronze.ipynb`
- `notebooks/bronze/inventory_quants_landing_to_bronze.ipynb`
- `notebooks/bronze/inventory_pickings_landing_to_bronze.ipynb`
- `notebooks/silver/products_bronze_to_silver.ipynb`
- `notebooks/silver/sales_orders_bronze_to_silver.ipynb`
- `notebooks/silver/inventory_quants_bronze_to_silver.ipynb`
- `notebooks/silver/inventory_pickings_bronze_to_silver.ipynb`

Each Silver notebook has an opt-in checkpoint-rerun cell (`RUN_CHECKPOINT_RERUN = False` initially). Each dataset must keep its own checkpoint.
Open notebooks with the `product_analysis` directory as the Jupyter project root and run cells top-to-bottom. In each Landing notebook, set `SOURCE_FILE`, `MANIFEST_FILE`, `BATCH_ID`, and `BRONZE_PATH` for the selected source before running the ingestion cell. Run the corresponding Silver notebook only after Landing-to-Bronze has written data to its Bronze path; set its checkpoint and table environment values before creating the stream. The inventory notebook source defaults are intentionally paths for the not-present CSVs and will stop with `FileNotFoundError` until those files are supplied.

### Dataset validation and semantics

- **Products:** required ID/name/relations, nonnegative `list_price`, required stock values and timestamps, and `write_date >= create_date`. Negative inventory quantities and inactive products remain valid. `qty_available` is the approved usable-stock measure; `virtual_available` is projected stock; `list_price` is not cost.
- **Sales orders:** required order/customer/date/state/amount/currency/company/salesperson and operational flags. `team_id` is optional but must parse when present. `ops_confirmed_at` is optional. Raw order state is retained; known values are standardized and unknown nonempty values become `OTHER` without rejection. `ops_unmet_demand` and `ops_closed` remain separate.
- **Inventory quants:** required quant/product/location/quantity/reserved quantity/write date; `company_id=False` is accepted as null. Negative quantities and `reserved_quantity > quantity` are preserved. `available_quantity = quantity - reserved_quantity` is a location measure, not the approved stock-cover numerator.
- **Inventory pickings:** required ID/name/state/locations/scheduled and lifecycle timestamps/picking type/stage/nonnegative handling seconds. Optional partner/company/sale/purchase relations accept `False`/null. Done/completed records require `date_done`. Unknown nonempty state is retained and standardized to `OTHER`.

The warning-only cross-dataset report is `spark-submit jobs/cross_dataset_quality_report.py`; it reports orphan keys and does not filter records.

### Source and validation evidence available in this workspace

`data/source-manifest.json` describes batch `api-00001`: 4,071 products and zero sales orders, inventory quants, and inventory pickings. The local `data/products/2026-10-07_23_39.csv` has 4,071 rows and the expected product header. The local `data/orders/2026-10-07_23_39.csv` has 930 rows and the expected order header, so its record count and filename do not match that manifest. There are no local quants or pickings CSVs. Landing jobs warn about manifest row-count mismatch and require an explicit source file if the default is absent; they do not claim the manifest describes a mismatched file.

The supplied local full-file profile found no null/blank values in the 930 orders fields inspected. `state` values were `sale` (787) and `cancel` (143); partner/currency/company/user/team relationships were populated. `ops_confirmed_at` contained `False` in 143 rows; Landing-to-Bronze maps that optional timestamp sentinel to null while preserving the timestamp contract. Products contains 4,071 rows; the manifest's legacy product schema declared `id` integer, compatible with the requested logical `long` schema.
The same CSV profile found all product relations parseable, no negative `list_price`, and no product `write_date` earlier than `create_date`; all 930 order relations parsed, no order total fell below untaxed amount, and no order `write_date` preceded `create_date`.

The Spark/HDFS/Iceberg services and source CSVs for inventory quants and pickings are not present in this workspace. Therefore no Spark streaming counts, Iceberg MERGE checks, eight-job execution evidence, or checkpoint-rerun evidence is claimed here. The handoff's append-based retry limitation remains: a failure after quarantine/exploration/GX append but before checkpoint commit can duplicate those audit/rejected rows.
