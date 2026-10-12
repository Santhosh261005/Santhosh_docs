# Product Analysis

## Sales order lines Bronze-to-Silver

The development notebook is `notebooks/bronze/bronze_to_silver.ipynb`; it imports the production modules. Run the job from the `product_analysis` directory with:

```bash
spark-submit --master local[2] jobs/sales_order_lines_bronze_to_silver.py
```

Command-line options override these environment defaults: `SPARK_MASTER`, `HDFS_DEFAULT_FS`, `ICEBERG_WAREHOUSE`, `ICEBERG_SPARK_RUNTIME_JAR`, `LOG_LEVEL`, `ENVIRONMENT`, `DATASET_NAME`, `BRONZE_PATH`, `CHECKPOINT_PATH`, `SILVER_TABLE`, `REJECTED_TABLE`, `AUDIT_TABLE`, `EXPLORATION_TABLE`, `EXPECTATION_TABLE`, `SCHEMA_FILE`, `PIPELINE_RUN_ID`, `MAX_FILES_PER_TRIGGER`, and `TRIGGER_TYPE`.

Defaults are Bronze `hdfs:///capstone/warehouse/bronze/sales_order_lines`, checkpoint `hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/sales_order_lines`, Silver `datalake.silver.sales_order_lines`, quarantine `datalake.quarantine.sales_order_lines_rejected`, ingestion audit `datalake.operations.silver_ingestion_audit`, exploration `datalake.operations.explorations`, and GX `datalake.operations.expectation_profiles`.

The materialized Iceberg staging-table MERGE remains in place. Quarantine, exploration, and GX writes append records, so a retry after those writes but before checkpoint advancement can create duplicate operational or rejected rows.
