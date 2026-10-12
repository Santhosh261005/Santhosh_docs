"""Inventory-quant current-state Silver contract and pipeline wrappers."""
from pipelines.multi_dataset_silver import DatasetSpec, Relation, transform_dataset, create_valid_df, create_rejected_df, create_dataset_tables, merge_dataset_to_silver, build_dataset_batch_processor

SPEC=DatasetSpec(
 name="inventory_quants",source_schema="inventory_quants_schema.json",primary_key="id",key_output="quant_id",
 silver_table="datalake.silver.inventory_quants",rejected_table="datalake.quarantine.inventory_quants_rejected",
 relations=(Relation("product_id","product_id","product_name"),Relation("location_id","location_id","location_name"),Relation("company_id","company_id","company_name",required=False)),
 required=("id","product_id","location_id","quantity","reserved_quantity","write_date"),
 silver_fields=(("quant_id","id","long"),("product_id","product_id","long"),("product_name","product_name","string"),("location_id","location_id","long"),("location_name","location_name","string"),("quantity","quantity","double"),("reserved_quantity","reserved_quantity","double"),("available_quantity","expr:quantity - reserved_quantity","double"),("company_id","company_id","long"),("company_name","company_name","string"),("write_date","write_date","timestamp")),
 rejected_fields=(("id","long"),("quantity","double"),("reserved_quantity","double"),("write_date","timestamp")),
)

def transform_and_validate_inventory_quants(df,*,stream_batch_id,pipeline_run_id,processed_at=None): return transform_dataset(df,spec=SPEC,stream_batch_id=stream_batch_id,pipeline_run_id=pipeline_run_id,processed_at=processed_at)
def create_valid_silver_df(df): return create_valid_df(df,SPEC)
def create_rejected_records_df(df): return create_rejected_df(df,SPEC)
def deduplicate_valid_records(df):
 from pyspark.sql import functions as F
 from pyspark.sql.window import Window
 ranked=df.withColumn("_rank",F.row_number().over(Window.partitionBy("quant_id").orderBy(F.col("write_date").desc(),F.col("source_batch_id").desc(),F.col("source_file").desc())))
 return ranked.filter("_rank = 1").drop("_rank"),ranked.filter("_rank > 1").drop("_rank")
def create_inventory_quants_tables(spark,config): return create_dataset_tables(spark,spec=SPEC,config=config)
def merge_inventory_quants_to_silver(spark,df,*,config,stream_batch_id,logger): return merge_dataset_to_silver(spark,df,spec=SPEC,config=config,stream_batch_id=stream_batch_id,logger=logger)
def build_inventory_quants_batch_processor(*,config,logger=None): return build_dataset_batch_processor(spec=SPEC,config=config,logger=logger)
