"""Products Silver contract and pipeline entry points."""
from pipelines.multi_dataset_silver import DatasetSpec, Relation, transform_dataset, create_valid_df, create_rejected_df, create_dataset_tables, merge_dataset_to_silver, build_dataset_batch_processor

SPEC = DatasetSpec(
    name="products", source_schema="products_schema.json", primary_key="id", key_output="product_id",
    silver_table="datalake.silver.products", rejected_table="datalake.quarantine.products_rejected",
    relations=(Relation("categ_id", "category_id", "category_name"), Relation("uom_id", "uom_id", "uom_name"), Relation("product_tmpl_id", "product_template_id", "product_template_name")),
    required=("id", "name", "categ_id", "uom_id", "product_tmpl_id", "list_price", "active", "qty_available", "virtual_available", "create_date", "write_date"),
    extra_rules=(("LIST_PRICE_NONNEGATIVE", "list_price must be >= 0", "list_price < 0"), ("WRITE_DATE_NOT_BEFORE_CREATE", "write_date must be >= create_date", "write_date < create_date")),
    silver_fields=(("product_id","id","long"),("product_code","default_code","string"),("product_name","name","string"),("category_id","category_id","long"),("category_name","category_name","string"),("uom_id","uom_id","long"),("uom_name","uom_name","string"),("list_price","list_price","double"),("active","active","boolean"),("qty_available","qty_available","double"),("virtual_available","virtual_available","double"),("product_template_id","product_template_id","long"),("product_template_name","product_template_name","string"),("create_date","create_date","timestamp"),("write_date","write_date","timestamp")),
    rejected_fields=(("id","long"),("default_code","string"),("name","string"),("list_price","double"),("active","boolean"),("qty_available","double"),("virtual_available","double"),("create_date","timestamp"),("write_date","timestamp")),
    gx_bounds=(("list_price",0.0,None),),
)

def transform_and_validate_products(df, *, stream_batch_id, pipeline_run_id, processed_at=None):
    return transform_dataset(df, spec=SPEC, stream_batch_id=stream_batch_id, pipeline_run_id=pipeline_run_id, processed_at=processed_at)
def create_valid_silver_df(df): return create_valid_df(df, SPEC)
def create_rejected_records_df(df): return create_rejected_df(df, SPEC)
def deduplicate_valid_records(df):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    ranked=df.withColumn("_rank",F.row_number().over(Window.partitionBy("product_id").orderBy(F.col("write_date").desc(),F.col("source_batch_id").desc(),F.col("source_file").desc())))
    return ranked.filter("_rank = 1").drop("_rank"),ranked.filter("_rank > 1").drop("_rank")
def create_products_tables(spark, config): return create_dataset_tables(spark,spec=SPEC,config=config)
def merge_products_to_silver(spark,df,*,config,stream_batch_id,logger): return merge_dataset_to_silver(spark,df,spec=SPEC,config=config,stream_batch_id=stream_batch_id,logger=logger)
def build_products_batch_processor(*,config,logger=None): return build_dataset_batch_processor(spec=SPEC,config=config,logger=logger)
