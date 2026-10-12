"""Sales-order Silver contract, transformation, validation and merge wrappers."""
from pipelines.multi_dataset_silver import DatasetSpec, Relation, transform_dataset, create_valid_df, create_rejected_df, create_dataset_tables, merge_dataset_to_silver, build_dataset_batch_processor

SPEC=DatasetSpec(
 name="sales_orders",source_schema="sales_orders_schema.json",primary_key="id",key_output="sales_order_id",
 silver_table="datalake.silver.sales_orders",rejected_table="datalake.quarantine.sales_orders_rejected",
 relations=(Relation("partner_id","customer_id","customer_name"),Relation("currency_id","currency_id","currency_name"),Relation("company_id","company_id","company_name"),Relation("user_id","salesperson_id","salesperson_name"),Relation("team_id","sales_team_id","sales_team_name",required=False)),
 required=("id","name","partner_id","date_order","state","amount_untaxed","amount_total","currency_id","company_id","user_id","create_date","write_date","ops_order_at","ops_unmet_demand","ops_closed"),
 extra_rules=(("WRITE_DATE_NOT_BEFORE_CREATE","write_date must be >= create_date","write_date < create_date"),("TOTAL_NOT_BELOW_UNTAXED","amount_total should be >= amount_untaxed","amount_total < amount_untaxed")),
 state_source="state",standardized_state_output="standardized_order_state",known_states=("draft","sent","sale","done","cancel"),partition_by="months(date_order)",
 silver_fields=(("sales_order_id","id","long"),("sales_order_name","name","string"),("customer_id","customer_id","long"),("customer_name","customer_name","string"),("date_order","date_order","timestamp"),("order_state","state","string"),("standardized_order_state","standardized_order_state","string"),("amount_untaxed","amount_untaxed","double"),("amount_total","amount_total","double"),("currency_id","currency_id","long"),("currency_name","currency_name","string"),("company_id","company_id","long"),("company_name","company_name","string"),("salesperson_id","salesperson_id","long"),("salesperson_name","salesperson_name","string"),("sales_team_id","sales_team_id","long"),("sales_team_name","sales_team_name","string"),("client_order_ref","client_order_ref","string"),("ops_order_at","ops_order_at","timestamp"),("ops_confirmed_at","ops_confirmed_at","timestamp"),("ops_unmet_demand","ops_unmet_demand","boolean"),("ops_closed","ops_closed","boolean"),("create_date","create_date","timestamp"),("write_date","write_date","timestamp")),
 rejected_fields=(("id","long"),("name","string"),("date_order","timestamp"),("state","string"),("amount_untaxed","double"),("amount_total","double"),("create_date","timestamp"),("write_date","timestamp"),("client_order_ref","string"),("ops_order_at","timestamp"),("ops_confirmed_at","timestamp"),("ops_unmet_demand","boolean"),("ops_closed","boolean")),
)

def transform_and_validate_sales_orders(df,*,stream_batch_id,pipeline_run_id,processed_at=None): return transform_dataset(df,spec=SPEC,stream_batch_id=stream_batch_id,pipeline_run_id=pipeline_run_id,processed_at=processed_at)
def create_valid_silver_df(df): return create_valid_df(df,SPEC)
def create_rejected_records_df(df): return create_rejected_df(df,SPEC)
def deduplicate_valid_records(df):
 from pyspark.sql import functions as F
 from pyspark.sql.window import Window
 ranked=df.withColumn("_rank",F.row_number().over(Window.partitionBy("sales_order_id").orderBy(F.col("write_date").desc(),F.col("source_batch_id").desc(),F.col("source_file").desc())))
 return ranked.filter("_rank = 1").drop("_rank"),ranked.filter("_rank > 1").drop("_rank")
def create_sales_orders_tables(spark,config): return create_dataset_tables(spark,spec=SPEC,config=config)
def merge_sales_orders_to_silver(spark,df,*,config,stream_batch_id,logger): return merge_dataset_to_silver(spark,df,spec=SPEC,config=config,stream_batch_id=stream_batch_id,logger=logger)
def build_sales_orders_batch_processor(*,config,logger=None): return build_dataset_batch_processor(spec=SPEC,config=config,logger=logger)
