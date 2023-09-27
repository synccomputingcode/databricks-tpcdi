# Databricks notebook source
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("dbt_schema", "")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.sdk import WorkspaceClient

# get parameters
warehouse_id = dbutils.widgets.get("warehouse_id")  
catalog_name = dbutils.widgets.get("catalog_name")
dbt_schema = dbutils.widgets.get("dbt_schema")
batch_id = dbutils.widgets.get("batch_id")
audit_schema = 'dbt_tpcdi_audit'

# Databricks client
w = WorkspaceClient()

# create audit_schema
spark.sql(f"create schema if not exists {catalog_name}.{audit_schema}").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC The dbt project has the Elementary package applied.
# MAGIC This is a data observability framework written in dbt to collect run results and execution times, as well as doing anomaly detection.
# MAGIC I hijacked some of the built-in columns in Elementary to store things like warehouse size and warehouse_id

# COMMAND ----------

df = (
    spark.sql(f"select *, vars:scaling_factor from {catalog_name}.`{dbt_schema}`.dbt_invocations")
    .withColumn('warehouse_size', lower('job_id'))
    .withColumn('warehouse_type', lower('job_name'))
    .withColumn('warehouse_id', lower('job_run_id'))
    .withColumn('batch_id', lit(batch_id))
    .drop('job_id','job_name','job_run_id')
    .write.mode('append')
    .option('mergeSchema','true')
    .format("delta")
    .saveAsTable(f"{catalog_name}.{audit_schema}.dbt_invocations")
    # .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC The only thing I add to the `model_run_results` table is the Medallion zone based on schema name.
# MAGIC This might be helpful in the future for isolating runtimes by zone.

# COMMAND ----------

df = (
    spark.table(f"{catalog_name}.`{dbt_schema}`.model_run_results")
    .withColumn('zone', regexp_extract('schema_name', '(dl_)(.+)(_)(.+)', 4))
    .withColumn('zone', when(col('zone') == '', None).otherwise(col('zone')))
    .write.mode('append')
    .format("delta")
    .option('mergeSchema','true')
    .saveAsTable(f"{catalog_name}.{audit_schema}.model_run_results")
    # .display()
)

# COMMAND ----------

# delete the schemas
for zone in ['_gold', '_silver', '_bronze', '']:
    w.statement_execution.execute_statement(
        statement=f"drop schema if exists {catalog_name}.`{dbt_schema}{zone}` cascade",
        warehouse_id=warehouse_id
    )

# COMMAND ----------

# delete the warehouse
w.warehouses.delete(warehouse_id)
