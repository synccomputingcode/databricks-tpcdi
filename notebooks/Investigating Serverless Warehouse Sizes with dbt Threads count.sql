-- Databricks notebook source
-- MAGIC %md
-- MAGIC I'm interested in the effect of Serverless Warehouse Sizes when using dbt.
-- MAGIC I'm also interested in the effect that the number of parallel dbt DAG executions (called "threads") has on the different warehouse sizes.
-- MAGIC
-- MAGIC I ran 3 iterations of each of the following perumutations:
-- MAGIC
-- MAGIC Warehouse size: ['2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large']
-- MAGIC
-- MAGIC Threads count: ['4', '8', '16', '24', '32', '40', '48']
-- MAGIC
-- MAGIC Each iteration X permutation ran in an isolated Serverless SQL warehouse, created immediately prior to the run, and deleted immediately afterwards.
-- MAGIC Each iteration X permutation ran from isolated job compute.

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.widgets.text("batch_id", "")
-- MAGIC dbutils.widgets.text("catalog_name", "stewart")
-- MAGIC dbutils.widgets.text("audit_schema", "dbt_tpcdi_audit")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set CATALOG and SCHEMA.

-- COMMAND ----------

use catalog ${catalog_name};
use schema ${audit_schema};

-- COMMAND ----------

select batch_id, count(*) cnt from dbt_invocations group by all order by cnt desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a temporary view for job executions. This is one row for every `dbt build` executed, which is unique using `invocation_id`. This table is generated by the [Elementary](https://docs.elementary-data.com/quickstart) dbt observability package, and captures only dbt execution metrics.

-- COMMAND ----------

create
or replace temporary view job_execution_results as with s1 as (
  select
    *
  except
    (run_started_at, run_completed_at),
    to_timestamp(run_started_at) run_started_at,
    to_timestamp(run_completed_at) run_completed_at
  from
    dbt_invocations
  where
    batch_id = '${batch_id}'
    -- removing execution with a single thread
    -- it throws the entire graph off
    and threads <> 1
    -- medium results can't be trusted
    --and warehouse_size <> 'medium'
)
select
  invocation_id,
  warehouse_id,
  batch_id,
  scaling_factor,
  warehouse_size,
  warehouse_type,
  threads,
  case
    warehouse_size
    when '2x-small' then 1
    when 'x-small' then 2
    when 'small' then 3
    when 'medium' then 4
    when 'large' then 5
    when 'x-large' then 6
    when '2x-large' then 7
    when '3x-large' then 8
    when '4x-large' then 9
  end warehouse_size_metric,
  timestampdiff(second, run_started_at, run_completed_at) duration
from
  s1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a temporary view for the individual model execution results. The column `seconds` can be aggregated to an individual `invocation_id` to give aggregate processing time, but this is not the same as the `duration` from job runs because of multi-threading.

-- COMMAND ----------

create
or replace temporary view model_execution_results as with s1 as (
  select
    invocation_id,
    warehouse_id,
    warehouse_type,
    warehouse_size,
    threads,
    warehouse_size_metric,
    execution_time seconds
  from
    model_run_results
    join job_execution_results using (invocation_id)
  -- where
  --   package_name = 'tpcdi'
  order by
    warehouse_size_metric asc,
    threads asc
)
select
  *
from
  s1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A scatterplot showing duration versus number of threads.
-- MAGIC Click the legend on the upper right to isolate individual warehouse sizes.
-- MAGIC The working theory is that more concurrent processes mean that each individual process underperforms.
-- MAGIC
-- MAGIC Notice that the Medium warehouse size vastly overperforms compared against warehouses that are larger.

-- COMMAND ----------

select
  --invocation_id,
  warehouse_size,
  threads,
  avg(round(duration / 60, 2)) minutes,
  warehouse_size_metric
from
  job_execution_results
group by
  all
order by
  warehouse_size_metric asc,
  threads asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we look at the average runtime for models by thread count and warehouse size, we see that, as thread count goes up, the models tend to run longer.
-- MAGIC The working theory is the same: more concurrent processes mean that each individual process underperforms.
-- MAGIC
-- MAGIC Notice that the Medium warehouse size vastly overperforms compared against warehouses that are larger.

-- COMMAND ----------

select warehouse_size, threads, avg(seconds) seconds from model_execution_results group by all;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a temporary view that combines `job_execution_rsults` with cost data from the system tables. This is done by pulling the `warehouse_id` from the Serverless warehouse metadata which is also instrumented in the dbt collection data.

-- COMMAND ----------

create or replace temporary view job_execution_costs
as
with s1 as (
  select
    *,
    cast(pricing.default as float) * usage_quantity cost,
    usage_metadata.warehouse_id warehouse_id
  from
    system.billing.usage
    join system.billing.list_prices using (sku_name, account_id)
  order by
    usage_start_time desc
),
s2 as (
  select
    warehouse_id,
    sum(cost) cost
  from
    s1
  group by
    all
),
s3 as (
  select
    warehouse_id,
    invocation_id,
    scaling_factor,
    warehouse_size,
    warehouse_size_metric,
    warehouse_type,
    threads,
    duration
  from job_execution_results
    group by
    all
)
select
  *
from
  s2
  join s3 using (warehouse_id)
order by
  warehouse_size_metric,
  threads;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This bubble chart adds `cost` to the visualization, and bubbles thread count into the Scatter Plot.
-- MAGIC Notice again that Medium is truly an outlier and I'm not sure why that is.
-- MAGIC
-- MAGIC The conclusion to draw from this is that workloads are fickle, and the optimal setting for thread count depends on the warehouse size used.
-- MAGIC And the decision on which warehouse size to use depends on the SLA for duration combined with budget.

-- COMMAND ----------

select warehouse_size, warehouse_size_metric, threads, avg(cost) cost, avg(round(duration / 60, 2)) minutes from job_execution_costs group by all;
