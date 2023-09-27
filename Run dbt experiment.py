# Databricks notebook source
dbutils.widgets.multiselect('size_list','Small',['2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large'],'Warehouse Size')
dbutils.widgets.multiselect('compute_list','SERVERLESS',['SERVERLESS', 'PRO'],'Warehouse Type')
dbutils.widgets.dropdown('scaling_factor','1000',['10','100','1000'],'Scaling Factor')
dbutils.widgets.text('catalog_name','stewart','Catalog Name')
dbutils.widgets.text('creator','stewart.bryson@synccomputing.com','Creator')
dbutils.widgets.multiselect('threads_list','8', ['4', '8', '16', '24', '32', '40', '48'])
dbutils.widgets.text('iterations','1')

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

import itertools as its
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from databricks.sdk.core import DatabricksError
import json
import httpx
import uuid

# get a few widget values
catalog_name = dbutils.widgets.get('catalog_name')
scaling_factor:int = dbutils.widgets.get('scaling_factor')
creator = dbutils.widgets.get('creator')
# set some defaults
job_name = "dbt-sql-tpcdi"
dbt_task_key = "run-dbt-tpcdi"
schema_prefix = 'dl'
cleanup_task_key = "delete-schemas-and-warehouse"
cluster_key = f"{job_name}-cluster"
node_size = "m5d.large"
# secrets are secured
token = dbutils.secrets.get(scope = "cli_stewart", key = "token")
db_instance = dbutils.secrets.get(scope = "cli_stewart", key = "db_instance")
create_api = f"https://{db_instance}/api/2.0/jobs/create"
run_api = f"https://{db_instance}/api/2.0/jobs/run-now"
job_cluster = cluster_key

# get a UUID for the current batch
batch_id = str(uuid.uuid1())

# Databricks client
w = WorkspaceClient()


# COMMAND ----------


def create_warehouse(
        warehouse_name: str,
        size: str,
        warehouse_type: str,
):
    serverless = False
    warehouse_id = ''
    
    if warehouse_type.upper() == 'SERVERLESS':
        compute_type = sql.GetWarehouseResponseWarehouseType('PRO')
        serverless = True
    else:
        compute_type = sql.GetWarehouseResponseWarehouseType(warehouse_type.upper())

    try:
        created = (
            w.warehouses.create(
                name=warehouse_name,
                warehouse_type=compute_type,
                enable_serverless_compute=serverless,
                cluster_size=size,
                max_num_clusters=1,
                auto_stop_mins=10
            )
            .result()
        )
        warehouse_id = w.warehouses.get(id=created.id).id
        print(f"Warehouse {warehouse_name} with id {warehouse_id} created.")

    except DatabricksError as err:
        if str(err) == f"SQL warehouse with name `{warehouse_name}` already exists":
            for warehouse in w.warehouses.list():
                if warehouse.name == warehouse_name:
                    # restart the warehouse
                    # this should never actually happen, but is here for failed run restarts
                    print(f"Warehouse {warehouse_name} already exists. Restarting...")
                    warehouse_id = warehouse.id
                    w.warehouses.stop(warehouse_id)
                    w.warehouses.start(warehouse_id)

    return warehouse_id

def run_experiment(
        size:str,
        warehouse_type:str,
        threads:int,
        iteration:int
):
    # create the warehouse
    # this is still serialized.
    # Serverless spins up fast, but Pro does not and can be a serious constraint.
    warehouse_name = (f"tpcdi-{warehouse_type}-{size}-{threads}-{iteration}").lower()
    warehouse_id = create_warehouse(warehouse_name, size, warehouse_type)
    dbt_schema = f"{schema_prefix}_{warehouse_name}"

    # prepare the dbt task
    dbt_task = (
        f"dbt build --fail-fast --threads {threads}"
        + " --vars '{"
        + f'"orchestrator": "Databricks", '
        + f'"job_name": "{warehouse_type}", '
        + f'"job_id": "{size}", '
        + f'"job_run_id": "{warehouse_id}", '
        + f'"scaling_factor": {scaling_factor}'
        + "}'"
    )

    #print(dbt_task)
    job_name_id = f"{job_name}-{warehouse_name}"

    # prepare the job JSON
    job = {
            "run_as": {
                "user_name": creator
            },
            "name": job_name_id,
            "email_notifications": {
                "no_alert_for_skipped_runs": False
            },
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": dbt_task_key,
                    "run_if": "ALL_SUCCESS",
                    "dbt_task": {
                        "project_directory": "",
                        "commands": [
                            "dbt deps",
                            dbt_task
                        ],
                        "schema": dbt_schema,
                        "warehouse_id": warehouse_id,
                        "catalog": catalog_name
                    },
                    "job_cluster_key": cluster_key,
                    "libraries": [
                        {
                            "pypi": {
                                "package": "dbt-databricks==1.6.4"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                        "no_alert_for_skipped_runs": False,
                        "no_alert_for_canceled_runs": False,
                        "alert_on_last_attempt": False
                    }
                },
                {
                    "task_key": cleanup_task_key,
                    "depends_on": [
                        {
                            "task_key": dbt_task_key
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "/Users/stewart.bryson@synccomputing.com/Run dbt experiment cleanup",
                        "base_parameters": {
                            "warehouse_id": warehouse_id,
                            "catalog_name": catalog_name,
                            "dbt_schema": dbt_schema,
                            "batch_id": batch_id
                        
                        },
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": job_cluster,
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                        "no_alert_for_skipped_runs": False,
                        "no_alert_for_canceled_runs": False,
                        "alert_on_last_attempt": False
                    }
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": cluster_key,
                    "new_cluster": {
                        "cluster_name": "",
                        "spark_version": "14.0.x-scala2.12",
                        "aws_attributes": {
                            "first_on_demand": 1,
                            "availability": "SPOT_WITH_FALLBACK",
                            "zone_id": "us-east-1b",
                            "spot_bid_price_percent": 100,
                            "ebs_volume_count": 0
                        },
                        "node_type_id": node_size,
                        "enable_elastic_disk": False,
                        "data_security_mode": "SINGLE_USER",
                        "runtime_engine": "PHOTON",
                        "num_workers": 2
                    }
                }
            ],
            "git_source": {
                "git_url": "https://github.com/stewartbryson/databricks-tpcdi.git",
                "git_provider": "gitHub",
                "git_branch": "databricks-job"
            },
            "tags": {
                "scaling_factor": scaling_factor,
                "warehouse_id": warehouse_id,
                "warehouse_size": size,
                "warehouse_type": warehouse_type,
                "job_name": job_name,
                "batch_id": batch_id
            },
            "format": "MULTI_TASK"
        }
    job = json.dumps(job, indent=2)
    
    # create the job
    rc = httpx.post(create_api, data=job, auth=("token", token))
    print(rc.text)
    job_id = json.loads(rc.text)['job_id']

    # run the job
    run = json.dumps({"job_id": job_id})
    rr = httpx.post(run_api, data=run, auth=("token", token))
    print(json.loads(rr.text))
    print(f"Job ID {job_id} submitted.")
    return job_id


# COMMAND ----------

# one off experiment
size_list = dbutils.widgets.get('size_list')
size_list = size_list.split(',')
compute_list = dbutils.widgets.get('compute_list')
compute_list = compute_list.split(',')
threads_list = dbutils.widgets.get('threads_list')
threads_list = threads_list.split(',')
threads_list = [eval(i) for i in threads_list]
iterations = int(dbutils.widgets.get('iterations'))

# generate iterations
index = 0
iteration_list = []
for _ in range(iterations):
    index += 1
    iteration_list.append(index)

# run all the different permutations
# submit them as dbt + notebook(cleanup) tasks
p = its.product(size_list, compute_list, threads_list, iteration_list)
job_ids = []
for size, warehouse_type, threads, iteration in p:

    print(size, warehouse_type, threads, iteration)
    try:
        job_id = run_experiment(size, warehouse_type, threads, iteration)
        job_ids.append(job_id)
    except Exception as err:
        print(f"Exception for iteration {size}, {warehouse_type}, {threads}: \n{str(err)}")

print(f"All Job IDS: {job_ids}")
print(f"Batch ID is {batch_id}.")


# COMMAND ----------

# MAGIC %md
# MAGIC Execution stop is a placeholder for the cleanup cells that will go below.

# COMMAND ----------

#dbutils.notebook.exit("Ensure all jobs run successfully before building results table")
