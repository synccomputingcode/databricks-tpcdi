import itertools as its
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from databricks.sdk.core import DatabricksError
import concurrent.futures as futures
from pathlib import Path
import json

# set some of the defaults
catalog_name = 'stewart'
scaling_factor = 10
job_name = "dbt-sql-tpcdi-experiment"
task_key = f"{job_name}-1"
cluster_key = f"{task_key}-cluster"
creator = "stewart.bryson@synccomputing.com"
node_size = "m5d.large"

# Databricks client
w = WorkspaceClient()

def create_warehouse(
        size: str,
        warehouse_type: str,
        threads: int = 0,
):
    serverless = False
    warehouse_id = ''
    warehouse_name = (f"tpcdi-{warehouse_type}-{size}-{threads}").lower()
    
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
                    warehouse_id = warehouse.id
            # restart warehouse

    return warehouse_id

def run_experiment(
        size:str,
        warehouse_type:str,
        threads:int
):
    # create the warehouse
    warehouse_id = create_warehouse(size, warehouse_type, threads)

    # prepare the Job JSON
    dbt_task = (
        "dbt build --fail-fast --vars '{"
        + f'"orchestrator": "Databricks", '
        + f'"job_name": "{warehouse_type}", '
        + f'"job_id": "{size}", '
        + f'"job_run_id": "{warehouse_id}", '
        + f'"scaling_factor": {scaling_factor}'
        + "}'"
    )

    dbt_elem_task = dbt_task + ' --select elementary'

    # print(dbt_task)
    # print(dbt_elem_task)

    job = {
            "run_as": {
                "user_name": creator
            },
            "name": job_name,
            "email_notifications": {
                "no_alert_for_skipped_runs": False
            },
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": task_key,
                    "run_if": "ALL_SUCCESS",
                    "dbt_task": {
                        "project_directory": "",
                        "commands": [
                            "dbt deps",
                            dbt_task,
                            dbt_elem_task,
                        ],
                        "schema": "dl",
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
                        "num_workers": 8
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
                "job_name": job_name
            },
            "format": "MULTI_TASK"
        }
    job = json.dumps(job, indent=2)
    print(job)

    # delete the schemas
    for zone in ['gold', 'silver', 'bronze']:
        w.statement_execution.execute_statement(
            statement=f"drop schema if exists stewart.dl_{warehouse_id}_{zone} cascade",
            warehouse_id=warehouse_id
        )

    # delete the warehouse
    w.warehouses.delete(warehouse_id)

# generate list of test cases
#size_list = ['2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large']
#size_list = ['Small', 'Medium', 'Large', 'X-Large']
size_list = ['Small']
#compute_list = ['SERVERLESS', 'PRO', 'CLASSIC']
compute_list = ['SERVERLESS']
#thread_list = [8, 16, 24, 32]
thread_list = [16]

# run all the different permutations
p = its.product(size_list, compute_list, thread_list)

for size, warehouse_type, threads in p:
    run_experiment(size, warehouse_type, threads)

