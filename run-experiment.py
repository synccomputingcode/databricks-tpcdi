from dbt.cli.main import dbtRunner, dbtRunnerResult
import itertools as its
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from databricks.sdk.core import DatabricksError
import concurrent.futures as futures

# Databricks client
w = WorkspaceClient()
# dbt runner
dbt = dbtRunner()

# set the audit catalog
audit_catalog = 'stewart'
audit_schema = 'tpcdi_audit'
audit_table = 'model_executions'
scaling_factor = 10

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

# create the audit schema if it doesn't exist
try:
    audit_schema = w.schemas.create(name=f'tpcdi_audit', catalog_name=audit_catalog)
except DatabricksError as err:
     print(str(err))

# create a warehouse for audit puposes
audit_warehouse_id = audit_warehouse = create_warehouse('X-Small','serverless')

# create the audit table
w.statement_execution.execute_statement(
    statement=f"""create table if not exists {audit_catalog}.{audit_schema}.{audit_table} 
                (experiment_id STRING, warehouse_size STRING, warehouse_type STRING, threads INT, schema_name STRING, model_name STRING, execution_time FLOAT, scaling_factor INT )""",
    warehouse_id=audit_warehouse_id
)

# generate list of test cases
#size_list = ['2X-Small', 'X-Small', 'Small', 'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large']
size_list = ['Small']
#compute_list = ['SERVERLESS', 'PRO', 'CLASSIC']
compute_list = ['SERVERLESS']
#thread_list = [8, 16, 24, 32]
thread_list = [16]

p = its.product(size_list, compute_list, thread_list)

index = 0
for compute_size, compute_type, thread_case in p:
    index += 1

    # create the warehouse
    warehouse_id = create_warehouse(compute_size, compute_type, thread_case)
    vars = '{"warehouse_id": ' + f'"{warehouse_id}", ' + f'"schema": "{warehouse_id}"' + '}'

    # run the dbt workflow
    #args = ["--fail-fast", "build", "--vars", vars, "--threads", thread_case, "--target", "dynamic"]
    args = ["--fail-fast", "build", "--vars", vars, "--threads", thread_case, "--target", "dynamic", '--select', 'hr_employee reference_trade_type']
    res: dbtRunnerResult = dbt.invoke(args)
    for r in res.result:
        #print(f"{r}")
        sql_statement = f"""insert into {audit_catalog}.{audit_schema}.{audit_table} 
                        values ('{warehouse_id}', '{compute_size}', '{compute_type.lower()}', {thread_case}, '{r.node.schema}', '{r.node.name}', {r.execution_time}, {scaling_factor})"""
        #print(sql_statement)
        insert = w.statement_execution.execute_statement(
            statement=sql_statement,
            warehouse_id=audit_warehouse_id
        )
        print(insert)

    # delete the schemas
    for zone in ['gold', 'silver', 'bronze']:
        w.statement_execution.execute_statement(
            statement=f"drop schema stewart.dl_{warehouse_id}_{zone} cascade",
            warehouse_id=warehouse_id
        )

    # delete the warehouse
    w.warehouses.delete(warehouse_id)

# delete audit warehouse
w.warehouses.delete(audit_warehouse_id)
