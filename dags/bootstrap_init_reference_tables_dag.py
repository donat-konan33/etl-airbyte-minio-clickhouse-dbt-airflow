import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
if AIRFLOW_HOME:
    sys.path.append(AIRFLOW_HOME)

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

from project_functions.python.clickhouse_crud import ClickHouseQueries

with DAG(
    dag_id="bootstrap_init_reference_tables",
    tags=["bootstrap", "clickhouse", "reference data"],
    default_args={"owner": "data-platform"},
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    schedule_interval=None,
    catchup=False,
) as dag:

    bootstrap_reference_tables = PythonOperator(
        task_id="bootstrap_reference_tables_ready",
        python_callable=ClickHouseQueries().bootstrap_init_reference_tables,
    )

    bootstrap_reference_tables
