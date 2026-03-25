import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_DIR = os.environ.get("DBT_DIR")

sys.path.append(AIRFLOW_HOME) # help python retrieving AIRFLOW_HOME

from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="dbt_transformation",
    tags=["dbt actions in ClickHouse"],
    default_args={'owner': 'dbt'},
    start_date=pendulum.datetime(2026, 3, 22, tz="UTC"),
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    wait_for_loading_data_to_warehouse_sensor = ExternalTaskSensor(
        task_id="wait_for_loading_data_sensor",
        external_dag_id="python_etl",
        external_task_id="loadings_done",
        mode = 'poke',
        poke_interval=10,
        timeout=1200,
        allowed_states=["success"]
    )

    dbt_transformation = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build -m +mart_newdata_+ --project-dir {DBT_DIR}"
    )

    wait_for_loading_data_to_warehouse_sensor >> dbt_transformation
