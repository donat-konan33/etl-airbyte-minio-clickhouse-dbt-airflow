

from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.python import PythonOperator
import os
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_DIR = os.environ.get("DBT_DIR")

import sys
sys.path.append(AIRFLOW_HOME)

from project_functions.python.clickhouse_crud import ClickHouseQueries

with DAG(
    dag_id="dbt_models_clickhouse",
    tags=["dbt on ClickHouse"],
    default_args={'owner': 'dbt'},
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    schedule_interval="0 2 * * *", #CronTriggerTimetable("0 2 * * *", timezone="UTC"),
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    wait_for_loading_recent_data_to_warehouse_sensor = ExternalTaskSensor(
        task_id="wait_for_load_data_from_datalake_to_clickhouse_sensor",
        external_dag_id="load_data_from_datalake_to_clickhouse",
        external_task_id="end_of_load_to_clickhouse",
        mode = 'poke',
        poke_interval=10,
        timeout=1200,
        allowed_states=["success"]
    )

    ensure_reference_tables_ready = PythonOperator(
        task_id="ensure_reference_tables_ready",
        python_callable=ClickHouseQueries().ensure_reference_tables_ready,
    )

    dbt_transformation = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build -m +mart_newdata --project-dir {DBT_DIR}"
    )

    append_martnewdata_to_all_weather = PythonOperator(
        task_id="append_martnewdata_to_all_weather",
        python_callable=ClickHouseQueries().merge_daily_data,
        op_kwargs={
            "table_name": "mart_newdata",
            "target_table_name": "archived_data"  # Assuming 'archived_data' is the target table
        }
    )

    wait_for_loading_recent_data_to_warehouse_sensor >> ensure_reference_tables_ready >> dbt_transformation >> append_martnewdata_to_all_weather
