import os
import sys
import pendulum
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="trigger_new_dags",
    tags=["trigger and dags synchronizer"],
    start_date=pendulum.datetime(2026, 3, 22, tz="UTC"),
    catchup=False,
    schedule_interval=None,
) as dag:

    start_task = EmptyOperator(task_id="start_orchestration_project")

    trigger_A = TriggerDagRunOperator(
        task_id="trigger_python_etl",
        trigger_dag_id="python_etl",
        logical_date="{{ ts }}",
    )

    trigger_B = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id="dbt_transformation",
        logical_date="{{ ts }}",
    )

    start_task >> [trigger_A, trigger_B]
