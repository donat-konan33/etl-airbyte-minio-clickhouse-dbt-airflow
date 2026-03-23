from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="trigger_dags",
    start_date=datetime(2025, 7, 10),
    catchup=False,
    schedule_interval=None,
) as dag:

    start_task = EmptyOperator(task_id="start")

    trigger_A = TriggerDagRunOperator(
        task_id="trigger_airbyte_sync_data",
        trigger_dag_id="http_sync_dag",
        logical_date="{{ ts }}",
    )

    trigger_B = TriggerDagRunOperator(
        task_id='trigger_minio_optimization',
        trigger_dag_id="airbyte_data_transform",
        logical_date="{{ ts }}",
    )

    trigger_C = TriggerDagRunOperator(
        task_id="trigger_load_data_from_datalake_to_clickhouse",
        trigger_dag_id="load_data_from_datalake_to_clickhouse",
        logical_date="{{ ts }}",
    )

    trigger_D = TriggerDagRunOperator(
        task_id='trigger_dbt_models_clickhouse',
        trigger_dag_id="dbt_models_clickhouse",
        logical_date="{{ ts }}",
    )

    start_task >> [trigger_A, trigger_B, trigger_C, trigger_D]
