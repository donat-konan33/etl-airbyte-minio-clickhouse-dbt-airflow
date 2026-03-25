import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from python.etl import extract_transform, loader
import asyncio
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from pathlib import Path
from typing import Literal, Union
import json
import pendulum


project_root = Path().resolve()
payload_path = project_root / "airbyte/france_departments_chunks.json"
with open(str(payload_path), "r") as file:
    payload = json.load(file)
    n_chunks = len(payload)

raw_weather__schema_path = project_root / "schemas/clickhouse/raw_weather__schema.json"
with open(str(raw_weather__schema_path), "r") as file:
    schema = json.load(file)
    columns = [column['name'] for column in schema['DESCRIBE TABLE raw_weather_']]

def et_task(n_chunk: Union[int, Literal["all"]]):
    """
    Asynchronous data extraction and transformation (almost data structure and departments name)
    n_chunk should be under the max length of existing chunks
    n_chunk should be between 0 and 20
    """
    print(payload)
    if isinstance(n_chunk, int):
        if n_chunk > len(payload):
            raise ValueError(f"The number of chunk you want will not be reached, choose a number under or equal to {len(payload)}")
        return extract_transform(n_chunks=n_chunk)

    if n_chunk == "all":
        return extract_transform(n_chunks=n_chunks)


with DAG(
    dag_id="python_etl",
    tags=["etl from api to clickhouse and minio with python"],
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2026, 3, 22, tz="UTC"),
    catchup=False
) as dag:

    extract_transform_task = PythonOperator(
        task_id="extract_transform",
        python_callable=et_task,
        op_kwargs={
            "n_chunk": "all" # 2 for test, in prod "all" is desired
        }
    )

    def load_data_to_minio(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids="extract_transform")
        loader(data, target='minio')

    def load_data_to_clickhouse(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids="extract_transform")
        # get only these columns
        data = data[columns]
        loader(data, target='clickhouse')

    load_minio_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_data_to_minio,
        provide_context=True
    )

    load_clickhouse_task = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_data_to_clickhouse,
        provide_context=True
    )

    end_task = EmptyOperator(
        task_id="loadings_done",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

# récupérer les données extraites et stockées dans le data lake : changer la fonction de load
    extract_transform_task >> [load_minio_task, load_clickhouse_task] >> end_task
