import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.minio_client import get_minio_client
from project_functions.python.minio_utils import MinioUtils
from project_functions.python.clickhouse_crud import ClickHouseQueries

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum

object_name = "staging/daily/france_weather_daily_data_extracted.parquet"


def retrieve_daily_data(**kwargs):
    return MinioUtils(bucket_name="weather").retrieve_parquet_data(object_name=object_name)

with DAG(
    dag_id="load_data_from_datalake_to_clickhouse",
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2025, 7, 19, tz="UTC"),
    catchup=False
    ) as dag:

    # wait for MinIO data retrieval to complete
    wait_for_transform_datalake_task = ExternalTaskSensor(
        task_id="wait_for_transform_datalake",
        external_dag_id="airbyte_data_transform",
        external_task_id="transformations_done",
        mode="poke",
        timeout=3600,
        poke_interval=30
    )

    # retrieve data from MinIO daily directory
    retrieving_data_task = PythonOperator(
        task_id="retrieve_daily_data",
        python_callable=retrieve_daily_data,
    )

    # store data into ClickHouse
    def store_data_to_clickhouse(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids="retrieve_daily_data")
        ClickHouseQueries().load_data_to_clickhouse(table_name="raw_weather", data=data, is_to_truncate=True)

    storing_data_task = PythonOperator(
        task_id="store_data",
        python_callable=store_data_to_clickhouse,
        provide_context=True
    )
    end_task = EmptyOperator(task_id="end_of_load_to_clickhouse", trigger_rule=TriggerRule.ALL_SUCCESS)  # Ensure this task runs only if all previous tasks succeed

    wait_for_transform_datalake_task >> retrieving_data_task >> storing_data_task >> end_task
