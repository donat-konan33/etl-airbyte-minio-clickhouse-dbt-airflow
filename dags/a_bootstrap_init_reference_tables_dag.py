import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
if AIRFLOW_HOME:
    sys.path.append(AIRFLOW_HOME)

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

from project_functions.python.clickhouse_crud import ClickHouseQueries

def load_department_reference():
    """Load department reference data into raw_depcode_"""
    queries = ClickHouseQueries()
    queries._load_department_reference_data()
    print("✓ Department reference data loaded successfully")

def validate_reference_tables():
    """Validate that all reference tables are ok and ready for ETL"""
    queries = ClickHouseQueries()
    queries.bootstrap_init_reference_tables()
    print("✓ All reference tables validated and ready")

with DAG(
    dag_id="bootstrap_init_reference_tables",
    tags=["bootstrap", "clickhouse", "reference data"],
    default_args={"owner": "data-platform"},
    start_date=pendulum.datetime(2026, 9, 27, tz="UTC"),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_departments_task = PythonOperator(
        task_id="load_department_reference",
        python_callable=load_department_reference,
        doc="Loads French department codes and geographic data into raw_depcode_"
    )

    validate_tables_task = PythonOperator(
        task_id="validate_reference_tables",
        python_callable=validate_reference_tables,
        doc="Validates that raw_depcode_ and raw_weather_ tables are really created and raw_depcode_ is populated before etl"
    )

    load_departments_task >> validate_tables_task
