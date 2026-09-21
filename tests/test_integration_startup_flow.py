from pathlib import Path

import yaml


def _load_compose() -> dict:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    return yaml.safe_load(compose_path.read_text())


def test_compose_startup_order_is_safe_for_runtime_dependencies() -> None:
    """The stack must start from storage services to the scheduler and API."""
    compose = _load_compose()
    services = compose["services"]

    expected_start_order = [
        "minio",
        "clickhouse-server",
        "postgres",
        "scheduler",
        "webserver",
        "databaseapi",
    ]

    for service_name in expected_start_order:
        assert service_name in services, f"Service {service_name} is missing from docker-compose."

    assert "healthcheck" in services["minio"], "MinIO must expose a healthcheck."
    assert "healthcheck" in services["clickhouse-server"], "ClickHouse must expose a healthcheck."
    assert "healthcheck" in services["postgres"], "Postgres must expose a healthcheck."
    assert "healthcheck" in services["scheduler"], "Airflow scheduler must expose a healthcheck."

    clickhouse_depends = services["clickhouse-server"].get("depends_on", {})
    assert "minio" in clickhouse_depends, "ClickHouse depends on MinIO."
    assert clickhouse_depends["minio"].get("condition") == "service_healthy"

    scheduler_depends = services["scheduler"].get("depends_on", {})
    for dependency in ("postgres", "clickhouse-server", "minio"):
        assert dependency in scheduler_depends, f"Scheduler depends on {dependency}."
        assert scheduler_depends[dependency].get("condition") == "service_healthy"


def test_bootstrap_and_daily_dbt_are_separated_by_design() -> None:
    """Startup bootstrap is a cold-start workflow, while daily dbt includes a guard check."""
    compose = _load_compose()
    assert "bootstrap_init_reference_tables" not in compose["services"], (
        "The bootstrap is a dedicated DAG, not a service in the Compose stack."
    )

    dag_files = [
        Path(__file__).resolve().parents[1] / "dags" / "bootstrap_init_reference_tables_dag.py",
        Path(__file__).resolve().parents[1] / "dags" / "transform_dbt_models_clickhouse_dag.py",
    ]
    for dag_file in dag_files:
        assert dag_file.exists(), f"Missing DAG file: {dag_file}"

    bootstrap_text = (Path(__file__).resolve().parents[1] / "dags" / "bootstrap_init_reference_tables_dag.py").read_text()
    daily_text = (Path(__file__).resolve().parents[1] / "dags" / "transform_dbt_models_clickhouse_dag.py").read_text()

    assert "bootstrap_init_reference_tables" in bootstrap_text
    assert "ensure_reference_tables_ready" in daily_text
    assert "bootstrap_init_reference_tables" not in daily_text


def test_airflow_scheduler_healthcheck_runs_through_poetry() -> None:
    """The scheduler healthcheck must use the Poetry-managed Airflow CLI."""
    compose = _load_compose()
    scheduler_health = compose["services"]["scheduler"]["healthcheck"]["test"]
    assert scheduler_health[0] == "CMD-SHELL"

    healthcheck_cmd = scheduler_health[1]
    assert "poetry run airflow jobs check --job-type SchedulerJob" in healthcheck_cmd
    assert "CMD-SHELL airflow jobs check" not in healthcheck_cmd


def test_reference_tables_are_declared_as_required_startup_prerequisites() -> None:
    """Reference tables are not optional: they are expected by dbt and the startup guard."""
    project_root = Path(__file__).resolve().parents[1]

    sources_yml = (project_root / "dbt_project" / "models" / "source" / "sources.yml").read_text()
    sql_file = (project_root / "project_functions" / "sql" / "create_location_table.sql").read_text()

    assert "raw_depcode" in sources_yml
    assert "raw_depcode_" in sources_yml
    assert "CREATE TABLE IF NOT EXISTS raw_depcode" in sql_file
    assert "CREATE TABLE IF NOT EXISTS raw_depcode_" in sql_file


def test_dbt_pipeline_requires_reference_data_before_fact_models() -> None:
    """The dbt staging and intermediate models must depend on ref tables that are created before fact build."""
    project_root = Path(__file__).resolve().parents[1]
    staging_depcode = (project_root / "dbt_project" / "models" / "staging" / "stg_depcode.sql").read_text()
    staging_depcode_alt = (project_root / "dbt_project" / "models" / "staging" / "stg_depcode_.sql").read_text()
    mart_newdata = (project_root / "dbt_project" / "models" / "mart" / "analytics-team" / "mart_newdata.sql").read_text()

    assert "raw_depcode" in staging_depcode
    assert "raw_depcode_" in staging_depcode_alt
    assert "int_depcode" in mart_newdata
