import os
from pathlib import Path


def test_core_runtime_hosts_are_declared_for_internal_docker_connections() -> None:
    required = {
        "MINIO_HOST": "minio",
        "MINIO_HOST_IP": "minio",
        "CLICKHOUSE_HOST": "clickhouse-server",
        "CLICKHOUSE_HOST_IP": "clickhouse-server",
        "CLICKHOUSE_HOSTNAME": "clickhouse-server",
        "POSTGRES_HOST": "postgres",
    }

    for key, expected in required.items():
        assert os.environ.get(key) in {expected, None}, (
            f"{key} should resolve to the Docker service name '{expected}' for internal networking."
        )


def test_core_runtime_ports_are_exposed_for_services() -> None:
    required_ports = {
        "MINIO_API_PORT": "9000",
        "CLICKHOUSE_HTTP_PORT": "8123",
        "CLICKHOUSE_NATIVE_PORT": "19000",
        "POSTGRES_PORT": "5432",
        "AIRFLOW_PORT": "8080",
        "API_PORT": "8005",
    }

    for key, expected in required_ports.items():
        value = os.environ.get(key)
        assert value in {expected, None}, f"{key} must use the published port {expected}."


def test_published_service_ports_are_unique() -> None:
    published_ports = {
        "MINIO_API_PORT": os.environ.get("MINIO_API_PORT"),
        "MINIO_CONSOLE_PORT": os.environ.get("MINIO_CONSOLE_PORT"),
        "CLICKHOUSE_HTTP_PORT": os.environ.get("CLICKHOUSE_HTTP_PORT"),
        "CLICKHOUSE_NATIVE_PORT": os.environ.get("CLICKHOUSE_NATIVE_PORT"),
        "POSTGRES_PORT": os.environ.get("POSTGRES_PORT"),
        "AIRFLOW_PORT": os.environ.get("AIRFLOW_PORT"),
        "API_PORT": os.environ.get("API_PORT"),
    }

    values = [value for value in published_ports.values() if value is not None]
    assert len(set(values)) == len(values), (
        "Published host ports must be unique across services; "
        "MINIO API and ClickHouse native cannot both use 9000."
    )


def test_airflow_scheduler_runs_db_migration_before_starting() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    compose_text = compose_path.read_text()

    assert "airflow db migrate" in compose_text, (
        "Scheduler must run Airflow database migration before starting to avoid missing metadata tables."
    )
