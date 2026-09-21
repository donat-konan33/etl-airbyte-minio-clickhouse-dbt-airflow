from pathlib import Path

import yaml


def _load_compose() -> dict:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    return yaml.safe_load(compose_path.read_text())


def test_compose_declares_persistent_named_volumes() -> None:
    compose = _load_compose()
    expected_volumes = {"minio-data", "clickhouse-data", "postgres-data"}
    declared_volumes = set(compose.get("volumes", {}).keys())

    assert expected_volumes.issubset(declared_volumes), (
        "Persistent named volumes are missing for MinIO, ClickHouse and Postgres."
    )

    for service_name in ("minio", "clickhouse-server", "postgres"):
        service = compose["services"][service_name]
        mounted = [entry.split(":", 1)[0] for entry in service.get("volumes", []) if isinstance(entry, str)]
        assert any(volume in mounted for volume in ("minio-data", "clickhouse-data", "postgres-data")), (
            f"{service_name} is not using a persistent named volume."
        )


def test_compose_uses_healthy_dependencies_and_healthchecks() -> None:
    compose = _load_compose()

    service_checks = {
        "minio": {"healthcheck": True},
        "clickhouse-server": {"healthcheck": True, "depends_on": {"minio": "service_healthy"}},
        "postgres": {"healthcheck": True},
        "scheduler": {"healthcheck": True, "depends_on": {"postgres": "service_healthy", "clickhouse-server": "service_healthy", "minio": "service_healthy"}},
        "webserver": {"healthcheck": True, "depends_on": {"postgres": "service_healthy", "scheduler": "service_healthy"}},
        "databaseapi": {"healthcheck": True, "depends_on": {"clickhouse-server": "service_healthy"}},
    }

    for service_name, expected in service_checks.items():
        service = compose["services"][service_name]
        assert "healthcheck" in service, f"{service_name} does not expose a healthcheck."

        if "depends_on" in expected:
            depends_on = service.get("depends_on", {})
            for dependency, condition in expected["depends_on"].items():
                assert dependency in depends_on, f"{service_name} is missing dependency on {dependency}."
                assert depends_on[dependency].get("condition") == condition, (
                    f"{service_name} dependency {dependency} must use condition '{condition}'."
                )


def test_compose_network_is_shared_across_core_services() -> None:
    compose = _load_compose()
    network_name = "etl-network"
    services = compose["services"]

    for service_name in ("minio", "clickhouse-server", "postgres", "scheduler", "webserver", "databaseapi"):
        assert network_name in services[service_name].get("networks", []), (
            f"{service_name} is not attached to {network_name}."
        )

    assert network_name in compose.get("networks", {}), "The shared application network is not declared."
