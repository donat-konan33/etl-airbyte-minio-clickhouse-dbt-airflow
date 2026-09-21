from pathlib import Path


def test_airflow_image_installs_curl_for_healthchecks() -> None:
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    content = dockerfile.read_text()

    assert "curl" in content, "Airflow image must install curl so the webserver healthcheck can run."
