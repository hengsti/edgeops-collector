from pathlib import Path

import pytest

from edgeops_collector.config import Settings, validate_collector_environment


def test_settings_load_canonical_environment_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COLLECTOR_API_KEY", "environment-secret")
    monkeypatch.setenv("COLLECTOR_SOURCE_HOST", "environment-host")
    monkeypatch.setenv("COLLECTOR_ALLOWED_SERVICES", "ingestion-service,influxdb")
    monkeypatch.setenv("COLLECTOR_HTTP_TIMEOUT_SECONDS", "7.5")

    settings = Settings(_env_file=None)

    assert settings.source_host == "environment-host"
    assert settings.allowed_service_names == frozenset({"ingestion-service", "influxdb"})
    assert settings.http_timeout_seconds == 7.5


def test_settings_load_from_dotenv() -> None:
    settings = Settings(_env_file=Path("tests/fixtures/production.env"))

    assert settings.source_host == "dotenv-host"


def test_unknown_collector_environment_name_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COLLECTOR_HPPT_TIMEOUT_SECOND", "secret-value")

    with pytest.raises(ValueError, match="COLLECTOR_HPPT_TIMEOUT_SECOND") as exc_info:
        validate_collector_environment(Path("tests/fixtures/missing.env"))

    assert "secret-value" not in str(exc_info.value)


@pytest.mark.parametrize(
    "name",
    [
        "COLLECTOR_MODE",
        "COLLECTOR_SIMULATION_SEED",
        "COLLECTOR_SIMULATION_ADMIN_API_KEY",
    ],
)
def test_old_simulation_environment_is_rejected(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(name, "sensitive-value")
    with pytest.raises(ValueError, match=name) as exc_info:
        validate_collector_environment(Path("tests/fixtures/missing.env"))
    assert "sensitive-value" not in str(exc_info.value)
