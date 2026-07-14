from pathlib import Path

import pytest
from pydantic import ValidationError

from edgeops_collector.config import (
    CollectorMode,
    Settings,
    validate_collector_environment,
)


def test_settings_load_canonical_environment_names(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COLLECTOR_API_KEY", "environment-secret")
    monkeypatch.setenv("COLLECTOR_SOURCE_HOST", "environment-host")
    monkeypatch.setenv("COLLECTOR_ALLOWED_SERVICES", "ingestion-service,influxdb")
    monkeypatch.setenv("COLLECTOR_HTTP_TIMEOUT_SECONDS", "7.5")
    monkeypatch.setenv("COLLECTOR_SIMULATION_PROFILE_PATH", "custom-profile.json")

    settings = Settings()

    assert settings.mode is CollectorMode.SIMULATION
    assert settings.source_host == "environment-host"
    assert settings.allowed_service_names == frozenset({"ingestion-service", "influxdb"})
    assert settings.http_timeout_seconds == 7.5
    assert settings.simulation_profile_path == Path("custom-profile.json")


def test_settings_load_from_dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "COLLECTOR_API_KEY=dotenv-secret\nCOLLECTOR_SOURCE_HOST=dotenv-host\n",
        encoding="utf-8",
    )

    settings = Settings()

    assert settings.source_host == "dotenv-host"


def test_unknown_collector_environment_name_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COLLECTOR_HPPT_TIMEOUT_SECOND", "secret-value")

    with pytest.raises(ValueError, match="COLLECTOR_HPPT_TIMEOUT_SECOND") as exc_info:
        validate_collector_environment()

    assert "secret-value" not in str(exc_info.value)


def test_admin_key_validation_redacts_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    secret = "same-very-sensitive-key"

    with pytest.raises(ValidationError) as exc_info:
        Settings(
            api_key=secret,
            simulation_runtime_enabled=True,
            simulation_admin_api_key=secret,
        )

    assert secret not in str(exc_info.value)
