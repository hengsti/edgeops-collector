import os
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CollectorMode(StrEnum):
    PRODUCTION = "production"
    SIMULATION = "simulation"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="COLLECTOR_",
        extra="ignore",
    )

    mode: CollectorMode = CollectorMode.SIMULATION

    api_key: SecretStr

    source_host: str = "rpi-smarthome"

    ingestion_metrics_url: str = "http://ingest:9090/metrics"
    ingestion_cache_url: str = "http://ingest:8085"

    allowed_services: str = (
        "nanomq,"
        "ingestion-service,"
        "influxdb,"
        "telegraf,"
        "grafana,"
        "device-management,"
        "control-ui,"
        "homekit-api"
    )

    http_timeout_seconds: float = Field(
        default=5.0, gt=0, le=60, description="Timeout for HTTP requests in seconds"
    )

    max_log_lines: int = Field(
        default=1000, ge=1, le=5000, description="Maximum number of log lines to keep in memory"
    )

    simulation_seed: int = 42

    simulation_profile_path: Path = Path("config/simulation-profile.json")

    simulation_device_id: str = "esp32-simulated-01"

    simulation_runtime_enabled: bool = False
    simulation_admin_api_key: SecretStr | None = None
    simulation_scenario_path: Path = Path("config/scenarios")

    @model_validator(mode="after")
    def validate_simulation_admin_configuration(self) -> "Settings":
        if not self.simulation_runtime_enabled:
            return self

        if self.mode is not CollectorMode.SIMULATION:
            raise ValueError("simulation runtime control requires simulation mode")

        if self.simulation_admin_api_key is None:
            raise ValueError(
                "COLLECTOR_SIMULATION_ADMIN_API_KEY is required when runtime control is enabled"
            )

        if secrets_equal(self.api_key, self.simulation_admin_api_key):
            raise ValueError("collector and simulation admin API keys must be different")

        return self

    @property
    def allowed_service_names(self) -> frozenset[str]:
        return frozenset(
            service.strip() for service in self.allowed_services.split(",") if service.strip()
        )


def secrets_equal(left: SecretStr, right: SecretStr) -> bool:
    return left.get_secret_value() == right.get_secret_value()


def _dotenv_keys(path: Path) -> set[str]:
    if not path.is_file():
        return set()

    keys: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        keys.add(key)
    return keys


def validate_collector_environment(env_file: Path = Path(".env")) -> None:
    """Reject misspelled collector settings without ever reading their values."""
    supported = {f"COLLECTOR_{name.upper()}" for name in Settings.model_fields}
    configured = {name for name in os.environ if name.startswith("COLLECTOR_")}
    configured.update(name for name in _dotenv_keys(env_file) if name.startswith("COLLECTOR_"))
    unsupported = sorted(configured - supported)

    if unsupported:
        names = ", ".join(unsupported)
        raise ValueError(f"Unsupported collector environment variable(s): {names}")


@lru_cache
def get_settings() -> Settings:
    validate_collector_environment()
    return Settings()
