import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="COLLECTOR_",
        extra="ignore",
    )

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

    @property
    def allowed_service_names(self) -> frozenset[str]:
        return frozenset(
            service.strip() for service in self.allowed_services.split(",") if service.strip()
        )


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
