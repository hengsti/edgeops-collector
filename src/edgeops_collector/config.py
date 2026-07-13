from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
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

    mode: CollectorMode = CollectorMode.PRODUCTION

    api_key: SecretStr

    source_host: str = "rpi-smarthome"

    ingestion_metrics_url: str = "http://ingest:9090/metrics"
    ingestion_cache_url: str = "http://ingest:8085"

    allowed_metrics: str = (
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

    simulation_profile_path: Path = Path("/app/config/simulation_profile.json")

    simulation_device_id: str = "esp32-simulated-01"

    @property
    def allowed_service_names(self) -> frozenset[str]:
        return frozenset(
            service.strip() for service in self.allowed_metrics.split(",") if service.strip()
        )


@lru_cache
def get_settings() -> Settings:
    # TODO: Add validation for the simulation profile path if in simulation mode
    return Settings()  # type: ignore
