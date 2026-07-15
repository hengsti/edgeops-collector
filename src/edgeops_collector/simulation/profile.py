import json
from math import isfinite
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from edgeops_collector.config import Settings
from edgeops_collector.schemas import INGESTION_COUNTER_NAMES, INGESTION_GAUGE_NAMES

NonNegativeFiniteFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class SimulatedService(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = "running"
    health: str | None = "healthy"

    restart_count: int = Field(
        default=0,
        ge=0,
    )


class SimulationProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    counter_initial_values: dict[str, NonNegativeFiniteFloat]
    counter_rates_per_second: dict[str, NonNegativeFiniteFloat]
    gauge_initial_values: dict[str, NonNegativeFiniteFloat]
    services: dict[str, SimulatedService]

    @model_validator(mode="after")
    def validate_metrics(self) -> "SimulationProfile":
        for field_name in ("counter_initial_values", "counter_rates_per_second"):
            values = getattr(self, field_name)
            actual = set(values)
            missing = sorted(INGESTION_COUNTER_NAMES - actual)
            extra = sorted(actual - INGESTION_COUNTER_NAMES)
            if missing or extra:
                details = []
                if missing:
                    details.append(f"missing {missing}")
                if extra:
                    details.append(f"unknown {extra}")
                raise ValueError(
                    f"{field_name} must contain exactly supported metrics: {'; '.join(details)}"
                )

            if any(not isfinite(value) or value < 0 for value in values.values()):
                raise ValueError(f"{field_name} values must be finite and nonnegative")

        actual_gauges = set(self.gauge_initial_values)
        missing_gauges = sorted(INGESTION_GAUGE_NAMES - actual_gauges)
        extra_gauges = sorted(actual_gauges - INGESTION_GAUGE_NAMES)
        if missing_gauges or extra_gauges:
            details = []
            if missing_gauges:
                details.append(f"missing {missing_gauges}")
            if extra_gauges:
                details.append(f"unknown {extra_gauges}")
            raise ValueError(
                "gauge_initial_values must contain exactly supported metrics: " + "; ".join(details)
            )

        if self.gauge_initial_values["ingest_queue_capacity"] <= 0:
            raise ValueError("ingest queue capacity must be greater than zero")
        if (
            self.gauge_initial_values["ingest_queue_depth"]
            > self.gauge_initial_values["ingest_queue_capacity"]
        ):
            raise ValueError("ingest queue depth cannot exceed capacity")
        if self.gauge_initial_values["influxdb_healthy"] > 1:
            raise ValueError("influxdb_healthy must be between zero and one")

        if (
            self.counter_initial_values["ingest_messages_processed_total"]
            > self.counter_initial_values["ingest_messages_enqueued_total"]
        ):
            raise ValueError("processed message total cannot exceed enqueued message total")

        return self


def validate_profile(profile: SimulationProfile, settings: Settings) -> None:
    configured = settings.allowed_service_names
    actual = frozenset(profile.services)
    missing = sorted(configured - actual)
    unknown = sorted(actual - configured)

    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing services {missing}")
        if unknown:
            details.append(f"unknown services {unknown}")
        raise ValueError(f"Simulation profile service mismatch: {'; '.join(details)}")


def load_profile(
    path: Path,
) -> SimulationProfile:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Unable to read simulation profile: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON simulation profile: {path}") from exc

    return SimulationProfile.model_validate(raw)
