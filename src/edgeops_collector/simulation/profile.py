import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


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

    counter_initial_values: dict[str, float]
    counter_rates_per_second: dict[str, float]
    services: dict[str, SimulatedService]


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
