from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from edgeops_collector.schemas import INGESTION_METRIC_NAMES, SimulationScenarioSummary
from edgeops_collector.simulation.profile import SimulatedService

NonNegativeFiniteFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class SimulatedDeviceEffect(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available: bool
    rssi_dbm: int | None = None


class ScenarioEffects(BaseModel):
    model_config = ConfigDict(extra="forbid")

    counter_rate_multipliers: dict[str, NonNegativeFiniteFloat] = Field(default_factory=dict)
    counter_rate_additions: dict[str, NonNegativeFiniteFloat] = Field(default_factory=dict)
    services: dict[str, SimulatedService] = Field(default_factory=dict)
    service_logs: dict[str, list[str]] = Field(default_factory=dict)
    device: SimulatedDeviceEffect | None = None

    @model_validator(mode="after")
    def validate_metric_names(self) -> "ScenarioEffects":
        supplied = set(self.counter_rate_multipliers) | set(self.counter_rate_additions)
        unknown = sorted(supplied - INGESTION_METRIC_NAMES)
        if unknown:
            raise ValueError(f"unknown scenario metrics: {unknown}")
        return self


class ScenarioPhase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    duration_seconds: float = Field(gt=0, allow_inf_nan=False)
    effects: ScenarioEffects = Field(default_factory=ScenarioEffects)


class ScenarioDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1, ge=1, le=1)
    id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    description: str = Field(min_length=1)
    phases: list[ScenarioPhase] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_phases(self) -> "ScenarioDefinition":
        phase_ids = [phase.id for phase in self.phases]
        if len(phase_ids) != len(set(phase_ids)):
            raise ValueError("scenario phase IDs must be unique")
        return self

    def summary(self) -> SimulationScenarioSummary:
        return SimulationScenarioSummary(
            id=self.id,
            description=self.description,
            phases=[phase.id for phase in self.phases],
        )


class ScenarioRepository:
    def __init__(self, scenarios: dict[str, ScenarioDefinition]) -> None:
        self._scenarios = scenarios

    @classmethod
    def load(
        cls,
        directory: Path,
        *,
        allowed_services: frozenset[str],
    ) -> "ScenarioRepository":
        if not directory.is_dir():
            raise ValueError(f"Unable to read simulation scenario directory: {directory}")

        scenarios: dict[str, ScenarioDefinition] = {}
        paths = sorted((*directory.glob("*.yaml"), *directory.glob("*.yml")))
        if not paths:
            raise ValueError(f"No simulation scenarios found in: {directory}")

        for path in paths:
            try:
                raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                raise ValueError(f"Unable to load simulation scenario: {path}") from exc

            scenario = ScenarioDefinition.model_validate(raw)
            if scenario.id in scenarios:
                raise ValueError(f"Duplicate simulation scenario ID: {scenario.id}")

            for phase in scenario.phases:
                referenced = set(phase.effects.services) | set(phase.effects.service_logs)
                unknown = sorted(referenced - allowed_services)
                if unknown:
                    raise ValueError(
                        f"Scenario {scenario.id} phase {phase.id} references "
                        f"unknown services: {unknown}"
                    )

            scenarios[scenario.id] = scenario

        return cls(scenarios)

    def list(self) -> list[SimulationScenarioSummary]:
        return [self._scenarios[key].summary() for key in sorted(self._scenarios)]

    def get(self, scenario_id: str) -> ScenarioDefinition | None:
        return self._scenarios.get(scenario_id)
