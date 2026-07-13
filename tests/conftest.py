from pathlib import Path

import pytest
from pydantic import SecretStr

from edgeops_collector.backends.simulation import (
    SimulationBackend,
)
from edgeops_collector.config import (
    CollectorMode,
    Settings,
)
from edgeops_collector.simulation.profile import (
    load_profile,
)


@pytest.fixture
def simulation_settings() -> Settings:
    profile_path = Path(__file__).parent.parent / "config" / "simulation-profile.json"

    return Settings(
        mode=CollectorMode.SIMULATION,
        api_key=SecretStr("test-key"),
        source_host="test-host",
        simulation_profile_path=profile_path,
        simulation_seed=42,
        simulation_device_id="esp32-simulated-01",
    )


@pytest.fixture
def simulation_backend(
    simulation_settings: Settings,
) -> SimulationBackend:
    return SimulationBackend(
        settings=simulation_settings,
        profile=load_profile(simulation_settings.simulation_profile_path),
    )
