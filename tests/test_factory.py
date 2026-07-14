from pathlib import Path

import pytest

from edgeops_collector.backends.factory import create_backend
from edgeops_collector.backends.production import ProductionBackend
from edgeops_collector.backends.simulation import SimulationBackend
from edgeops_collector.config import CollectorMode, Settings
from edgeops_collector.main import create_app


def test_factory_starts_with_checked_in_profile(simulation_settings: Settings) -> None:
    backend = create_backend(simulation_settings)

    assert isinstance(backend, SimulationBackend)


def test_create_app_uses_real_backend_factory(simulation_settings: Settings) -> None:
    app = create_app(settings=simulation_settings)

    assert app.title == "EdgeOps Collector"


def test_production_backend_requires_explicit_mode(simulation_settings: Settings) -> None:
    settings = simulation_settings.model_copy(update={"mode": CollectorMode.PRODUCTION})

    backend = create_backend(settings)

    assert isinstance(backend, ProductionBackend)


def test_missing_profile_fails_during_startup(
    simulation_settings: Settings, tmp_path: Path
) -> None:
    settings = simulation_settings.model_copy(
        update={"simulation_profile_path": tmp_path / "missing.json"}
    )

    with pytest.raises(ValueError, match="Unable to read simulation profile"):
        create_backend(settings)
