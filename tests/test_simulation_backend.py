import asyncio

import pytest

from edgeops_collector.backends.simulation import (
    SimulationBackend,
)
from edgeops_collector.errors import (
    ResourceNotFoundError,
)


async def test_simulation_counters_are_monotonic(
    simulation_backend: SimulationBackend,
) -> None:
    first = await simulation_backend.get_ingestion_metrics()

    await asyncio.sleep(0.02)

    second = await simulation_backend.get_ingestion_metrics()

    first_values = first.data.model_dump()
    second_values = second.data.model_dump()
    assert all(second_values[name] >= value for name, value in first_values.items())


async def test_concurrent_simulation_reads_remain_monotonic(
    simulation_backend: SimulationBackend,
) -> None:
    results = await asyncio.gather(*(simulation_backend.get_ingestion_metrics() for _ in range(20)))

    for previous, current in zip(results, results[1:], strict=False):
        previous_values = previous.data.model_dump()
        current_values = current.data.model_dump()
        assert all(current_values[name] >= value for name, value in previous_values.items())


async def test_unknown_service_is_rejected(
    simulation_backend: SimulationBackend,
) -> None:
    with pytest.raises(ResourceNotFoundError):
        await simulation_backend.get_service("not-allowed")


async def test_known_service_is_healthy(
    simulation_backend: SimulationBackend,
) -> None:
    result = await simulation_backend.get_service("ingestion-service")

    assert result.data.status == "running"
    assert result.data.health == "healthy"
    assert result.metadata.simulated is True


async def test_known_device_is_available(
    simulation_backend: SimulationBackend,
) -> None:
    result = await simulation_backend.get_device("esp32-simulated-01")

    assert result.data.available is True
    assert result.data.rssi_dbm == -51
    assert result.metadata.simulated is True


async def test_unknown_device_is_rejected(
    simulation_backend: SimulationBackend,
) -> None:
    with pytest.raises(ResourceNotFoundError):
        await simulation_backend.get_device("unknown-device")
