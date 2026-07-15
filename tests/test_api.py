from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, TypeAdapter

from edgeops_collector.backends.factory import create_backend
from edgeops_collector.backends.simulation import SimulationBackend
from edgeops_collector.config import Settings
from edgeops_collector.main import create_app
from edgeops_collector.schemas import (
    CollectorEnvelope,
    CollectorMeta,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
    SimulationRunState,
    SimulationScenarioSummary,
)
from edgeops_collector.simulation.profile import load_profile

READ_ROUTES = (
    "/v1/meta",
    "/v1/metrics/ingestion",
    "/v1/services",
    "/v1/services/ingestion-service",
    "/v1/services/ingestion-service/logs",
    "/v1/devices/esp32-simulated-01",
)


@pytest.fixture
def client(simulation_settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings=simulation_settings)) as test_client:
        yield test_client


def test_health_does_not_require_api_key(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "0.1.0"}


@pytest.mark.parametrize("route", READ_ROUTES)
@pytest.mark.parametrize("headers", [{}, {"X-EdgeOps-Key": "incorrect-key"}])
def test_versioned_routes_require_valid_api_key(
    client: TestClient, route: str, headers: dict[str, str]
) -> None:
    response = client.get(route, headers=headers)

    assert response.status_code == 401


def test_meta_endpoint_returns_configured_values(
    client: TestClient, simulation_settings: Settings
) -> None:
    response = client.get("/v1/meta", headers={"X-EdgeOps-Key": "test-key"})

    assert response.status_code == 200
    meta = CollectorMeta.model_validate(response.json())
    assert meta.mode.value == "simulation"
    assert meta.source_host == "test-host"
    assert meta.allowed_services == sorted(simulation_settings.allowed_service_names)


def test_all_data_routes_return_typed_responses(client: TestClient) -> None:
    headers = {"X-EdgeOps-Key": "test-key"}
    expectations: tuple[tuple[str, TypeAdapter[Any]], ...] = (
        ("/v1/metrics/ingestion", TypeAdapter(CollectorEnvelope[IngestionMetrics])),
        ("/v1/services", TypeAdapter(CollectorEnvelope[list[ServiceState]])),
        ("/v1/services/ingestion-service", TypeAdapter(CollectorEnvelope[ServiceState])),
        ("/v1/services/ingestion-service/logs", TypeAdapter(CollectorEnvelope[ServiceLogs])),
        ("/v1/devices/esp32-simulated-01", TypeAdapter(CollectorEnvelope[DeviceState])),
    )

    for route, adapter in expectations:
        response = client.get(route, headers=headers)
        assert response.status_code == 200
        adapter.validate_python(response.json())


def test_log_tail_contains_and_query_limits(client: TestClient) -> None:
    headers = {"X-EdgeOps-Key": "test-key"}
    matching = client.get(
        "/v1/services/ingestion-service/logs?tail=1&contains=healthy", headers=headers
    )
    filtered = client.get(
        "/v1/services/ingestion-service/logs?tail=1&contains=absent", headers=headers
    )

    assert len(matching.json()["data"]["lines"]) == 1
    assert filtered.json()["data"]["lines"] == []
    assert (
        client.get("/v1/services/ingestion-service/logs?tail=0", headers=headers).status_code == 422
    )
    assert (
        client.get("/v1/services/ingestion-service/logs?tail=501", headers=headers).status_code
        == 422
    )


@pytest.mark.parametrize("route", ["/v1/services/not-allowed", "/v1/devices/unknown-device"])
def test_unknown_resources_return_404(client: TestClient, route: str) -> None:
    response = client.get(route, headers={"X-EdgeOps-Key": "test-key"})

    assert response.status_code == 404


class ClosingSimulationBackend(SimulationBackend):
    closed: bool = False

    async def close(self) -> None:
        self.closed = True


def test_application_lifespan_closes_backend(simulation_settings: Settings) -> None:
    prepared = create_backend(simulation_settings)
    assert isinstance(prepared, SimulationBackend)
    backend = ClosingSimulationBackend(
        settings=simulation_settings,
        profile=load_profile(simulation_settings.simulation_profile_path),
    )

    with TestClient(create_app(settings=simulation_settings, backend=backend)):
        assert backend.closed is False

    assert backend.closed is True


@pytest.fixture
def scenario_settings(simulation_settings: Settings) -> Settings:
    root = Path(__file__).parent.parent
    return simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )


def test_scenario_lifecycle_and_provenance(scenario_settings: Settings) -> None:
    headers = {"X-EdgeOps-Key": "admin-test-key"}
    read_headers = {"X-EdgeOps-Key": "test-key"}
    with TestClient(create_app(settings=scenario_settings)) as scenario_client:
        assert (
            scenario_client.get("/v1/simulation/scenarios", headers=read_headers).status_code == 401
        )

        scenarios_response = scenario_client.get("/v1/simulation/scenarios", headers=headers)
        assert scenarios_response.status_code == 200
        scenarios = TypeAdapter(list[SimulationScenarioSummary]).validate_python(
            scenarios_response.json()
        )
        assert {scenario.id for scenario in scenarios} == {
            "ingestion-backpressure",
            "influxdb-write-failure",
            "malformed-sensor-payload",
            "missing-device-heartbeat",
        }
        assert all(
            len(scenario.phases) == 3
            and scenario.phases[0] == "normal"
            and scenario.phases[-1] == "recovery"
            for scenario in scenarios
        )

        start_response = scenario_client.post(
            "/v1/simulation/runs",
            headers=headers,
            json={"scenario_id": "missing-device-heartbeat", "seed": 7, "speed": 1},
        )
        assert start_response.status_code == 201
        run = SimulationRunState.model_validate(start_response.json())

        device_response = scenario_client.get(
            "/v1/devices/esp32-simulated-01", headers=read_headers
        )
        device = TypeAdapter(CollectorEnvelope[DeviceState]).validate_python(device_response.json())
        assert device.data.available is True
        assert device.metadata.simulation_phase == "normal"
        assert device.metadata.simulation_run_id == run.run_id
        assert device.metadata.scenario_id == "missing-device-heartbeat"
        assert device.metadata.simulation_seed == 7

        assert (
            scenario_client.get("/v1/simulation/runs/current", headers=headers).status_code == 200
        )
        assert (
            scenario_client.delete("/v1/simulation/runs/current", headers=headers).status_code
            == 200
        )
        second_stop = scenario_client.delete("/v1/simulation/runs/current", headers=headers)
        assert second_stop.status_code == 200
        assert second_stop.json() is None


@pytest.mark.parametrize(
    ("method", "route"),
    [
        ("GET", "/v1/simulation/scenarios"),
        ("POST", "/v1/simulation/runs"),
        ("GET", "/v1/simulation/runs/current"),
        ("DELETE", "/v1/simulation/runs/current"),
    ],
)
def test_all_scenario_routes_require_admin_key(
    scenario_settings: Settings, method: str, route: str
) -> None:
    with TestClient(create_app(settings=scenario_settings)) as scenario_client:
        response = scenario_client.request(method, route)

    assert response.status_code == 401


def test_unknown_scenario_returns_404(scenario_settings: Settings) -> None:
    with TestClient(create_app(settings=scenario_settings)) as scenario_client:
        response = scenario_client.post(
            "/v1/simulation/runs",
            headers={"X-EdgeOps-Key": "admin-test-key"},
            json={"scenario_id": "unknown"},
        )

    assert response.status_code == 404
