from fastapi.testclient import TestClient

from edgeops_collector.backends.simulation import (
    SimulationBackend,
)
from edgeops_collector.config import Settings
from edgeops_collector.main import create_app


def test_health_does_not_require_api_key(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_versioned_routes_require_api_key(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get("/v1/meta")

    assert response.status_code == 401


def test_invalid_api_key_is_rejected(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/meta",
            headers={
                "X-EdgeOps-Key": "incorrect-key",
            },
        )

    assert response.status_code == 401


def test_meta_endpoint_returns_mode(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/meta",
            headers={
                "X-EdgeOps-Key": "test-key",
            },
        )

    assert response.status_code == 200
    assert response.json()["mode"] == "simulation"


def test_metrics_endpoint_uses_simulation_backend(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/metrics/ingestion",
            headers={
                "X-EdgeOps-Key": "test-key",
            },
        )

    assert response.status_code == 200

    payload = response.json()

    assert payload["metadata"]["collector_mode"] == "simulation"

    assert payload["metadata"]["simulated"] is True

    assert payload["data"]["ingest_messages_enqueued_total"] >= 100000


def test_unknown_service_returns_404(
    simulation_settings: Settings,
    simulation_backend: SimulationBackend,
) -> None:
    app = create_app(
        settings=simulation_settings,
        backend=simulation_backend,
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/services/not-allowed",
            headers={
                "X-EdgeOps-Key": "test-key",
            },
        )

    assert response.status_code == 404
