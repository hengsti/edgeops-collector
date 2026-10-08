from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from edgeops_collector.config import Settings
from edgeops_collector.errors import ResourceNotFoundError
from edgeops_collector.main import create_app
from edgeops_collector.schemas import (
    INGESTION_COUNTER_NAMES,
    CaptureMetadata,
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)


class FakeProductionBackend:
    def __init__(self) -> None:
        self.closed = False

    def metadata(self) -> CaptureMetadata:
        return CaptureMetadata(source_host="production-test")

    async def get_ingestion_metrics(self) -> CollectorEnvelope[IngestionMetrics]:
        values: dict[str, float | None] = {name: 0.0 for name in INGESTION_COUNTER_NAMES}
        values.update(
            {
                "ingest_queue_depth": None,
                "ingest_queue_capacity": None,
                "wal_forwarder_retry_outage_active": 0.0,
                "influxdb_healthy": 1.0,
            }
        )
        return CollectorEnvelope(
            metadata=self.metadata(), data=IngestionMetrics.model_validate(values)
        )

    async def get_services(self) -> CollectorEnvelope[list[ServiceState]]:
        return CollectorEnvelope(
            metadata=self.metadata(), data=[self._service("ingestion-service")]
        )

    def _service(self, service: str) -> ServiceState:
        if service != "ingestion-service":
            raise ResourceNotFoundError(f"Service is not allowlisted: {service}")
        return ServiceState(service=service, status="running", health="healthy", restart_count=0)

    async def get_service(self, service: str) -> CollectorEnvelope[ServiceState]:
        return CollectorEnvelope(metadata=self.metadata(), data=self._service(service))

    async def get_service_logs(
        self, service: str, *, tail: int, contains: str | None
    ) -> CollectorEnvelope[ServiceLogs]:
        self._service(service)
        lines = ["service healthy"]
        if contains:
            lines = [line for line in lines if contains.casefold() in line.casefold()]
        return CollectorEnvelope(
            metadata=self.metadata(), data=ServiceLogs(service=service, lines=lines[-tail:])
        )

    async def get_device(self, device_id: str) -> CollectorEnvelope[DeviceState]:
        if device_id != "esp32-production-01":
            raise ResourceNotFoundError(f"Device not found: {device_id}")
        return CollectorEnvelope(
            metadata=self.metadata(),
            data=DeviceState(device_id=device_id, available=True, heartbeat_age_seconds=1),
        )

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_key="production-test-key",
        source_host="production-test",
        allowed_services="ingestion-service",
    )


@pytest.fixture
def backend() -> FakeProductionBackend:
    return FakeProductionBackend()


@pytest.fixture
def client(settings: Settings, backend: FakeProductionBackend) -> Iterator[TestClient]:
    with TestClient(create_app(settings=settings, backend=backend)) as test_client:
        yield test_client


def test_health_is_public_and_read_routes_require_key(client: TestClient) -> None:
    assert client.get("/health").status_code == 200
    assert client.get("/v1/meta").status_code == 401
    assert client.get("/v1/meta", headers={"X-EdgeOps-Key": "wrong"}).status_code == 401


def test_meta_and_envelopes_have_fixed_production_provenance(client: TestClient) -> None:
    headers = {"X-EdgeOps-Key": "production-test-key"}
    meta = client.get("/v1/meta", headers=headers)
    metrics = client.get("/v1/metrics/ingestion", headers=headers)
    assert meta.status_code == 200
    assert meta.json()["mode"] == "production"
    metadata = metrics.json()["metadata"]
    assert metadata.keys() == {"captured_at", "collector_mode", "simulated", "source_host"}
    assert metadata["collector_mode"] == "production"
    assert metadata["simulated"] is False


@pytest.mark.parametrize(
    "path",
    [
        "/v1/simulation/scenarios",
        "/v1/simulation/runs",
        "/v1/simulation/runs/current",
    ],
)
def test_simulation_routes_are_unregistered(client: TestClient, path: str) -> None:
    headers = {"X-EdgeOps-Key": "production-test-key"}
    assert client.get(path, headers=headers).status_code == 404
    assert client.post(path, headers=headers, json={}).status_code == 404
    assert client.delete(path, headers=headers).status_code == 404


def test_read_contract_and_limits(client: TestClient) -> None:
    headers = {"X-EdgeOps-Key": "production-test-key"}
    assert client.get("/v1/services", headers=headers).status_code == 200
    assert client.get("/v1/services/ingestion-service", headers=headers).status_code == 200
    assert (
        client.get(
            "/v1/services/ingestion-service/logs?tail=1&contains=healthy", headers=headers
        ).status_code
        == 200
    )
    assert client.get("/v1/services/not-allowed", headers=headers).status_code == 404
    assert client.get("/v1/devices/esp32-production-01", headers=headers).status_code == 200
    assert client.get("/v1/devices/unknown", headers=headers).status_code == 404
    assert (
        client.get("/v1/services/ingestion-service/logs?tail=0", headers=headers).status_code == 422
    )


def test_lifespan_closes_backend(settings: Settings, backend: FakeProductionBackend) -> None:
    with TestClient(create_app(settings=settings, backend=backend)):
        assert backend.closed is False
    assert backend.closed is True
