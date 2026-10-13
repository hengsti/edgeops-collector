from collections.abc import Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

from edgeops_collector.config import Settings
from edgeops_collector.errors import (
    CollectorError,
    InvalidUpstreamResponseError,
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.main import create_app
from edgeops_collector.production.docker_reader import DockerReader
from edgeops_collector.production.metrics_reader import RAW_METRICS
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


@pytest.mark.parametrize(
    "path",
    [
        "/v1/meta",
        "/v1/metrics/ingestion",
        "/v1/services",
        "/v1/services/ingestion-service",
        "/v1/services/ingestion-service/logs",
        "/v1/devices/esp32-production-01",
    ],
)
def test_every_read_route_rejects_missing_wrong_and_query_only_keys(
    client: TestClient, path: str
) -> None:
    for response in (
        client.get(path),
        client.get(path, headers={"X-EdgeOps-Key": "wrong"}),
        client.get(path, params={"X-EdgeOps-Key": "production-test-key"}),
    ):
        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid collector API key"}


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (ResourceNotFoundError("not found"), 404),
        (UpstreamUnavailableError("unavailable"), 503),
        (InvalidUpstreamResponseError("invalid payload"), 502),
        (CollectorError("collector failed"), 502),
    ],
)
def test_collector_errors_keep_the_http_contract(
    client: TestClient,
    backend: FakeProductionBackend,
    monkeypatch: pytest.MonkeyPatch,
    error: CollectorError,
    expected_status: int,
) -> None:
    async def fail() -> CollectorEnvelope[IngestionMetrics]:
        raise error

    monkeypatch.setattr(backend, "get_ingestion_metrics", fail)
    response = client.get("/v1/metrics/ingestion", headers={"X-EdgeOps-Key": "production-test-key"})

    assert response.status_code == expected_status
    assert response.json() == {"detail": str(error)}


@pytest.fixture
def http_clients(monkeypatch: pytest.MonkeyPatch) -> list[httpx.AsyncClient]:
    clients: list[httpx.AsyncClient] = []
    client_type = httpx.AsyncClient

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/metrics":
            return httpx.Response(200, text="\n".join(f"{name} 0" for name in sorted(RAW_METRICS)))
        return httpx.Response(200, json={"sensor": None})

    def create_client(*, timeout: float) -> httpx.AsyncClient:
        client = client_type(timeout=timeout, transport=httpx.MockTransport(respond))
        clients.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", create_client)
    return clients


def test_production_uses_one_http_client_owned_by_the_lifespan(
    settings: Settings, http_clients: list[httpx.AsyncClient]
) -> None:
    app = create_app(settings=settings)
    assert http_clients == []

    with TestClient(app) as client:
        assert len(http_clients) == 1
        assert http_clients[0].is_closed is False
        headers = {"X-EdgeOps-Key": "production-test-key"}
        metrics = client.get("/v1/metrics/ingestion", headers=headers)
        device = client.get("/v1/devices/esp32-production-01", headers=headers)
        assert metrics.status_code == device.status_code == 200
        assert metrics.json()["metadata"]["source_host"] == "production-test"
        assert device.json()["data"]["available"] is False
        assert len(http_clients) == 1

    assert http_clients[0].is_closed is True


def test_injected_backend_creates_no_http_clients(
    settings: Settings,
    backend: FakeProductionBackend,
    http_clients: list[httpx.AsyncClient],
) -> None:
    with TestClient(create_app(settings=settings, backend=backend)):
        assert http_clients == []
    assert backend.closed is True


def test_http_client_closes_even_if_docker_cleanup_fails(
    settings: Settings,
    http_clients: list[httpx.AsyncClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_close(self: DockerReader) -> None:
        raise RuntimeError("Docker cleanup failed")

    monkeypatch.setattr(DockerReader, "close", fail_close)
    with (
        pytest.raises(RuntimeError, match="Docker cleanup failed"),
        TestClient(create_app(settings=settings)),
    ):
        pass
    assert http_clients
    assert all(client.is_closed for client in http_clients)
