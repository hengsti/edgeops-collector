from datetime import UTC, datetime

import httpx

from edgeops_collector.backends.production import ProductionBackend
from edgeops_collector.production.device_reader import DeviceReader
from edgeops_collector.production.docker_reader import DockerReader
from edgeops_collector.production.metrics_reader import RAW_METRICS, MetricsReader


async def test_backend_keeps_fresh_metadata_captured_before_each_read() -> None:
    read_times: list[datetime] = []

    def respond(request: httpx.Request) -> httpx.Response:
        read_times.append(datetime.now(UTC))
        if request.url.path == "/metrics":
            return httpx.Response(200, text="\n".join(f"{name} 0" for name in RAW_METRICS))
        return httpx.Response(200, json={"sensor": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        backend = ProductionBackend(
            source_host="production-test",
            metrics_reader=MetricsReader(url="http://ingestion.test/metrics", client=client),
            device_reader=DeviceReader(base_url="http://ingestion.test", client=client),
            docker_reader=DockerReader(allowed_services=frozenset(), max_log_lines=100),
        )

        metrics = await backend.get_ingestion_metrics()
        device = await backend.get_device("esp32-1")

    assert metrics.data.influxdb_healthy == 1.0
    assert device.data.device_id == "esp32-1"
    assert device.data.available is False
    assert metrics.metadata is not device.metadata
    assert metrics.metadata.captured_at <= read_times[0]
    assert read_times[0] <= device.metadata.captured_at <= read_times[1]
    for metadata in (metrics.metadata, device.metadata):
        assert metadata.source_host == "production-test"
        assert metadata.collector_mode == "production"
        assert metadata.simulated is False
