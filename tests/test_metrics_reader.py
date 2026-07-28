import httpx
import pytest

from edgeops_collector.errors import InvalidUpstreamResponseError
from edgeops_collector.production.metrics_reader import (
    OPTIONAL_ZERO_METRICS,
    RAW_METRICS,
    RAW_TO_CANONICAL,
    MetricsReader,
)
from edgeops_collector.schemas import IngestionMetrics


def metrics_text(
    *,
    omit: frozenset[str] = frozenset(),
    values: dict[str, float] | None = None,
) -> str:
    selected = values or {}
    return "\n".join(
        f"{metric} {selected.get(metric, 1.0)}" for metric in sorted(RAW_METRICS - omit)
    )


def mock_client(body: str) -> httpx.AsyncClient:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=body, request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(respond))


def test_reader_mapping_covers_the_production_schema() -> None:
    canonical_metrics = {RAW_TO_CANONICAL.get(metric, metric) for metric in RAW_METRICS}
    production_fields = set(IngestionMetrics.model_fields) - {
        "ingest_queue_depth",
        "ingest_queue_capacity",
        "influxdb_healthy",
    }

    assert canonical_metrics == production_fields


async def test_reader_maps_full_ingestion_contract_and_aggregates_labels() -> None:
    custom = frozenset(
        {
            "ingest_queue_full_total",
            "ingest_transform_duration_seconds_sum",
            "wal_forwarder_retry_outage_active",
        }
    )
    body = "\n".join(
        (
            metrics_text(
                omit=custom,
                values={
                    "ingest_event_queue_full_total": 7.0,
                    "influx_write_errors_total": 4.0,
                },
            ),
            'ingest_queue_full_total{kind="sensor"} 2',
            'ingest_queue_full_total{kind="status"} 3',
            'ingest_transform_duration_seconds_sum{result="success"} 1.5',
            'ingest_transform_duration_seconds_sum{result="failed"} 0.5',
            'wal_forwarder_retry_outage_active{source="primary"} 0',
            'wal_forwarder_retry_outage_active{source="secondary"} 1',
        )
    )

    async with mock_client(body) as client:
        result = await MetricsReader(
            url="http://ingestion.test/metrics",
            timeout_seconds=1,
            client=client,
        ).read()

    assert result.ingest_event_queue_full_total == 7
    assert result.ingest_wal_queue_full_total == 5
    assert result.ingest_transform_duration_seconds_sum == 2
    assert result.influx_write_failed_total == 4
    assert result.wal_forwarder_retry_outage_active == 1
    assert result.influxdb_healthy == 0
    assert "ingest_queue_full_total" not in type(result).model_fields


async def test_reader_defaults_never_emitted_failure_metrics_to_zero() -> None:
    async with mock_client(metrics_text(omit=OPTIONAL_ZERO_METRICS)) as client:
        result = await MetricsReader(
            url="http://ingestion.test/metrics",
            timeout_seconds=1,
            client=client,
        ).read()

    assert result.ingest_event_queue_full_total == 0
    assert result.ingest_wal_queue_full_total == 0
    assert result.dlq_publish_errors_total == 0
    assert result.wal_writer_fatal_total == 0
    assert result.wal_forwarder_retry_outage_active == 0
    assert result.influxdb_healthy == 1


async def test_reader_rejects_missing_required_denominator() -> None:
    body = metrics_text(omit=frozenset({"mqtt_messages_received_total"}))

    async with mock_client(body) as client:
        reader = MetricsReader(
            url="http://ingestion.test/metrics",
            timeout_seconds=1,
            client=client,
        )
        with pytest.raises(
            InvalidUpstreamResponseError,
            match="mqtt_messages_received_total",
        ):
            await reader.read()


async def test_reader_rejects_negative_metric() -> None:
    body = metrics_text(values={"mqtt_messages_received_total": -1.0})

    async with mock_client(body) as client:
        reader = MetricsReader(
            url="http://ingestion.test/metrics",
            timeout_seconds=1,
            client=client,
        )
        with pytest.raises(
            InvalidUpstreamResponseError,
            match="Failed to parse",
        ):
            await reader.read()
