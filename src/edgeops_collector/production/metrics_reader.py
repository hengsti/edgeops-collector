import httpx
from prometheus_client.parser import text_string_to_metric_families

from edgeops_collector.errors import InvalidUpstreamResponseError, UpstreamUnavailableError
from edgeops_collector.schemas import IngestionMetrics

REQUIRED_METRICS = frozenset(
    {
        "ingest_messages_enqueued_total",
        "ingest_messages_processed_total",
        "ingest_sensor_messages_processed_total",
        "ingest_status_messages_processed_total",
        "ingest_queue_full_total",
        "ingest_transform_success_total",
        "ingest_transform_failed_total",
        "influx_lines_written_total",
        "influx_write_success_total",
        "influx_write_failed_total",
        "ingest_pipeline_duration_seconds_sum",
        "ingest_pipeline_duration_seconds_count",
        "ingest_queue_depth",
        "ingest_queue_capacity",
        "influxdb_healthy",
    }
)


class MetricsReader:
    def __init__(self, *, url: str, timeout_seconds: float) -> None:
        self._url = url
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def read(self) -> IngestionMetrics:
        try:
            response = await self._client.get(self._url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise UpstreamUnavailableError(f"Failed to fetch metrics from {self._url}") from e

        parsed: dict[str, float] = {}

        try:
            families = text_string_to_metric_families(response.text)

            for family in families:
                for sample in family.samples:
                    if sample.name in REQUIRED_METRICS:
                        parsed[sample.name] = sample.value

        except (ValueError, TypeError) as e:
            raise InvalidUpstreamResponseError(
                f"Failed to parse ingestion prometheus metrics from {self._url}"
            ) from e

        missing = REQUIRED_METRICS.difference(parsed)

        if missing:
            missing_names = ", ".join(sorted(missing))

            raise InvalidUpstreamResponseError(
                f"Missing required ingestion prometheus metrics from {self._url}: {missing_names}"
            )

        return IngestionMetrics(**parsed)

    async def close(self) -> None:
        await self._client.aclose()
