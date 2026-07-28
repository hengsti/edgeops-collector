from math import isfinite

import httpx
from prometheus_client.parser import text_string_to_metric_families

from edgeops_collector.errors import (
    InvalidUpstreamResponseError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import IngestionMetrics

RAW_COUNTER_METRICS = frozenset(
    {
        "ingest_messages_enqueued_total",
        "ingest_messages_processed_total",
        "ingest_sensor_messages_processed_total",
        "ingest_status_messages_processed_total",
        "ingest_event_queue_full_total",
        "ingest_queue_full_total",
        "ingest_transform_success_total",
        "ingest_transform_failed_total",
        "influx_lines_written_total",
        "influx_write_success_total",
        "influx_write_errors_total",
        "ingest_pipeline_duration_seconds_sum",
        "ingest_pipeline_duration_seconds_count",
    }
)

RAW_GAUGE_METRICS = frozenset(
    {
        "wal_forwarder_retry_outage_active",
    }
)

RAW_METRICS = RAW_COUNTER_METRICS | RAW_GAUGE_METRICS

# The Rust metrics recorder may omit counters that have never been incremented.
OPTIONAL_ZERO_METRICS = frozenset(
    {
        "ingest_event_queue_full_total",
        "ingest_queue_full_total",
        "ingest_transform_failed_total",
        "influx_write_errors_total",
        "wal_forwarder_retry_outage_active",
    }
)

REQUIRED_RAW_METRICS = RAW_METRICS - OPTIONAL_ZERO_METRICS


class MetricsReader:
    def __init__(
        self,
        *,
        url: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._url = url
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._owns_client = client is None

    async def read(self) -> IngestionMetrics:
        try:
            response = await self._client.get(self._url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError(
                f"Failed to fetch metrics from {self._url}"
            ) from exc

        parsed: dict[str, float] = {}

        try:
            families = text_string_to_metric_families(response.text)

            for family in families:
                for sample in family.samples:
                    if sample.name not in RAW_METRICS:
                        continue

                    value = float(sample.value)

                    if not isfinite(value) or value < 0.0:
                        raise ValueError(
                            f"Metric {sample.name} must be finite and nonnegative"
                        )

                    if sample.name in RAW_GAUGE_METRICS:
                        # If labels are introduced later, one active outage
                        # must make the derived health value unhealthy.
                        parsed[sample.name] = max(
                            parsed.get(sample.name, 0.0),
                            value,
                        )
                    else:
                        # Metrics such as ingest_messages_enqueued_total and
                        # ingest_queue_full_total contain a `kind` label.
                        # All labeled samples must be summed.
                        parsed[sample.name] = (
                            parsed.get(sample.name, 0.0) + value
                        )

        except (ValueError, TypeError) as exc:
            raise InvalidUpstreamResponseError(
                f"Failed to parse ingestion prometheus metrics from {self._url}"
            ) from exc

        missing = REQUIRED_RAW_METRICS.difference(parsed)

        if missing:
            missing_names = ", ".join(sorted(missing))
            raise InvalidUpstreamResponseError(
                "Missing required ingestion prometheus metrics "
                f"from {self._url}: {missing_names}"
            )

        for metric in OPTIONAL_ZERO_METRICS:
            parsed.setdefault(metric, 0.0)

        # Convert the current WAL-based ingestion metrics into the existing
        # canonical collector schema.
        return IngestionMetrics(
            ingest_messages_enqueued_total=parsed[
                "ingest_messages_enqueued_total"
            ],
            ingest_messages_processed_total=parsed[
                "ingest_messages_processed_total"
            ],
            ingest_sensor_messages_processed_total=parsed[
                "ingest_sensor_messages_processed_total"
            ],
            ingest_status_messages_processed_total=parsed[
                "ingest_status_messages_processed_total"
            ],
            ingest_queue_full_total=(
                parsed["ingest_event_queue_full_total"]
                + parsed["ingest_queue_full_total"]
            ),
            ingest_transform_success_total=parsed[
                "ingest_transform_success_total"
            ],
            ingest_transform_failed_total=parsed[
                "ingest_transform_failed_total"
            ],
            influx_lines_written_total=parsed[
                "influx_lines_written_total"
            ],
            influx_write_success_total=parsed[
                "influx_write_success_total"
            ],
            influx_write_failed_total=parsed[
                "influx_write_errors_total"
            ],
            ingest_pipeline_duration_seconds_sum=parsed[
                "ingest_pipeline_duration_seconds_sum"
            ],
            ingest_pipeline_duration_seconds_count=parsed[
                "ingest_pipeline_duration_seconds_count"
            ],
            influxdb_healthy=(
                0.0
                if parsed["wal_forwarder_retry_outage_active"] >= 1.0
                else 1.0
            ),
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()