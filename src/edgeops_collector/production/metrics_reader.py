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
        "mqtt_messages_received_total",
        "ingest_event_queue_full_total",
        "ingest_decode_success_total",
        "ingest_incoming_oversized_total",
        "ingest_incoming_non_utf8_total",
        "ingest_incoming_invalid_json_total",
        "ingest_decode_payload_bytes_sum",
        "ingest_decode_payload_bytes_count",
        "ingest_decode_duration_seconds_sum",
        "ingest_decode_duration_seconds_count",
        "ingest_validate_raw_success_total",
        "ingest_validate_raw_ignored_total",
        "ingest_validate_raw_failed_total",
        "ingest_validate_raw_duration_seconds_sum",
        "ingest_validate_raw_duration_seconds_count",
        "ingest_transform_attempt_total",
        "ingest_transform_success_total",
        "ingest_transform_failed_total",
        "ingest_transform_deserialize_failed_total",
        "ingest_transform_ignored_total",
        "ingest_transform_duration_seconds_sum",
        "ingest_transform_duration_seconds_count",
        "ingest_validate_business_success_total",
        "ingest_validate_business_failed_total",
        "ingest_validate_business_duration_seconds_sum",
        "ingest_validate_business_duration_seconds_count",
        "ingest_cache_updates_total",
        "ingest_messages_enqueued_total",
        "ingest_queue_full_total",
        "ingest_queue_closed_total",
        "ingest_durability_ack_failed_total",
        "ingest_persist_duration_seconds_sum",
        "ingest_persist_duration_seconds_count",
        "dlq_messages_published_total",
        "dlq_publish_errors_total",
        "ingest_dlq_publish_duration_seconds_sum",
        "ingest_dlq_publish_duration_seconds_count",
        "ingest_messages_processed_total",
        "ingest_sensor_messages_processed_total",
        "ingest_status_messages_processed_total",
        "ingest_pipeline_duration_seconds_sum",
        "ingest_pipeline_duration_seconds_count",
        "influx_lines_written_total",
        "influx_write_success_total",
        "influx_write_errors_total",
        "influx_write_duration_seconds_sum",
        "influx_write_duration_seconds_count",
        "wal_forwarder_committed_total",
        "wal_forwarder_drop_total",
        "wal_forwarder_retry_total",
        "wal_forwarder_commit_retry_total",
        "wal_forwarder_retry_outage_seconds_sum",
        "wal_forwarder_retry_outage_seconds_count",
        "wal_subscription_corrupt_skipped_total",
        "wal_writer_fatal_total",
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
        "ingest_incoming_oversized_total",
        "ingest_incoming_non_utf8_total",
        "ingest_incoming_invalid_json_total",
        "ingest_validate_raw_ignored_total",
        "ingest_validate_raw_failed_total",
        "ingest_transform_failed_total",
        "ingest_transform_deserialize_failed_total",
        "ingest_transform_ignored_total",
        "ingest_validate_business_failed_total",
        "ingest_queue_full_total",
        "ingest_queue_closed_total",
        "ingest_durability_ack_failed_total",
        "dlq_messages_published_total",
        "dlq_publish_errors_total",
        "ingest_dlq_publish_duration_seconds_sum",
        "ingest_dlq_publish_duration_seconds_count",
        "influx_write_errors_total",
        "wal_forwarder_drop_total",
        "wal_forwarder_retry_total",
        "wal_forwarder_commit_retry_total",
        "wal_forwarder_retry_outage_seconds_sum",
        "wal_forwarder_retry_outage_seconds_count",
        "wal_forwarder_retry_outage_active",
        "wal_subscription_corrupt_skipped_total",
        "wal_writer_fatal_total",
    }
)

REQUIRED_RAW_METRICS = RAW_METRICS - OPTIONAL_ZERO_METRICS

RAW_TO_CANONICAL = {
    "ingest_queue_full_total": "ingest_wal_queue_full_total",
    "influx_write_errors_total": "influx_write_failed_total",
}


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
            raise UpstreamUnavailableError(f"Failed to fetch metrics from {self._url}") from exc

        parsed: dict[str, float] = {}

        try:
            families = text_string_to_metric_families(response.text)

            for family in families:
                for sample in family.samples:
                    if sample.name not in RAW_METRICS:
                        continue

                    value = float(sample.value)

                    if not isfinite(value) or value < 0.0:
                        raise ValueError(f"Metric {sample.name} must be finite and nonnegative")

                    if sample.name in RAW_GAUGE_METRICS:
                        # If labels are introduced later, one active outage
                        # must make the derived health value unhealthy.
                        parsed[sample.name] = max(
                            parsed.get(sample.name, 0.0),
                            value,
                        )
                    else:
                        # Counter and histogram samples may contain `kind`,
                        # `result`, or `reason` labels. The first collector
                        # contract deliberately aggregates those variants.
                        parsed[sample.name] = parsed.get(sample.name, 0.0) + value

        except (ValueError, TypeError) as exc:
            raise InvalidUpstreamResponseError(
                f"Failed to parse ingestion prometheus metrics from {self._url}"
            ) from exc

        missing = REQUIRED_RAW_METRICS.difference(parsed)

        if missing:
            missing_names = ", ".join(sorted(missing))
            raise InvalidUpstreamResponseError(
                f"Missing required ingestion prometheus metrics from {self._url}: {missing_names}"
            )

        for metric in OPTIONAL_ZERO_METRICS:
            parsed.setdefault(metric, 0.0)

        canonical = {RAW_TO_CANONICAL.get(name, name): value for name, value in parsed.items()}
        canonical["influxdb_healthy"] = (
            0.0 if parsed["wal_forwarder_retry_outage_active"] >= 1.0 else 1.0
        )

        return IngestionMetrics.model_validate(canonical)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
