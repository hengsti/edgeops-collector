from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Literal, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class CaptureMetadata(BaseModel):
    captured_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when the data was captured",
    )

    collector_mode: Literal["production"] = "production"
    simulated: Literal[False] = False
    source_host: str


class CollectorEnvelope[T](BaseModel):
    metadata: CaptureMetadata
    data: T


class CollectorMeta(BaseModel):
    version: str
    mode: Literal["production"] = "production"
    source_host: str
    allowed_services: list[str]


@dataclass(frozen=True)
class Metric:
    kind: Literal["counter", "gauge"] = "counter"
    raw_name: str | None = None
    optional_zero: bool = False
    exported: bool = True
    contract_required: bool = True


ZERO_IF_MISSING = Metric(optional_zero=True)


class IngestionMetrics(BaseModel):
    mqtt_messages_received_total: float | None = Field(default=None, ge=0)
    ingest_event_queue_full_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )

    ingest_decode_success_total: float | None = Field(default=None, ge=0)
    ingest_incoming_oversized_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_incoming_non_utf8_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_incoming_invalid_json_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_decode_payload_bytes_sum: float | None = Field(default=None, ge=0)
    ingest_decode_payload_bytes_count: float | None = Field(default=None, ge=0)
    ingest_decode_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_decode_duration_seconds_count: float | None = Field(default=None, ge=0)

    ingest_validate_raw_success_total: float | None = Field(default=None, ge=0)
    ingest_validate_raw_ignored_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_validate_raw_failed_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_validate_raw_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_validate_raw_duration_seconds_count: float | None = Field(default=None, ge=0)

    ingest_transform_attempt_total: float | None = Field(default=None, ge=0)
    ingest_transform_success_total: float = Field(ge=0)
    ingest_transform_failed_total: Annotated[float, ZERO_IF_MISSING] = Field(ge=0)
    ingest_transform_deserialize_failed_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_transform_ignored_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_transform_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_transform_duration_seconds_count: float | None = Field(default=None, ge=0)

    ingest_validate_business_success_total: float | None = Field(default=None, ge=0)
    ingest_validate_business_failed_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_validate_business_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_validate_business_duration_seconds_count: float | None = Field(default=None, ge=0)

    ingest_cache_updates_total: float | None = Field(default=None, ge=0)
    ingest_messages_enqueued_total: float = Field(ge=0)
    ingest_wal_queue_full_total: Annotated[
        float | None, Metric(raw_name="ingest_queue_full_total", optional_zero=True)
    ] = Field(default=None, ge=0)
    ingest_queue_closed_total: Annotated[float | None, ZERO_IF_MISSING] = Field(default=None, ge=0)
    ingest_durability_ack_failed_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_persist_duration_seconds_sum: float | None = Field(default=None, ge=0)
    ingest_persist_duration_seconds_count: float | None = Field(default=None, ge=0)

    dlq_messages_published_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    dlq_publish_errors_total: Annotated[float | None, ZERO_IF_MISSING] = Field(default=None, ge=0)
    ingest_dlq_publish_duration_seconds_sum: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    ingest_dlq_publish_duration_seconds_count: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )

    ingest_messages_processed_total: float = Field(ge=0)
    ingest_sensor_messages_processed_total: float | None = Field(default=None, ge=0)
    ingest_status_messages_processed_total: float | None = Field(default=None, ge=0)

    influx_lines_written_total: float = Field(ge=0)
    influx_write_success_total: float | None = Field(default=None, ge=0)
    influx_write_failed_total: Annotated[
        float, Metric(raw_name="influx_write_errors_total", optional_zero=True)
    ] = Field(ge=0)
    influx_write_duration_seconds_sum: float | None = Field(default=None, ge=0)
    influx_write_duration_seconds_count: float | None = Field(default=None, ge=0)

    ingest_pipeline_duration_seconds_sum: float = Field(ge=0)
    ingest_pipeline_duration_seconds_count: float = Field(ge=0)

    wal_forwarder_committed_total: float | None = Field(default=None, ge=0)
    wal_forwarder_drop_total: Annotated[float | None, ZERO_IF_MISSING] = Field(default=None, ge=0)
    wal_forwarder_retry_total: Annotated[float | None, ZERO_IF_MISSING] = Field(default=None, ge=0)
    wal_forwarder_commit_retry_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    wal_forwarder_retry_outage_seconds_sum: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    wal_forwarder_retry_outage_seconds_count: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    wal_subscription_corrupt_skipped_total: Annotated[float | None, ZERO_IF_MISSING] = Field(
        default=None, ge=0
    )
    wal_writer_fatal_total: Annotated[float | None, ZERO_IF_MISSING] = Field(default=None, ge=0)

    ingest_queue_depth: Annotated[
        float | None, Metric(kind="gauge", exported=False, contract_required=False)
    ] = Field(default=None, ge=0)
    ingest_queue_capacity: Annotated[
        float | None, Metric(kind="gauge", exported=False, contract_required=False)
    ] = Field(default=None, gt=0)

    wal_forwarder_retry_outage_active: Annotated[
        float | None, Metric(kind="gauge", optional_zero=True)
    ] = Field(default=None, ge=0, le=1)
    influxdb_healthy: Annotated[float, Metric(kind="gauge", exported=False)] = Field(ge=0, le=1)


METRICS = {
    name: next((item for item in field.metadata if isinstance(item, Metric)), Metric())
    for name, field in IngestionMetrics.model_fields.items()
}
INGESTION_METRIC_NAMES = frozenset(METRICS)
INGESTION_COUNTER_NAMES = frozenset(
    name for name, metric in METRICS.items() if metric.kind == "counter"
)
INGESTION_GAUGE_NAMES = INGESTION_METRIC_NAMES - INGESTION_COUNTER_NAMES


class ServiceState(BaseModel):
    service: str
    container_id: str | None = None

    status: str
    health: str | None = None

    restart_count: int = Field(ge=0)


class ServiceLogs(BaseModel):
    service: str
    lines: list[str]


class DeviceState(BaseModel):
    device_id: str
    available: bool

    last_seen: datetime | None = None
    heartbeat_age_seconds: float | None = Field(default=None, ge=0)
    rssi_dbm: int | None = None
    fw_version: str | None = None

    raw: dict[str, object] = Field(default_factory=dict)
