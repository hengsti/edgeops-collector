from datetime import UTC, datetime
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field

from edgeops_collector.config import CollectorMode

T = TypeVar("T")


class CaptureMetadata(BaseModel):
    captured_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when the data was captured",
    )

    collector_mode: CollectorMode
    simulated: bool
    source_host: str
    scenario_id: str | None = None
    simulation_run_id: str | None = None
    simulation_seed: int | None = None
    simulation_phase: str | None = None


class CollectorEnvelope[T](BaseModel):
    metadata: CaptureMetadata
    data: T


class CollectorMeta(BaseModel):
    version: str
    mode: CollectorMode
    source_host: str
    allowed_services: list[str]


class IngestionMetrics(BaseModel):
    mqtt_messages_received_total: float = Field(ge=0)
    ingest_event_queue_full_total: float = Field(ge=0)

    ingest_decode_success_total: float = Field(ge=0)
    ingest_incoming_oversized_total: float = Field(ge=0)
    ingest_incoming_non_utf8_total: float = Field(ge=0)
    ingest_incoming_invalid_json_total: float = Field(ge=0)
    ingest_decode_payload_bytes_sum: float = Field(ge=0)
    ingest_decode_payload_bytes_count: float = Field(ge=0)
    ingest_decode_duration_seconds_sum: float = Field(ge=0)
    ingest_decode_duration_seconds_count: float = Field(ge=0)

    ingest_validate_raw_success_total: float = Field(ge=0)
    ingest_validate_raw_ignored_total: float = Field(ge=0)
    ingest_validate_raw_failed_total: float = Field(ge=0)
    ingest_validate_raw_duration_seconds_sum: float = Field(ge=0)
    ingest_validate_raw_duration_seconds_count: float = Field(ge=0)

    ingest_transform_attempt_total: float = Field(ge=0)
    ingest_transform_success_total: float = Field(ge=0)
    ingest_transform_failed_total: float = Field(ge=0)
    ingest_transform_deserialize_failed_total: float = Field(ge=0)
    ingest_transform_ignored_total: float = Field(ge=0)
    ingest_transform_duration_seconds_sum: float = Field(ge=0)
    ingest_transform_duration_seconds_count: float = Field(ge=0)

    ingest_validate_business_success_total: float = Field(ge=0)
    ingest_validate_business_failed_total: float = Field(ge=0)
    ingest_validate_business_duration_seconds_sum: float = Field(ge=0)
    ingest_validate_business_duration_seconds_count: float = Field(ge=0)

    ingest_cache_updates_total: float = Field(ge=0)
    ingest_messages_enqueued_total: float = Field(ge=0)
    ingest_wal_queue_full_total: float = Field(ge=0)
    ingest_queue_closed_total: float = Field(ge=0)
    ingest_durability_ack_failed_total: float = Field(ge=0)
    ingest_persist_duration_seconds_sum: float = Field(ge=0)
    ingest_persist_duration_seconds_count: float = Field(ge=0)

    dlq_messages_published_total: float = Field(ge=0)
    dlq_publish_errors_total: float = Field(ge=0)
    ingest_dlq_publish_duration_seconds_sum: float = Field(ge=0)
    ingest_dlq_publish_duration_seconds_count: float = Field(ge=0)

    ingest_messages_processed_total: float = Field(ge=0)
    ingest_sensor_messages_processed_total: float = Field(ge=0)
    ingest_status_messages_processed_total: float = Field(ge=0)

    influx_lines_written_total: float = Field(ge=0)
    influx_write_success_total: float = Field(ge=0)
    influx_write_failed_total: float = Field(ge=0)
    influx_write_duration_seconds_sum: float = Field(ge=0)
    influx_write_duration_seconds_count: float = Field(ge=0)

    ingest_pipeline_duration_seconds_sum: float = Field(ge=0)
    ingest_pipeline_duration_seconds_count: float = Field(ge=0)

    wal_forwarder_committed_total: float = Field(ge=0)
    wal_forwarder_drop_total: float = Field(ge=0)
    wal_forwarder_retry_total: float = Field(ge=0)
    wal_forwarder_commit_retry_total: float = Field(ge=0)
    wal_forwarder_retry_outage_seconds_sum: float = Field(ge=0)
    wal_forwarder_retry_outage_seconds_count: float = Field(ge=0)
    wal_subscription_corrupt_skipped_total: float = Field(ge=0)
    wal_writer_fatal_total: float = Field(ge=0)

    # The WAL-based ingestion service does not export queue gauges.
    # Simulation mode continues to supply both values.
    ingest_queue_depth: float | None = Field(default=None, ge=0)
    ingest_queue_capacity: float | None = Field(default=None, gt=0)

    wal_forwarder_retry_outage_active: float = Field(ge=0, le=1)
    influxdb_healthy: float = Field(ge=0, le=1)


INGESTION_METRIC_NAMES = frozenset(IngestionMetrics.model_fields)
INGESTION_COUNTER_NAMES = frozenset(
    name
    for name in INGESTION_METRIC_NAMES
    if name.endswith("_total") or name.endswith("_sum") or name.endswith("_count")
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


class SimulationScenarioSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    description: str
    phases: list[str]


class SimulationRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scenario_id: str
    seed: int | None = None
    speed: float = Field(default=1.0, gt=0, le=100)


class SimulationRunState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    scenario_id: str
    seed: int
    speed: float
    started_at: datetime
    phase: str
