from datetime import UTC, datetime
from typing import TypeVar

from pydantic import BaseModel, Field

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


class CollectorEnvelope[T](BaseModel):
    metadata: CaptureMetadata
    data: T


class CollectorMeta(BaseModel):
    version: str
    mode: CollectorMode
    source_host: str
    allowed_services: list[str]


class IngestionMetrics(BaseModel):
    ingest_messages_enqueued_total: float = Field(ge=0)
    ingest_messages_processed_total: float = Field(ge=0)

    ingest_sensor_messages_processed_total: float = Field(ge=0)
    ingest_status_messages_processed_total: float = Field(ge=0)

    ingest_queue_full_total: float = Field(ge=0)

    ingest_transform_success_total: float = Field(ge=0)
    ingest_transform_failed_total: float = Field(ge=0)

    influx_lines_written_total: float = Field(ge=0)
    influx_write_success_total: float = Field(ge=0)

    ingest_pipeline_duration_seconds_sum: float = Field(ge=0)
    ingest_pipeline_duration_seconds_count: float = Field(ge=0)


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
    rssi_dbm: int | None = None
    fw_version: str | None = None

    raw: dict[str, object] = Field(default_factory=dict)
