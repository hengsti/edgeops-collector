from typing import Protocol

from edgeops_collector.schemas import (
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)


class CollectorBackend(Protocol):
    async def get_ingestion_metrics(self) -> CollectorEnvelope[IngestionMetrics]: ...

    async def get_services(self) -> CollectorEnvelope[list[ServiceState]]: ...

    async def get_service(self, service: str) -> CollectorEnvelope[ServiceState]: ...

    async def get_service_logs(
        self, service: str, *, tail: int, contains: str | None
    ) -> CollectorEnvelope[ServiceLogs]: ...

    async def get_device(self, device_id: str) -> CollectorEnvelope[DeviceState]: ...

    async def close(self) -> None: ...
