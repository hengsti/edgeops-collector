from edgeops_collector.config import Settings
from edgeops_collector.production.device_reader import (
    DeviceReader,
)
from edgeops_collector.production.docker_reader import (
    DockerReader,
)
from edgeops_collector.production.metrics_reader import (
    MetricsReader,
)
from edgeops_collector.schemas import (
    CaptureMetadata,
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)


class ProductionBackend:
    def __init__(
        self,
        *,
        settings: Settings,
        metrics_reader: MetricsReader,
        docker_reader: DockerReader,
        device_reader: DeviceReader,
    ) -> None:
        self._settings = settings
        self._metrics_reader = metrics_reader
        self._docker_reader = docker_reader
        self._device_reader = device_reader

    def _metadata(self) -> CaptureMetadata:
        return CaptureMetadata(
            collector_mode="production",
            simulated=False,
            source_host=self._settings.source_host,
        )

    async def get_ingestion_metrics(
        self,
    ) -> CollectorEnvelope[IngestionMetrics]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=await self._metrics_reader.read(),
        )

    async def get_services(
        self,
    ) -> CollectorEnvelope[list[ServiceState]]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=await self._docker_reader.get_services(),
        )

    async def get_service(
        self,
        service: str,
    ) -> CollectorEnvelope[ServiceState]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=await self._docker_reader.get_service(service),
        )

    async def get_service_logs(
        self,
        service: str,
        *,
        tail: int,
        contains: str | None,
    ) -> CollectorEnvelope[ServiceLogs]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=await self._docker_reader.get_logs(
                service,
                tail=tail,
                contains=contains,
            ),
        )

    async def get_device(
        self,
        device_id: str,
    ) -> CollectorEnvelope[DeviceState]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=await self._device_reader.read(device_id),
        )

    async def close(self) -> None:
        await self._metrics_reader.close()
        await self._device_reader.close()
        await self._docker_reader.close()
