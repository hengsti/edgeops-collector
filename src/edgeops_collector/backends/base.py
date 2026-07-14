from typing import Protocol, runtime_checkable

from edgeops_collector.schemas import (
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
    SimulationRunRequest,
    SimulationRunState,
    SimulationScenarioSummary,
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


@runtime_checkable
class SimulationControlBackend(Protocol):
    async def list_scenarios(self) -> list[SimulationScenarioSummary]: ...

    async def start_run(self, request: SimulationRunRequest) -> SimulationRunState: ...

    async def get_current_run(self) -> SimulationRunState | None: ...

    async def stop_run(self) -> SimulationRunState | None: ...
