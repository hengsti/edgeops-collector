import asyncio
import random
import time
from datetime import UTC, datetime

from edgeops_collector.config import (
    CollectorMode,
    Settings,
)
from edgeops_collector.errors import (
    ResourceNotFoundError,
)
from edgeops_collector.schemas import (
    CaptureMetadata,
    CollectorEnvelope,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)
from edgeops_collector.simulation.profile import (
    SimulationProfile,
)


class SimulationBackend:
    def __init__(
        self,
        *,
        settings: Settings,
        profile: SimulationProfile,
    ) -> None:
        self._settings = settings
        self._profile = profile

        self._lock = asyncio.Lock()

        self._random = random.Random(settings.simulation_seed)

        self._last_tick = time.monotonic()

        self._counters = dict(profile.counter_initial_values)

    def _metadata(self) -> CaptureMetadata:
        return CaptureMetadata(
            collector_mode=CollectorMode.SIMULATION,
            simulated=True,
            source_host=self._settings.source_host,
        )

    async def _tick(self) -> None:
        async with self._lock:
            now = time.monotonic()

            elapsed = max(
                0.0,
                now - self._last_tick,
            )

            self._last_tick = now

            for (
                metric,
                rate,
            ) in self._profile.counter_rates_per_second.items():
                jitter = self._random.uniform(
                    0.995,
                    1.005,
                )

                increment = max(
                    0.0,
                    rate * elapsed * jitter,
                )

                self._counters[metric] = self._counters.get(metric, 0.0) + increment

    async def get_ingestion_metrics(
        self,
    ) -> CollectorEnvelope[IngestionMetrics]:
        await self._tick()

        async with self._lock:
            metrics = IngestionMetrics.model_validate(dict(self._counters))

        return CollectorEnvelope(
            metadata=self._metadata(),
            data=metrics,
        )

    def _service_state(
        self,
        service: str,
    ) -> ServiceState:
        if service not in self._settings.allowed_service_names:
            raise ResourceNotFoundError(f"Service is not allowlisted: {service}")

        simulated = self._profile.services.get(service)

        if simulated is None:
            return ServiceState(
                service=service,
                container_id=None,
                status="running",
                health="healthy",
                restart_count=0,
            )

        return ServiceState(
            service=service,
            container_id=None,
            status=simulated.status,
            health=simulated.health,
            restart_count=simulated.restart_count,
        )

    async def get_services(
        self,
    ) -> CollectorEnvelope[list[ServiceState]]:
        states = [
            self._service_state(service) for service in sorted(self._settings.allowed_service_names)
        ]

        return CollectorEnvelope(
            metadata=self._metadata(),
            data=states,
        )

    async def get_service(
        self,
        service: str,
    ) -> CollectorEnvelope[ServiceState]:
        return CollectorEnvelope(
            metadata=self._metadata(),
            data=self._service_state(service),
        )

    async def get_service_logs(
        self,
        service: str,
        *,
        tail: int,
        contains: str | None,
    ) -> CollectorEnvelope[ServiceLogs]:
        self._service_state(service)

        timestamp = datetime.now(UTC).isoformat()

        lines = [(f"{timestamp} INFO service={service} status=healthy source=simulation")]

        if contains:
            needle = contains.casefold()

            lines = [line for line in lines if needle in line.casefold()]

        return CollectorEnvelope(
            metadata=self._metadata(),
            data=ServiceLogs(
                service=service,
                lines=lines[-tail:],
            ),
        )

    async def get_device(
        self,
        device_id: str,
    ) -> CollectorEnvelope[DeviceState]:
        if device_id != self._settings.simulation_device_id:
            raise ResourceNotFoundError(f"Device not found: {device_id}")

        return CollectorEnvelope(
            metadata=self._metadata(),
            data=DeviceState(
                device_id=device_id,
                available=True,
                last_seen=datetime.now(UTC),
                rssi_dbm=-51,
                fw_version="0.0.6-simulated",
                raw={
                    "source": "simulation",
                },
            ),
        )

    async def close(self) -> None:
        return None
