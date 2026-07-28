import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

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
    SimulationRunRequest,
    SimulationRunState,
    SimulationScenarioSummary,
)
from edgeops_collector.simulation.profile import (
    SimulationProfile,
)
from edgeops_collector.simulation.scenario import (
    ScenarioDefinition,
    ScenarioEffects,
    ScenarioPhase,
    ScenarioRepository,
)

_SIMULATION_EPOCH = datetime(2024, 1, 1, tzinfo=UTC)


@dataclass
class ActiveRun:
    run_id: str
    scenario: ScenarioDefinition
    seed: int
    speed: float
    started_at: datetime
    started_tick: float


class SimulationBackend:
    def __init__(
        self,
        *,
        settings: Settings,
        profile: SimulationProfile,
        scenarios: ScenarioRepository | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._profile = profile
        self._scenarios = scenarios
        self._clock = clock
        self._active_run: ActiveRun | None = None

        self._lock = asyncio.Lock()

        self._random = random.Random(settings.simulation_seed)

        self._clock_origin = self._clock()
        self._last_tick = self._clock_origin

        self._counters = dict(profile.counter_initial_values)
        self._queue_depth = profile.gauge_initial_values["ingest_queue_depth"]

    def _simulated_now(self) -> datetime:
        return _SIMULATION_EPOCH + timedelta(seconds=self._clock() - self._clock_origin)

    def _run_phase(self) -> tuple[ActiveRun, ScenarioPhase] | None:
        run = self._active_run
        if run is None:
            return None

        elapsed = max(0.0, self._clock() - run.started_tick) * run.speed
        phase_start = 0.0
        for phase in run.scenario.phases:
            phase_end = phase_start + phase.duration_seconds
            if elapsed < phase_end:
                return run, phase
            phase_start = phase_end

        return None

    def _metadata(self) -> CaptureMetadata:
        active = self._run_phase()
        return CaptureMetadata(
            captured_at=self._simulated_now(),
            collector_mode=CollectorMode.SIMULATION,
            simulated=True,
            source_host=self._settings.source_host,
            scenario_id=active[0].scenario.id if active else None,
            simulation_run_id=active[0].run_id if active else None,
            simulation_seed=active[0].seed if active else None,
            simulation_phase=active[1].id if active else None,
        )

    def _influxdb_health(self) -> float:
        active = self._run_phase()
        service = self._profile.services["influxdb"]
        if active:
            service = active[1].effects.services.get("influxdb", service)
        return 1.0 if service.health == "healthy" else 0.0

    def _wal_retry_outage_active(self) -> float:
        if self._influxdb_health() == 0.0:
            return 1.0
        return self._profile.gauge_initial_values["wal_forwarder_retry_outage_active"]

    def _phase_started_at(self, run: ActiveRun, selected: ScenarioPhase) -> datetime:
        elapsed_seconds = 0.0
        for phase in run.scenario.phases:
            if phase is selected:
                break
            elapsed_seconds += phase.duration_seconds / run.speed
        phase_tick = run.started_tick + elapsed_seconds
        return _SIMULATION_EPOCH + timedelta(seconds=phase_tick - self._clock_origin)

    def _advance_counters(self, now: float) -> None:
        interval_start = self._last_tick
        elapsed = max(0.0, now - interval_start)
        self._last_tick = now

        run = self._active_run
        segments: list[tuple[float, ScenarioEffects]] = []
        if run is None:
            segments.append((elapsed, ScenarioEffects()))
        else:
            before_run = max(0.0, min(now, run.started_tick) - interval_start)
            if before_run:
                segments.append((before_run, ScenarioEffects()))

            run_end = run.started_tick
            phase_start = 0.0
            for phase in run.scenario.phases:
                phase_end = phase_start + phase.duration_seconds
                phase_started_at = run.started_tick + phase_start / run.speed
                phase_ended_at = run.started_tick + phase_end / run.speed
                overlap = max(
                    0.0,
                    min(now, phase_ended_at) - max(interval_start, phase_started_at),
                )
                if overlap:
                    segments.append((overlap, phase.effects))
                run_end = phase_ended_at
                phase_start = phase_end

            after_run = max(0.0, now - max(interval_start, run_end))
            if after_run:
                segments.append((after_run, ScenarioEffects()))

        jitters: dict[str, float] = {}
        for metric, rate in self._profile.counter_rates_per_second.items():
            jitter = self._random.uniform(0.995, 1.005)
            jitters[metric] = jitter
            increment = sum(
                max(
                    0.0,
                    (
                        rate * effects.counter_rate_multipliers.get(metric, 1.0)
                        + effects.counter_rate_additions.get(metric, 0.0)
                    )
                    * duration
                    * jitter,
                )
                for duration, effects in segments
            )
            self._counters[metric] = self._counters.get(metric, 0.0) + increment

        capacity = self._profile.gauge_initial_values["ingest_queue_capacity"]
        for duration, effects in segments:
            enqueued_rate = self._profile.counter_rates_per_second["ingest_messages_enqueued_total"]
            processed_rate = self._profile.counter_rates_per_second[
                "ingest_messages_processed_total"
            ]
            enqueued = max(
                0.0,
                (
                    enqueued_rate
                    * effects.counter_rate_multipliers.get("ingest_messages_enqueued_total", 1.0)
                    + effects.counter_rate_additions.get("ingest_messages_enqueued_total", 0.0)
                )
                * duration
                * jitters["ingest_messages_enqueued_total"],
            )
            processed = max(
                0.0,
                (
                    processed_rate
                    * effects.counter_rate_multipliers.get("ingest_messages_processed_total", 1.0)
                    + effects.counter_rate_additions.get("ingest_messages_processed_total", 0.0)
                )
                * duration
                * jitters["ingest_messages_processed_total"],
            )
            self._queue_depth = min(
                capacity,
                max(0.0, self._queue_depth + enqueued - processed),
            )

        if run is not None and now >= run_end:
            self._active_run = None

    async def _tick(self) -> None:
        async with self._lock:
            self._advance_counters(self._clock())

    async def get_ingestion_metrics(
        self,
    ) -> CollectorEnvelope[IngestionMetrics]:
        await self._tick()

        async with self._lock:
            metrics = IngestionMetrics.model_validate(
                {
                    **self._counters,
                    "ingest_queue_depth": self._queue_depth,
                    "ingest_queue_capacity": self._profile.gauge_initial_values[
                        "ingest_queue_capacity"
                    ],
                    "wal_forwarder_retry_outage_active": (self._wal_retry_outage_active()),
                    "influxdb_healthy": self._influxdb_health(),
                }
            )

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

        simulated = self._profile.services[service]
        active = self._run_phase()
        if active:
            simulated = active[1].effects.services.get(service, simulated)

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

        timestamp = self._simulated_now().isoformat()

        lines = [f"{timestamp} INFO service={service} status=healthy source=simulation"]
        active = self._run_phase()
        if active:
            lines.extend(
                f"{timestamp} {line} scenario={active[0].scenario.id} phase={active[1].id}"
                for line in active[1].effects.service_logs.get(service, [])
            )

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

        active = self._run_phase()
        device_effect = active[1].effects.device if active else None
        now = self._simulated_now()
        unavailable = device_effect is not None and not device_effect.available
        last_seen = self._phase_started_at(*active) if active and unavailable else now

        return CollectorEnvelope(
            metadata=self._metadata(),
            data=DeviceState(
                device_id=device_id,
                available=device_effect.available if device_effect else True,
                last_seen=last_seen,
                heartbeat_age_seconds=max(0.0, (now - last_seen).total_seconds()),
                rssi_dbm=device_effect.rssi_dbm if device_effect else -51,
                fw_version="0.0.6-simulated",
                raw={
                    "source": "simulation",
                    "scenario_id": active[0].scenario.id if active else None,
                },
            ),
        )

    def _require_scenarios(self) -> ScenarioRepository:
        if self._scenarios is None:
            raise ResourceNotFoundError("Runtime simulation control is disabled")
        return self._scenarios

    def _run_state(self, run: ActiveRun, phase: ScenarioPhase) -> SimulationRunState:
        return SimulationRunState(
            run_id=run.run_id,
            scenario_id=run.scenario.id,
            seed=run.seed,
            speed=run.speed,
            started_at=run.started_at,
            phase=phase.id,
        )

    async def list_scenarios(self) -> list[SimulationScenarioSummary]:
        return self._require_scenarios().list()

    async def start_run(self, request: SimulationRunRequest) -> SimulationRunState:
        repository = self._require_scenarios()
        async with self._lock:
            active = self._run_phase()
            if active is not None:
                raise ValueError("A simulation run is already active")
            if self._active_run is not None:
                self._advance_counters(self._clock())

            scenario = repository.get(request.scenario_id)
            if scenario is None:
                raise ResourceNotFoundError(f"Simulation scenario not found: {request.scenario_id}")

            seed = request.seed if request.seed is not None else self._settings.simulation_seed
            run = ActiveRun(
                run_id=str(uuid4()),
                scenario=scenario,
                seed=seed,
                speed=request.speed,
                started_at=self._simulated_now(),
                started_tick=self._clock(),
            )
            self._active_run = run
            self._random = random.Random(seed)
            return self._run_state(run, scenario.phases[0])

    async def get_current_run(self) -> SimulationRunState | None:
        async with self._lock:
            active = self._run_phase()
            return self._run_state(*active) if active else None

    async def stop_run(self) -> SimulationRunState | None:
        async with self._lock:
            active = self._run_phase()
            self._advance_counters(self._clock())
            self._active_run = None
            return self._run_state(*active) if active else None

    async def close(self) -> None:
        return None
