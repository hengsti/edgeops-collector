from pathlib import Path

from pydantic import SecretStr

from edgeops_collector.backends.simulation import SimulationBackend
from edgeops_collector.config import Settings
from edgeops_collector.schemas import SimulationRunRequest
from edgeops_collector.simulation.profile import load_profile
from edgeops_collector.simulation.scenario import ScenarioRepository


class FakeClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def build_scenario_backend(settings: Settings, clock: FakeClock) -> SimulationBackend:
    repository = ScenarioRepository.load(
        settings.simulation_scenario_path,
        allowed_services=settings.allowed_service_names,
    )
    return SimulationBackend(
        settings=settings,
        profile=load_profile(settings.simulation_profile_path),
        scenarios=repository,
        clock=clock,
    )


async def test_same_scenario_seed_and_time_are_deterministic(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    first_clock = FakeClock()
    second_clock = FakeClock()
    first = build_scenario_backend(settings, first_clock)
    second = build_scenario_backend(settings, second_clock)
    request = SimulationRunRequest(
        scenario_id="ingestion-backpressure",
        seed=1234,
        speed=2,
    )

    await first.start_run(request)
    await second.start_run(request)
    first_clock.advance(62.5)
    second_clock.advance(62.5)

    first_metrics = await first.get_ingestion_metrics()
    second_metrics = await second.get_ingestion_metrics()

    assert first_metrics.data == second_metrics.data
    assert first_metrics.metadata.scenario_id == second_metrics.metadata.scenario_id
    assert first_metrics.metadata.simulation_phase == "overloaded"


async def test_ingestion_backpressure_accounts_for_elapsed_time_across_phase_boundary(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    clock = FakeClock()
    backend = build_scenario_backend(settings, clock)

    initial_metrics = await backend.get_ingestion_metrics()
    await backend.start_run(
        SimulationRunRequest(
            scenario_id="ingestion-backpressure",
            seed=1234,
            speed=1,
        )
    )
    clock.advance(310)
    final_metrics = await backend.get_ingestion_metrics()

    delta = (
        final_metrics.data.ingest_messages_enqueued_total
        - initial_metrics.data.ingest_messages_enqueued_total
    )
    assert 3_300 < delta < 3_400


async def test_ingestion_backpressure_starts_at_run_boundary(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    clock = FakeClock()
    backend = build_scenario_backend(settings, clock)

    initial_metrics = await backend.get_ingestion_metrics()
    clock.advance(10)
    await backend.start_run(
        SimulationRunRequest(
            scenario_id="ingestion-backpressure",
            seed=1234,
            speed=1,
        )
    )
    clock.advance(10)
    final_metrics = await backend.get_ingestion_metrics()

    delta = (
        final_metrics.data.ingest_messages_enqueued_total
        - initial_metrics.data.ingest_messages_enqueued_total
    )
    assert 95 < delta < 105


async def test_stopping_run_accounts_for_elapsed_active_phase(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    clock = FakeClock()
    backend = build_scenario_backend(settings, clock)

    initial_metrics = await backend.get_ingestion_metrics()
    await backend.start_run(
        SimulationRunRequest(
            scenario_id="ingestion-backpressure",
            seed=1234,
            speed=1,
        )
    )
    clock.advance(100)
    stopped_run = await backend.stop_run()
    clock.advance(10)
    final_metrics = await backend.get_ingestion_metrics()

    assert stopped_run is not None
    delta = (
        final_metrics.data.ingest_messages_enqueued_total
        - initial_metrics.data.ingest_messages_enqueued_total
    )
    assert 540 < delta < 560


async def test_backpressure_exposes_queue_growth_and_recovery(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    clock = FakeClock()
    backend = build_scenario_backend(settings, clock)
    await backend.start_run(
        SimulationRunRequest(scenario_id="ingestion-backpressure", seed=42, speed=1)
    )

    clock.advance(130)
    overloaded = await backend.get_ingestion_metrics()
    clock.advance(180)
    recovery = await backend.get_ingestion_metrics()

    assert overloaded.metadata.simulation_phase == "overloaded"
    assert overloaded.data.ingest_queue_depth > 20
    assert recovery.metadata.simulation_phase == "recovery"
    assert recovery.data.ingest_queue_depth < overloaded.data.ingest_queue_capacity


async def test_write_failure_metrics_and_heartbeat_age(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )

    write_clock = FakeClock()
    write_backend = build_scenario_backend(settings, write_clock)
    before = await write_backend.get_ingestion_metrics()
    await write_backend.start_run(
        SimulationRunRequest(scenario_id="influxdb-write-failure", seed=42, speed=1)
    )
    write_clock.advance(150)
    during = await write_backend.get_ingestion_metrics()
    assert during.metadata.simulation_phase == "write-failure"
    assert during.data.influxdb_healthy == 0
    assert during.data.influx_write_failed_total > before.data.influx_write_failed_total

    heartbeat_clock = FakeClock()
    heartbeat_backend = build_scenario_backend(settings, heartbeat_clock)
    await heartbeat_backend.start_run(
        SimulationRunRequest(scenario_id="missing-device-heartbeat", seed=42, speed=1)
    )
    heartbeat_clock.advance(150)
    device = await heartbeat_backend.get_device(settings.simulation_device_id)
    assert device.metadata.simulation_phase == "heartbeat-missing"
    assert device.data.available is False
    assert device.data.heartbeat_age_seconds == 30


async def test_equal_fake_clock_progress_produces_equal_simulated_timestamps(
    simulation_settings: Settings,
) -> None:
    root = Path(__file__).parent.parent
    settings = simulation_settings.model_copy(
        update={
            "simulation_runtime_enabled": True,
            "simulation_admin_api_key": SecretStr("admin-test-key"),
            "simulation_scenario_path": root / "config" / "scenarios",
        }
    )
    first_clock = FakeClock()
    second_clock = FakeClock()
    first = build_scenario_backend(settings, first_clock)
    second = build_scenario_backend(settings, second_clock)
    request = SimulationRunRequest(
        scenario_id="ingestion-backpressure",
        seed=1234,
        speed=1,
    )

    first_run = await first.start_run(request)
    second_run = await second.start_run(request)

    assert first_run.run_id != second_run.run_id

    first_clock.advance(10)
    second_clock.advance(10)
    first_metrics = await first.get_ingestion_metrics()
    second_metrics = await second.get_ingestion_metrics()
    first_logs = await first.get_service_logs(
        "ingestion-service",
        tail=10,
        contains=None,
    )
    second_logs = await second.get_service_logs(
        "ingestion-service",
        tail=10,
        contains=None,
    )
    first_device = await first.get_device(settings.simulation_device_id)
    second_device = await second.get_device(settings.simulation_device_id)

    assert first_run.started_at == second_run.started_at
    assert first_metrics.metadata.captured_at == second_metrics.metadata.captured_at
    assert first_logs.metadata.captured_at == second_logs.metadata.captured_at
    assert first_device.metadata.captured_at == second_device.metadata.captured_at
    assert first_logs.data.lines == second_logs.data.lines
    assert first_device.data.last_seen == second_device.data.last_seen
