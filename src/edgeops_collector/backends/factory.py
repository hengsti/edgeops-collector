from edgeops_collector.backends.base import CollectorBackend
from edgeops_collector.config import CollectorMode, Settings


def create_backend(settings: Settings) -> CollectorBackend:
    match settings.mode:
        case CollectorMode.PRODUCTION:
            from edgeops_collector.backends.production import ProductionBackend
            from edgeops_collector.production.device_reader import DeviceReader
            from edgeops_collector.production.docker_reader import DockerReader
            from edgeops_collector.production.metrics_reader import MetricsReader

            return ProductionBackend(
                settings=settings,
                metrics_reader=MetricsReader(
                    url=settings.ingestion_metrics_url,
                    timeout_seconds=settings.http_timeout_seconds,
                ),
                docker_reader=DockerReader(
                    allowed_services=settings.allowed_service_names,
                    max_log_lines=settings.max_log_lines,
                ),
                device_reader=DeviceReader(
                    base_url=settings.ingestion_cache_url,
                    timeout_seconds=settings.http_timeout_seconds,
                ),
            )

        case CollectorMode.SIMULATION:
            from edgeops_collector.backends.simulation import SimulationBackend
            from edgeops_collector.simulation.profile import load_profile

            return SimulationBackend(
                settings=settings,
                profile=load_profile(settings.simulation_profile_path),
            )

        case _:
            raise ValueError(f"Unsupported collector mode: {settings.mode}")
