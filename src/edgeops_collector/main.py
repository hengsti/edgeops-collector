from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import (
    Depends,
    FastAPI,
    Query,
    Request,
)
from fastapi.responses import JSONResponse

from edgeops_collector import __version__
from edgeops_collector.backends.base import CollectorBackend
from edgeops_collector.backends.production import ProductionBackend
from edgeops_collector.config import (
    Settings,
    get_settings,
)
from edgeops_collector.errors import (
    CollectorError,
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.production.device_reader import DeviceReader
from edgeops_collector.production.docker_reader import DockerReader
from edgeops_collector.production.metrics_reader import MetricsReader
from edgeops_collector.schemas import (
    CollectorEnvelope,
    CollectorMeta,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)
from edgeops_collector.security import build_api_key_dependency


def create_app(
    *,
    settings: Settings | None = None,
    backend: CollectorBackend | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()

    resolved_backend = backend or ProductionBackend(
        settings=resolved_settings,
        metrics_reader=MetricsReader(
            url=resolved_settings.ingestion_metrics_url,
            timeout_seconds=resolved_settings.http_timeout_seconds,
        ),
        docker_reader=DockerReader(
            allowed_services=resolved_settings.allowed_service_names,
            max_log_lines=resolved_settings.max_log_lines,
        ),
        device_reader=DeviceReader(
            base_url=resolved_settings.ingestion_cache_url,
            timeout_seconds=resolved_settings.http_timeout_seconds,
        ),
    )

    require_api_key = build_api_key_dependency(resolved_settings)

    @asynccontextmanager
    async def lifespan(
        _: FastAPI,
    ) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await resolved_backend.close()

    app = FastAPI(
        title="EdgeOps Collector",
        version=__version__,
        lifespan=lifespan,
    )

    @app.exception_handler(ResourceNotFoundError)
    async def resource_not_found_handler(
        _: Request,
        exc: ResourceNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={
                "detail": str(exc),
            },
        )

    @app.exception_handler(UpstreamUnavailableError)
    async def upstream_unavailable_handler(
        _: Request,
        exc: UpstreamUnavailableError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "detail": str(exc),
            },
        )

    @app.exception_handler(CollectorError)
    async def collector_error_handler(
        _: Request,
        exc: CollectorError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=502,
            content={
                "detail": str(exc),
            },
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "version": __version__,
        }

    @app.get(
        "/v1/meta",
        dependencies=[Depends(require_api_key)],
        response_model=CollectorMeta,
    )
    async def meta() -> CollectorMeta:
        return CollectorMeta(
            version=__version__,
            mode="production",
            source_host=resolved_settings.source_host,
            allowed_services=sorted(resolved_settings.allowed_service_names),
        )

    @app.get(
        "/v1/metrics/ingestion",
        dependencies=[Depends(require_api_key)],
    )
    async def ingestion_metrics() -> CollectorEnvelope[IngestionMetrics]:
        return await resolved_backend.get_ingestion_metrics()

    @app.get(
        "/v1/services",
        dependencies=[Depends(require_api_key)],
    )
    async def services() -> CollectorEnvelope[list[ServiceState]]:
        return await resolved_backend.get_services()

    @app.get(
        "/v1/services/{service}",
        dependencies=[Depends(require_api_key)],
    )
    async def service_state(
        service: str,
    ) -> CollectorEnvelope[ServiceState]:
        return await resolved_backend.get_service(service)

    @app.get(
        "/v1/services/{service}/logs",
        dependencies=[Depends(require_api_key)],
    )
    async def service_logs(
        service: str,
        tail: Annotated[
            int,
            Query(
                ge=1,
                le=500,
            ),
        ] = 200,
        contains: Annotated[
            str | None,
            Query(max_length=100),
        ] = None,
    ) -> CollectorEnvelope[ServiceLogs]:
        return await resolved_backend.get_service_logs(
            service,
            tail=tail,
            contains=contains,
        )

    @app.get(
        "/v1/devices/{device_id}",
        dependencies=[Depends(require_api_key)],
    )
    async def device_state(
        device_id: str,
    ) -> CollectorEnvelope[DeviceState]:
        return await resolved_backend.get_device(device_id)

    return app


def run() -> None:
    uvicorn.run(
        "edgeops_collector.main:create_app",
        host="0.0.0.0",
        port=8095,
        reload=False,
        factory=True,
    )
