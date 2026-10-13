from typing import Annotated, cast

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from edgeops_collector import __version__
from edgeops_collector.backends.base import CollectorBackend
from edgeops_collector.config import Settings
from edgeops_collector.errors import ResourceNotFoundError, UpstreamUnavailableError
from edgeops_collector.schemas import (
    CollectorEnvelope,
    CollectorMeta,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
)
from edgeops_collector.security import build_api_key_dependency


def get_backend(request: Request) -> CollectorBackend:
    return cast(CollectorBackend, request.app.state.collector_backend)


Backend = Annotated[CollectorBackend, Depends(get_backend)]


async def collector_error_handler(_: Request, error: Exception) -> JSONResponse:
    if isinstance(error, ResourceNotFoundError):
        status_code = 404
    elif isinstance(error, UpstreamUnavailableError):
        status_code = 503
    else:
        status_code = 502
    return JSONResponse(status_code=status_code, content={"detail": str(error)})


def create_read_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/v1", dependencies=[Depends(build_api_key_dependency(settings))])

    @router.get("/meta", response_model=CollectorMeta)
    async def meta() -> CollectorMeta:
        return CollectorMeta(
            version=__version__,
            mode="production",
            source_host=settings.source_host,
            allowed_services=sorted(settings.allowed_service_names),
        )

    @router.get("/metrics/ingestion")
    async def ingestion_metrics(backend: Backend) -> CollectorEnvelope[IngestionMetrics]:
        return await backend.get_ingestion_metrics()

    @router.get("/services")
    async def services(backend: Backend) -> CollectorEnvelope[list[ServiceState]]:
        return await backend.get_services()

    @router.get("/services/{service}")
    async def service_state(service: str, backend: Backend) -> CollectorEnvelope[ServiceState]:
        return await backend.get_service(service)

    @router.get("/services/{service}/logs")
    async def service_logs(
        service: str,
        backend: Backend,
        tail: Annotated[int, Query(ge=1, le=500)] = 200,
        contains: Annotated[str | None, Query(max_length=100)] = None,
    ) -> CollectorEnvelope[ServiceLogs]:
        return await backend.get_service_logs(service, tail=tail, contains=contains)

    @router.get("/devices/{device_id}")
    async def device_state(device_id: str, backend: Backend) -> CollectorEnvelope[DeviceState]:
        return await backend.get_device(device_id)

    return router
