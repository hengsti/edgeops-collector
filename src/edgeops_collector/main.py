from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
)
from fastapi.responses import JSONResponse

from edgeops_collector import __version__
from edgeops_collector.backends.base import (
    CollectorBackend,
    SimulationControlBackend,
)
from edgeops_collector.backends.factory import (
    create_backend,
)
from edgeops_collector.config import (
    Settings,
    get_settings,
)
from edgeops_collector.errors import (
    CollectorError,
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import (
    CollectorEnvelope,
    CollectorMeta,
    DeviceState,
    IngestionMetrics,
    ServiceLogs,
    ServiceState,
    SimulationRunRequest,
    SimulationRunState,
    SimulationScenarioSummary,
)
from edgeops_collector.security import (
    build_api_key_dependency,
    build_secret_dependency,
)


def create_app(
    *,
    settings: Settings | None = None,
    backend: CollectorBackend | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()

    resolved_backend = backend if backend is not None else create_backend(resolved_settings)

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
            mode=resolved_settings.mode,
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

    if resolved_settings.simulation_runtime_enabled:
        admin_key = resolved_settings.simulation_admin_api_key
        if admin_key is None or not isinstance(resolved_backend, SimulationControlBackend):
            raise ValueError("Simulation runtime backend is not configured correctly")

        require_admin_key = build_secret_dependency(admin_key, "simulation admin")

        @app.get(
            "/v1/simulation/scenarios",
            dependencies=[Depends(require_admin_key)],
            response_model=list[SimulationScenarioSummary],
        )
        async def simulation_scenarios() -> list[SimulationScenarioSummary]:
            return await resolved_backend.list_scenarios()

        @app.post(
            "/v1/simulation/runs",
            status_code=201,
            dependencies=[Depends(require_admin_key)],
            response_model=SimulationRunState,
        )
        async def start_simulation_run(request: SimulationRunRequest) -> SimulationRunState:
            try:
                return await resolved_backend.start_run(request)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

        @app.get(
            "/v1/simulation/runs/current",
            dependencies=[Depends(require_admin_key)],
            response_model=SimulationRunState,
        )
        async def current_simulation_run() -> SimulationRunState:
            run_state = await resolved_backend.get_current_run()
            if run_state is None:
                raise ResourceNotFoundError("No simulation run is active")
            return run_state

        @app.delete(
            "/v1/simulation/runs/current",
            dependencies=[Depends(require_admin_key)],
            response_model=SimulationRunState | None,
        )
        async def stop_simulation_run() -> SimulationRunState | None:
            return await resolved_backend.stop_run()

    return app


def run() -> None:
    uvicorn.run(
        "edgeops_collector.main:create_app",
        host="0.0.0.0",
        port=8095,
        reload=False,
        factory=True,
    )
