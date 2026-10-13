from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI

from edgeops_collector import __version__
from edgeops_collector.api import collector_error_handler, create_read_router
from edgeops_collector.backends.base import CollectorBackend
from edgeops_collector.backends.production import ProductionBackend
from edgeops_collector.config import (
    Settings,
    get_settings,
)
from edgeops_collector.errors import CollectorError
from edgeops_collector.production.device_reader import DeviceReader
from edgeops_collector.production.docker_reader import DockerReader
from edgeops_collector.production.metrics_reader import MetricsReader


def create_app(
    *,
    settings: Settings | None = None,
    backend: CollectorBackend | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as resources:
            resolved_backend = backend
            if resolved_backend is None:
                client = await resources.enter_async_context(
                    httpx.AsyncClient(timeout=resolved_settings.http_timeout_seconds)
                )
                resolved_backend = ProductionBackend(
                    source_host=resolved_settings.source_host,
                    metrics_reader=MetricsReader(
                        url=resolved_settings.ingestion_metrics_url, client=client
                    ),
                    device_reader=DeviceReader(
                        base_url=resolved_settings.ingestion_cache_url, client=client
                    ),
                    docker_reader=DockerReader(
                        allowed_services=resolved_settings.allowed_service_names,
                        max_log_lines=resolved_settings.max_log_lines,
                    ),
                )
            resources.push_async_callback(resolved_backend.close)
            app.state.collector_backend = resolved_backend
            yield

    app = FastAPI(
        title="EdgeOps Collector",
        version=__version__,
        lifespan=lifespan,
    )

    app.add_exception_handler(CollectorError, collector_error_handler)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "version": __version__,
        }

    app.include_router(create_read_router(resolved_settings))
    return app


def run() -> None:
    uvicorn.run(
        "edgeops_collector.main:create_app",
        host="0.0.0.0",
        port=8095,
        reload=False,
        factory=True,
    )
