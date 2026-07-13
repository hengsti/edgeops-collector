from __future__ import annotations

import asyncio

import docker
from docker.client import DockerClient
from docker.errors import DockerException
from docker.models.containers import Container

from edgeops_collector.errors import (
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import (
    ServiceLogs,
    ServiceState,
)


class DockerReader:
    def __init__(
        self,
        *,
        allowed_services: frozenset[str],
        max_log_lines: int,
    ) -> None:
        self._allowed_services = allowed_services
        self._max_log_lines = max_log_lines

        self._docker_client: DockerClient | None = None

    def _validate_service(self, service: str) -> None:
        if service not in self._allowed_services:
            raise ResourceNotFoundError(f"Service is not allowlisted: {service}")

    def _client(self) -> DockerClient:
        if self._docker_client is not None:
            return self._docker_client

        try:
            self._docker_client = docker.from_env()
            return self._docker_client
        except DockerException as exc:
            raise UpstreamUnavailableError("Docker API is unavailable") from exc

    def _find_container_sync(
        self,
        service: str,
    ) -> Container:
        self._validate_service(service)

        try:
            client = self._client()

            containers = client.containers.list(
                all=True,
                filters={"label": (f"com.docker.compose.service={service}")},
            )
        except DockerException as exc:
            raise UpstreamUnavailableError("Docker API request failed") from exc

        if not containers:
            raise ResourceNotFoundError(f"No container found for service: {service}")

        if len(containers) > 1:
            raise UpstreamUnavailableError(
                f"Multiple containers found for service {service}; the baseline expects exactly one"
            )

        return containers[0]

    def _get_service_sync(
        self,
        service: str,
    ) -> ServiceState:
        container = self._find_container_sync(service)

        try:
            container.reload()
        except DockerException as exc:
            raise UpstreamUnavailableError(
                f"Unable to refresh container for service: {service}"
            ) from exc

        state = container.attrs.get("State", {})

        health_state = state.get("Health", {})
        health = health_state.get("Status")

        return ServiceState(
            service=service,
            container_id=container.short_id,
            status=str(state.get("Status", "unknown")),
            health=(str(health) if health is not None else None),
            restart_count=int(container.attrs.get("RestartCount", 0)),
        )

    async def get_service(
        self,
        service: str,
    ) -> ServiceState:
        return await asyncio.to_thread(
            self._get_service_sync,
            service,
        )

    async def _get_service_or_unavailable(
        self,
        service: str,
    ) -> ServiceState:
        try:
            return await self.get_service(service)
        except Exception:
            return ServiceState(
                service=service,
                container_id=None,
                status="unavailable",
                health=None,
                restart_count=0,
            )

    async def get_services(self) -> list[ServiceState]:
        services = sorted(self._allowed_services)

        return list(
            await asyncio.gather(
                *(self._get_service_or_unavailable(service) for service in services)
            )
        )

    def _get_logs_sync(
        self,
        service: str,
        *,
        tail: int,
        contains: str | None,
    ) -> ServiceLogs:
        container = self._find_container_sync(service)

        safe_tail = min(
            max(tail, 1),
            self._max_log_lines,
        )

        try:
            raw = container.logs(
                tail=safe_tail,
                timestamps=True,
            ).decode(
                "utf-8",
                errors="replace",
            )
        except DockerException as exc:
            raise UpstreamUnavailableError(f"Unable to read logs for service: {service}") from exc

        lines = raw.splitlines()

        if contains:
            needle = contains.casefold()

            lines = [line for line in lines if needle in line.casefold()]

        return ServiceLogs(
            service=service,
            lines=lines,
        )

    async def get_logs(
        self,
        service: str,
        *,
        tail: int,
        contains: str | None,
    ) -> ServiceLogs:
        return await asyncio.to_thread(
            self._get_logs_sync,
            service,
            tail=tail,
            contains=contains,
        )

    async def close(self) -> None:
        if self._docker_client is None:
            return

        await asyncio.to_thread(self._docker_client.close)

        self._docker_client = None
