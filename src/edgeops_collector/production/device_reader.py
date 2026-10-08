from datetime import UTC, datetime
from typing import Any

import httpx

from edgeops_collector.errors import (
    InvalidUpstreamResponseError,
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import DeviceState


def _parse_last_seen_ms(value: object) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None

    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


class DeviceReader:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._owns_client = client is None

    async def read(
        self,
        device_id: str,
    ) -> DeviceState:
        url = f"{self._base_url}/v1/state/{device_id}"

        try:
            response = await self._client.get(url)
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError(f"Unable to read device state from {url}") from exc

        if response.status_code == 404:
            raise ResourceNotFoundError(f"Device not found: {device_id}")

        try:
            response.raise_for_status()
            payload: dict[str, Any] = response.json()
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError(f"Device-state request failed for {device_id}") from exc
        except ValueError as exc:
            raise InvalidUpstreamResponseError(
                f"Invalid JSON device-state response for {device_id}"
            ) from exc

        if not isinstance(payload, dict):
            raise InvalidUpstreamResponseError(f"Device state for {device_id} is not an object")

        sensor = payload.get("sensor")

        if sensor is None:
            return DeviceState(device_id=device_id, available=False, raw=payload)

        if not isinstance(sensor, dict):
            raise InvalidUpstreamResponseError(f"Sensor state for {device_id} is not an object")

        last_seen = _parse_last_seen_ms(sensor.get("last_seen_ms"))
        heartbeat_age_seconds = (
            max(0.0, (datetime.now(UTC) - last_seen).total_seconds())
            if last_seen is not None
            else None
        )

        return DeviceState(
            device_id=device_id,
            available=not sensor.get("stale", False),
            last_seen=last_seen,
            heartbeat_age_seconds=heartbeat_age_seconds,
            raw=payload,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
