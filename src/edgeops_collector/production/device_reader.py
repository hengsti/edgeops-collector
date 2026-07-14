from datetime import UTC, datetime
from typing import Any

import httpx

from edgeops_collector.errors import (
    InvalidUpstreamResponseError,
    ResourceNotFoundError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import DeviceState


class DeviceReader:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
    ) -> None:
        self._base_url = base_url.rstrip("/")

        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
        )

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

        state_candidate = payload.get("state", payload)

        if not isinstance(state_candidate, dict):
            raise InvalidUpstreamResponseError(f"Device state for {device_id} is not an object")

        state: dict[str, Any] = state_candidate

        status_candidate = state.get("status", {})

        status_data: dict[str, Any] = status_candidate if isinstance(status_candidate, dict) else {}

        last_seen_raw = (
            state.get("last_seen") or state.get("time_iso") or status_data.get("time_iso")
        )

        last_seen: datetime | None = None

        if isinstance(last_seen_raw, str):
            try:
                last_seen = datetime.fromisoformat(
                    last_seen_raw.replace(
                        "Z",
                        "+00:00",
                    )
                )
            except ValueError:
                last_seen = None

        if last_seen is not None and last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=UTC)

        rssi = status_data.get("rssi") or status_data.get("rssi_dbm") or state.get("rssi")

        rssi_dbm: int | None = None

        if isinstance(rssi, int | float):
            rssi_dbm = int(rssi)

        fw_version = state.get("fw_version")

        return DeviceState(
            device_id=device_id,
            available=True,
            last_seen=last_seen,
            rssi_dbm=rssi_dbm,
            fw_version=(str(fw_version) if fw_version is not None else None),
            raw=payload,
        )

    async def close(self) -> None:
        await self._client.aclose()
