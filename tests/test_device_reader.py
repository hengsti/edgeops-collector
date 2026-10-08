from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from edgeops_collector.production.device_reader import DeviceReader
from edgeops_collector.schemas import DeviceState


async def read_state(payload: dict[str, Any]) -> DeviceState:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/state/esp32-1"
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        return await DeviceReader(
            base_url="http://ingestion.test/",
            timeout_seconds=1,
            client=client,
        ).read("esp32-1")


def epoch_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


async def test_fresh_sensor_is_available_with_heartbeat_age() -> None:
    last_seen = datetime.now(UTC) - timedelta(seconds=5)

    result = await read_state(
        {
            "ttl_ms": 60000,
            "device_id": "esp32-1",
            "sensor": {"stale": False, "last_seen_ms": epoch_ms(last_seen), "value": {}},
        }
    )

    assert result.available is True
    assert result.last_seen is not None
    assert abs((result.last_seen - last_seen).total_seconds()) < 0.01
    assert result.heartbeat_age_seconds is not None
    assert 4 <= result.heartbeat_age_seconds < 10


async def test_stale_sensor_is_unavailable() -> None:
    last_seen = datetime.now(UTC) - timedelta(minutes=5)

    result = await read_state(
        {
            "ttl_ms": 60000,
            "device_id": "esp32-1",
            "sensor": {"stale": True, "last_seen_ms": epoch_ms(last_seen), "value": {}},
        }
    )

    assert result.available is False
    assert result.heartbeat_age_seconds is not None
    assert result.heartbeat_age_seconds >= 300


async def test_unknown_device_is_unavailable() -> None:
    result = await read_state({"ttl_ms": 60000, "device_id": "esp32-1", "sensor": None})

    assert result.available is False
    assert result.last_seen is None
    assert result.heartbeat_age_seconds is None


async def test_invalid_last_seen_yields_unknown_heartbeat_age() -> None:
    result = await read_state(
        {
            "ttl_ms": 60000,
            "device_id": "esp32-1",
            "sensor": {"stale": False, "last_seen_ms": "yesterday", "value": {}},
        }
    )

    assert result.available is True
    assert result.last_seen is None
    assert result.heartbeat_age_seconds is None
