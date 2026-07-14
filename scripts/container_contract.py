"""Exercise the public API contract against the Compose deployment."""

import json
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import parse_qs

BASE_URL = os.environ.get("COLLECTOR_TEST_URL", "http://127.0.0.1:8095")
READ_KEY = os.environ.get("COLLECTOR_API_KEY", "replace-with-a-long-random-value")
ADMIN_KEY = os.environ.get(
    "COLLECTOR_SIMULATION_ADMIN_API_KEY", "replace-with-a-different-long-random-value"
)


def request(
    method: str,
    path: str,
    *,
    key: str | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    if key is not None:
        headers["X-EdgeOps-Key"] = key
    prepared = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(prepared, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def assert_status(method: str, path: str, expected: int, **kwargs: Any) -> Any:
    status, payload = request(method, path, **kwargs)
    if status != expected:
        raise AssertionError(f"{method} {path}: expected {expected}, received {status}: {payload}")
    return payload


def _is_string_or_none(value: Any) -> bool:
    return value is None or isinstance(value, str)


def _assert_metadata(payload: Any) -> None:
    assert isinstance(payload, dict)
    assert {"captured_at", "collector_mode", "simulated", "source_host"} <= payload.keys()
    assert isinstance(payload["captured_at"], str)
    assert isinstance(payload["collector_mode"], str)
    assert isinstance(payload["simulated"], bool)
    assert isinstance(payload["source_host"], str)
    for field in ("scenario_id", "simulation_run_id", "simulation_phase"):
        if field in payload:
            assert _is_string_or_none(payload[field])
    if "simulation_seed" in payload:
        assert payload["simulation_seed"] is None or (
            isinstance(payload["simulation_seed"], int)
            and not isinstance(payload["simulation_seed"], bool)
        )


def _assert_envelope(payload: Any) -> Any:
    assert isinstance(payload, dict)
    assert {"metadata", "data"} <= payload.keys()
    _assert_metadata(payload["metadata"])
    return payload["data"]


def _assert_service_state(payload: Any) -> None:
    assert isinstance(payload, dict)
    assert {"service", "container_id", "status", "health", "restart_count"} <= payload.keys()
    assert isinstance(payload["service"], str)
    assert _is_string_or_none(payload["container_id"])
    assert isinstance(payload["status"], str)
    assert _is_string_or_none(payload["health"])
    assert (
        isinstance(payload["restart_count"], int)
        and not isinstance(payload["restart_count"], bool)
        and payload["restart_count"] >= 0
    )


def assert_response_contract(path: str, payload: Any) -> None:
    path, _, query = path.partition("?")
    if path == "/v1/meta":
        assert isinstance(payload, dict)
        assert {"version", "mode", "source_host", "allowed_services"} <= payload.keys()
        assert isinstance(payload["version"], str)
        assert isinstance(payload["mode"], str)
        assert isinstance(payload["source_host"], str)
        assert isinstance(payload["allowed_services"], list)
        assert all(isinstance(service, str) for service in payload["allowed_services"])
    elif path == "/v1/metrics/ingestion":
        data = _assert_envelope(payload)
        assert isinstance(data, dict)
        metrics = (
            "ingest_messages_enqueued_total",
            "ingest_messages_processed_total",
            "ingest_sensor_messages_processed_total",
            "ingest_status_messages_processed_total",
            "ingest_queue_full_total",
            "ingest_transform_success_total",
            "ingest_transform_failed_total",
            "influx_lines_written_total",
            "influx_write_success_total",
            "ingest_pipeline_duration_seconds_sum",
            "ingest_pipeline_duration_seconds_count",
        )
        assert all(
            metric in data
            and isinstance(data[metric], (int, float))
            and not isinstance(data[metric], bool)
            and data[metric] >= 0
            for metric in metrics
        )
    elif path == "/v1/services":
        data = _assert_envelope(payload)
        assert isinstance(data, list)
        for state in data:
            _assert_service_state(state)
    elif path == "/v1/services/ingestion-service":
        _assert_service_state(_assert_envelope(payload))
    elif path == "/v1/services/ingestion-service/logs":
        data = _assert_envelope(payload)
        assert isinstance(data, dict)
        assert {"service", "lines"} <= data.keys()
        assert isinstance(data["service"], str)
        assert isinstance(data["lines"], list)
        assert all(isinstance(line, str) for line in data["lines"])
        parameters = parse_qs(query, keep_blank_values=True)
        if "tail" in parameters:
            assert len(data["lines"]) <= int(parameters["tail"][-1])
        if "contains" in parameters:
            contains = parameters["contains"][-1].lower()
            assert all(contains in line.lower() for line in data["lines"])
    elif path == "/v1/devices/esp32-simulated-01":
        data = _assert_envelope(payload)
        assert isinstance(data, dict)
        assert {
            "device_id",
            "available",
            "last_seen",
            "rssi_dbm",
            "fw_version",
            "raw",
        } <= data.keys()
        assert isinstance(data["device_id"], str)
        assert isinstance(data["available"], bool)
        assert _is_string_or_none(data["last_seen"])
        assert data["rssi_dbm"] is None or (
            isinstance(data["rssi_dbm"], int) and not isinstance(data["rssi_dbm"], bool)
        )
        assert _is_string_or_none(data["fw_version"])
        assert isinstance(data["raw"], dict)


def main() -> None:
    health = assert_status("GET", "/health", 200)
    assert health["status"] == "ok"

    read_routes = (
        "/v1/meta",
        "/v1/metrics/ingestion",
        "/v1/services",
        "/v1/services/ingestion-service",
        "/v1/services/ingestion-service/logs?tail=1&contains=healthy",
        "/v1/devices/esp32-simulated-01",
    )
    for route in read_routes:
        assert_status("GET", route, 401)
        assert_response_contract(route, assert_status("GET", route, 200, key=READ_KEY))

    assert_status("GET", "/v1/services/not-allowed", 404, key=READ_KEY)
    assert_status("GET", "/v1/devices/not-available", 404, key=READ_KEY)

    assert_status("GET", "/v1/simulation/scenarios", 401, key=READ_KEY)
    scenarios = assert_status("GET", "/v1/simulation/scenarios", 200, key=ADMIN_KEY)
    assert len(scenarios) == 4
    run = assert_status(
        "POST",
        "/v1/simulation/runs",
        201,
        key=ADMIN_KEY,
        body={"scenario_id": "ingestion-backpressure", "seed": 42, "speed": 1},
    )
    assert run["scenario_id"] == "ingestion-backpressure"
    assert_status("GET", "/v1/simulation/runs/current", 200, key=ADMIN_KEY)
    assert_status("DELETE", "/v1/simulation/runs/current", 200, key=ADMIN_KEY)
    assert_status("DELETE", "/v1/simulation/runs/current", 200, key=ADMIN_KEY)


if __name__ == "__main__":
    main()
