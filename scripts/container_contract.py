"""Exercise the public API contract against the Compose deployment."""

import json
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import parse_qs

BASE_URL = os.environ.get("COLLECTOR_TEST_URL", "http://127.0.0.1:8095")
READ_KEY = os.environ.get("COLLECTOR_API_KEY", "replace-with-a-long-random-value")
DEVICE_ID = os.environ.get("COLLECTOR_TEST_DEVICE_ID", "esp32-production-01")

INGESTION_COUNTER_METRICS = (
    "mqtt_messages_received_total",
    "ingest_event_queue_full_total",
    "ingest_decode_success_total",
    "ingest_incoming_oversized_total",
    "ingest_incoming_non_utf8_total",
    "ingest_incoming_invalid_json_total",
    "ingest_decode_payload_bytes_sum",
    "ingest_decode_payload_bytes_count",
    "ingest_decode_duration_seconds_sum",
    "ingest_decode_duration_seconds_count",
    "ingest_validate_raw_success_total",
    "ingest_validate_raw_ignored_total",
    "ingest_validate_raw_failed_total",
    "ingest_validate_raw_duration_seconds_sum",
    "ingest_validate_raw_duration_seconds_count",
    "ingest_transform_attempt_total",
    "ingest_transform_success_total",
    "ingest_transform_failed_total",
    "ingest_transform_deserialize_failed_total",
    "ingest_transform_ignored_total",
    "ingest_transform_duration_seconds_sum",
    "ingest_transform_duration_seconds_count",
    "ingest_validate_business_success_total",
    "ingest_validate_business_failed_total",
    "ingest_validate_business_duration_seconds_sum",
    "ingest_validate_business_duration_seconds_count",
    "ingest_cache_updates_total",
    "ingest_messages_enqueued_total",
    "ingest_wal_queue_full_total",
    "ingest_queue_closed_total",
    "ingest_durability_ack_failed_total",
    "ingest_persist_duration_seconds_sum",
    "ingest_persist_duration_seconds_count",
    "dlq_messages_published_total",
    "dlq_publish_errors_total",
    "ingest_dlq_publish_duration_seconds_sum",
    "ingest_dlq_publish_duration_seconds_count",
    "ingest_messages_processed_total",
    "ingest_sensor_messages_processed_total",
    "ingest_status_messages_processed_total",
    "influx_lines_written_total",
    "influx_write_success_total",
    "influx_write_failed_total",
    "influx_write_duration_seconds_sum",
    "influx_write_duration_seconds_count",
    "ingest_pipeline_duration_seconds_sum",
    "ingest_pipeline_duration_seconds_count",
    "wal_forwarder_committed_total",
    "wal_forwarder_drop_total",
    "wal_forwarder_retry_total",
    "wal_forwarder_commit_retry_total",
    "wal_forwarder_retry_outage_seconds_sum",
    "wal_forwarder_retry_outage_seconds_count",
    "wal_subscription_corrupt_skipped_total",
    "wal_writer_fatal_total",
)

INGESTION_GAUGE_METRICS = (
    "wal_forwarder_retry_outage_active",
    "influxdb_healthy",
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
    assert payload.keys() == {"captured_at", "collector_mode", "simulated", "source_host"}
    assert isinstance(payload["captured_at"], str)
    assert payload["collector_mode"] == "production"
    assert payload["simulated"] is False
    assert isinstance(payload["source_host"], str)


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
        assert all(
            metric in data
            and isinstance(data[metric], (int, float))
            and not isinstance(data[metric], bool)
            and data[metric] >= 0
            for metric in INGESTION_COUNTER_METRICS + INGESTION_GAUGE_METRICS
        )
        queue_depth = data.get("ingest_queue_depth")
        queue_capacity = data.get("ingest_queue_capacity")

        assert (queue_depth is None) == (queue_capacity is None)

        if queue_depth is not None and queue_capacity is not None:
            assert isinstance(queue_depth, (int, float))
            assert isinstance(queue_capacity, (int, float))
            assert queue_depth >= 0
            assert queue_capacity > 0
            assert queue_depth <= queue_capacity

        assert data["wal_forwarder_retry_outage_active"] <= 1
        assert data["influxdb_healthy"] <= 1
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
    elif path.startswith("/v1/devices/"):
        data = _assert_envelope(payload)
        assert isinstance(data, dict)
        assert {
            "device_id",
            "available",
            "last_seen",
            "heartbeat_age_seconds",
            "rssi_dbm",
            "fw_version",
            "raw",
        } <= data.keys()
        assert isinstance(data["device_id"], str)
        assert isinstance(data["available"], bool)
        assert _is_string_or_none(data["last_seen"])
        assert data["heartbeat_age_seconds"] is None or (
            isinstance(data["heartbeat_age_seconds"], (int, float))
            and not isinstance(data["heartbeat_age_seconds"], bool)
            and data["heartbeat_age_seconds"] >= 0
        )
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
        "/v1/services",
    )
    for route in read_routes:
        assert_status("GET", route, 401)
        assert_response_contract(route, assert_status("GET", route, 200, key=READ_KEY))

    # The Compose deployment runs without ingest and without a Docker socket.
    upstream_routes = (
        "/v1/metrics/ingestion",
        "/v1/services/ingestion-service",
        "/v1/services/ingestion-service/logs?tail=1&contains=healthy",
        f"/v1/devices/{DEVICE_ID}",
    )
    for route in upstream_routes:
        assert_status("GET", route, 401)
        assert_status("GET", route, 503, key=READ_KEY)

    assert_status("GET", "/v1/services/not-allowed", 404, key=READ_KEY)

    for path in (
        "/v1/simulation/scenarios",
        "/v1/simulation/runs",
        "/v1/simulation/runs/current",
    ):
        assert_status("GET", path, 404, key=READ_KEY)


if __name__ == "__main__":
    main()
