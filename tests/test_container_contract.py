import sys
import types
from pathlib import Path

import pytest

scripts_package = types.ModuleType("scripts")
scripts_package.__path__ = [str(Path(__file__).parent.parent / "scripts")]
sys.modules["scripts"] = scripts_package

from scripts.container_contract import assert_response_contract  # noqa: E402


def envelope(data: object) -> dict[str, object]:
    return {
        "metadata": {
            "captured_at": "2026-07-12T12:00:00Z",
            "collector_mode": "simulation",
            "simulated": True,
            "source_host": "local-simulation",
            "scenario_id": None,
            "simulation_run_id": "run-123",
            "simulation_seed": 42,
            "simulation_phase": "steady",
        },
        "data": data,
    }


SERVICE = {
    "service": "ingestion-service",
    "container_id": None,
    "status": "running",
    "health": "healthy",
    "restart_count": 0,
}

INGESTION_METRICS = {
    "ingest_messages_enqueued_total": 10,
    "ingest_messages_processed_total": 9,
    "ingest_sensor_messages_processed_total": 8,
    "ingest_status_messages_processed_total": 1,
    "ingest_queue_full_total": 0,
    "ingest_transform_success_total": 9,
    "ingest_transform_failed_total": 0,
    "influx_lines_written_total": 9,
    "influx_write_success_total": 9,
    "ingest_pipeline_duration_seconds_sum": 1.5,
    "ingest_pipeline_duration_seconds_count": 9,
}


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/v1/meta",
            {
                "version": "0.1.0",
                "mode": "simulation",
                "source_host": "local-simulation",
                "allowed_services": ["ingestion-service"],
            },
        ),
        ("/v1/metrics/ingestion", envelope(INGESTION_METRICS)),
        ("/v1/services", envelope([SERVICE])),
        ("/v1/services/ingestion-service", envelope(SERVICE)),
        (
            "/v1/services/ingestion-service/logs?tail=1&contains=healthy",
            envelope({"service": "ingestion-service", "lines": ["service healthy"]}),
        ),
        (
            "/v1/devices/esp32-simulated-01",
            envelope(
                {
                    "device_id": "esp32-simulated-01",
                    "available": True,
                    "last_seen": "2026-07-12T12:00:00Z",
                    "rssi_dbm": -55,
                    "fw_version": None,
                    "raw": {"sensor": "simulated"},
                }
            ),
        ),
    ],
)
def test_response_contract_accepts_valid_payloads(path: str, payload: object) -> None:
    assert_response_contract(path, payload)


def test_response_contract_rejects_malformed_metadata() -> None:
    payload = envelope(INGESTION_METRICS)
    metadata = payload["metadata"]
    assert isinstance(metadata, dict)
    metadata["simulated"] = "true"

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/metrics/ingestion", payload)


def test_response_contract_rejects_malformed_endpoint_data() -> None:
    payload = envelope(INGESTION_METRICS)
    data = payload["data"]
    assert isinstance(data, dict)
    del data["ingest_queue_full_total"]

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/metrics/ingestion", payload)


def test_response_contract_rejects_logs_exceeding_requested_tail() -> None:
    payload = envelope({"service": "ingestion-service", "lines": ["first line", "second line"]})

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/services/ingestion-service/logs?tail=1", payload)


def test_response_contract_rejects_logs_missing_requested_contains_value() -> None:
    payload = envelope({"service": "ingestion-service", "lines": ["service ready"]})

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/services/ingestion-service/logs?contains=healthy", payload)


def test_response_contract_rejects_negative_ingestion_metric() -> None:
    metrics = {**INGESTION_METRICS, "ingest_messages_enqueued_total": -1}

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/metrics/ingestion", envelope(metrics))


def test_response_contract_rejects_negative_service_restart_count() -> None:
    service = {**SERVICE, "restart_count": -1}

    with pytest.raises(AssertionError):
        assert_response_contract("/v1/services/ingestion-service", envelope(service))
