import sys
import types
from pathlib import Path

import pytest

scripts_package = types.ModuleType("scripts")
scripts_package.__path__ = [str(Path(__file__).parent.parent / "scripts")]
sys.modules["scripts"] = scripts_package

from edgeops_collector.schemas import IngestionMetrics  # noqa: E402
from scripts.container_contract import (  # noqa: E402
    INGESTION_COUNTER_METRICS,
    INGESTION_GAUGE_METRICS,
    assert_response_contract,
)


def envelope(data: object) -> dict[str, object]:
    return {
        "metadata": {
            "captured_at": "2026-07-12T12:00:00Z",
            "collector_mode": "production",
            "simulated": False,
            "source_host": "rpi-smarthome",
            "scenario_id": None,
            "simulation_run_id": None,
            "simulation_seed": None,
            "simulation_phase": None,
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
    **{metric: 0 for metric in INGESTION_COUNTER_METRICS},
    **{metric: 0 for metric in INGESTION_GAUGE_METRICS},
    "ingest_queue_depth": 1,
    "ingest_queue_capacity": 100,
}


def test_container_metric_list_matches_schema() -> None:
    required = set(INGESTION_COUNTER_METRICS + INGESTION_GAUGE_METRICS)
    schema_fields = set(IngestionMetrics.model_fields) - {
        "ingest_queue_depth",
        "ingest_queue_capacity",
    }

    assert required == schema_fields
    assert "ingest_queue_full_total" not in required


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/v1/meta",
            {
                "version": "0.1.0",
                "mode": "production",
                "source_host": "rpi-smarthome",
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
                    "heartbeat_age_seconds": 0,
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
    del data["ingest_wal_queue_full_total"]

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
