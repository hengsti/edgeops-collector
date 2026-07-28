from datetime import UTC, datetime
from typing import Any

import pytest

from edgeops_collector.features import (
    LABELS,
    build_training_record,
    counter_delta,
    extract_features,
    safe_divide,
)
from edgeops_collector.schemas import INGESTION_COUNTER_NAMES


def snapshot(**overrides: float) -> dict[str, Any]:
    data = {name: 0.0 for name in INGESTION_COUNTER_NAMES}
    data.update(
        {
            "mqtt_messages_received_total": 100.0,
            "ingest_event_queue_full_total": 2.0,
            "ingest_decode_success_total": 98.0,
            "ingest_incoming_oversized_total": 1.0,
            "ingest_incoming_non_utf8_total": 1.0,
            "ingest_incoming_invalid_json_total": 1.0,
            "ingest_decode_duration_seconds_sum": 10.0,
            "ingest_decode_duration_seconds_count": 100.0,
            "ingest_validate_raw_success_total": 90.0,
            "ingest_validate_raw_ignored_total": 5.0,
            "ingest_validate_raw_failed_total": 5.0,
            "ingest_transform_attempt_total": 90.0,
            "ingest_messages_enqueued_total": 100.0,
            "ingest_wal_queue_full_total": 1.0,
            "ingest_messages_processed_total": 90.0,
            "ingest_sensor_messages_processed_total": 72.0,
            "ingest_status_messages_processed_total": 18.0,
            "ingest_transform_success_total": 89.0,
            "ingest_transform_failed_total": 1.0,
            "ingest_validate_business_success_total": 88.0,
            "ingest_validate_business_failed_total": 1.0,
            "influx_lines_written_total": 88.0,
            "influx_write_success_total": 9.0,
            "influx_write_failed_total": 1.0,
            "wal_forwarder_committed_total": 88.0,
            "ingest_pipeline_duration_seconds_sum": 40.0,
            "ingest_pipeline_duration_seconds_count": 90.0,
            "ingest_queue_depth": 10.0,
            "ingest_queue_capacity": 100.0,
            "wal_forwarder_retry_outage_active": 0.0,
            "influxdb_healthy": 1.0,
        }
    )
    data.update(overrides)
    return {
        "metadata": {
            "captured_at": datetime(2024, 1, 1, tzinfo=UTC),
            "simulation_run_id": "run-42",
            "simulation_seed": 42,
            "scenario_id": "ingestion-backpressure",
            "simulation_phase": "overloaded",
        },
        "data": data,
    }


def test_extract_features_uses_windowed_counter_deltas() -> None:
    previous = snapshot()
    current = snapshot(
        mqtt_messages_received_total=120,
        ingest_event_queue_full_total=4,
        ingest_incoming_oversized_total=2,
        ingest_incoming_non_utf8_total=2,
        ingest_incoming_invalid_json_total=3,
        ingest_decode_duration_seconds_sum=12,
        ingest_decode_duration_seconds_count=104,
        ingest_validate_raw_success_total=98,
        ingest_validate_raw_ignored_total=6,
        ingest_validate_raw_failed_total=6,
        ingest_transform_attempt_total=102,
        ingest_messages_enqueued_total=120,
        ingest_wal_queue_full_total=3,
        ingest_queue_closed_total=1,
        ingest_messages_processed_total=100,
        ingest_sensor_messages_processed_total=80,
        ingest_status_messages_processed_total=20,
        ingest_transform_success_total=99,
        ingest_transform_failed_total=2,
        ingest_transform_deserialize_failed_total=1,
        ingest_validate_business_success_total=97,
        ingest_validate_business_failed_total=2,
        influx_lines_written_total=97,
        influx_write_success_total=10,
        influx_write_failed_total=2,
        wal_forwarder_committed_total=97,
        wal_forwarder_retry_outage_active=1,
        ingest_pipeline_duration_seconds_sum=42,
        ingest_pipeline_duration_seconds_count=92,
        ingest_queue_depth=20,
        influxdb_healthy=0,
    )

    features = extract_features(previous, current, window_seconds=10)

    assert features["ingest_messages_enqueued_rate"] == 2
    assert features["queue_growth_rate"] == 1
    assert features["pipeline_duration_average_seconds"] == 1
    assert features["queue_utilization_ratio"] == 0.2
    assert features["influx_write_failed_rate"] == 0.1
    assert features["event_queue_drop_ratio"] == pytest.approx(2 / 22)
    assert features["wal_queue_full_ratio"] == pytest.approx(2 / 23)
    assert features["decode_failure_ratio"] == pytest.approx(4 / 22)
    assert features["transform_failure_ratio"] == pytest.approx(2 / 12)
    assert features["decode_duration_average_seconds"] == 0.5
    assert features["wal_forwarder_retry_outage_active"] == 1
    assert features["influxdb_healthy"] == 0


def test_feature_helpers_handle_empty_windows_and_counter_resets() -> None:
    assert safe_divide(1, 0, default=1) == 1
    assert counter_delta(2, 3) == 0
    with pytest.raises(ValueError, match="greater than zero"):
        extract_features(snapshot(), snapshot(), window_seconds=0)


def test_build_training_record_uses_stable_label_mapping() -> None:
    current = snapshot()

    record = build_training_record(current, {"queue_growth_rate": 1.5})

    assert record == {
        "timestamp": "2024-01-01T00:00:00Z",
        "simulation_run_id": "run-42",
        "simulation_seed": 42,
        "scenario_id": "ingestion-backpressure",
        "simulation_phase": "overloaded",
        "label": "ingestion-backpressure",
        "label_id": LABELS["ingestion-backpressure"],
        "features": {"queue_growth_rate": 1.5},
    }


def test_normal_training_record_has_normal_phase_and_label() -> None:
    current = snapshot()
    current["metadata"].update(
        {
            "simulation_run_id": None,
            "simulation_seed": None,
            "scenario_id": None,
            "simulation_phase": None,
        }
    )

    record = build_training_record(current, {})

    assert record["label"] == "normal"
    assert record["label_id"] == 0
    assert record["simulation_phase"] == "normal"
