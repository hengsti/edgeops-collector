from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Final

from edgeops_collector.schemas import INGESTION_COUNTER_NAMES

COUNTER_KEYS: Final = tuple(sorted(INGESTION_COUNTER_NAMES))

LABELS: Final = {
    "normal": 0,
    "influxdb-write-failure": 1,
    "ingestion-backpressure": 2,
    "malformed-sensor-payload": 3,
    "missing-device-heartbeat": 4,
}


def safe_divide(
    numerator: float,
    denominator: float,
    default: float = 0.0,
) -> float:
    return numerator / denominator if denominator > 0.0 else default


def counter_delta(current: float, previous: float) -> float:
    """Return zero when a counter reset makes a window unusable."""
    delta = current - previous
    return delta if delta >= 0.0 else 0.0


def extract_features(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    window_seconds: float,
) -> dict[str, float]:
    """Extract inference-ready features from two collector response envelopes."""
    if window_seconds <= 0.0:
        raise ValueError("window_seconds must be greater than zero")

    previous_data = previous["data"]
    current_data = current["data"]

    delta = {
        key: counter_delta(float(current_data[key]), float(previous_data[key]))
        for key in COUNTER_KEYS
    }
    rates = {
        f"{key.removesuffix('_total')}_rate": value / window_seconds
        for key, value in delta.items()
        if key.endswith("_total")
    }

    enqueued = delta["ingest_messages_enqueued_total"]
    processed = delta["ingest_messages_processed_total"]
    transform_success = delta["ingest_transform_success_total"]
    transform_failed = delta["ingest_transform_failed_total"]
    transform_deserialize_failed = delta["ingest_transform_deserialize_failed_total"]
    transform_attempts = delta["ingest_transform_attempt_total"]
    lines_written = delta["influx_lines_written_total"]
    decode_failures = (
        delta["ingest_incoming_oversized_total"]
        + delta["ingest_incoming_non_utf8_total"]
        + delta["ingest_incoming_invalid_json_total"]
    )
    decode_inputs = delta["mqtt_messages_received_total"] + delta["ingest_event_queue_full_total"]
    raw_validation_outcomes = (
        delta["ingest_validate_raw_success_total"]
        + delta["ingest_validate_raw_ignored_total"]
        + delta["ingest_validate_raw_failed_total"]
    )
    business_validation_outcomes = (
        delta["ingest_validate_business_success_total"]
        + delta["ingest_validate_business_failed_total"]
    )
    wal_enqueue_attempts = (
        enqueued + delta["ingest_wal_queue_full_total"] + delta["ingest_queue_closed_total"]
    )
    dlq_attempts = delta["dlq_messages_published_total"] + delta["dlq_publish_errors_total"]
    wal_terminal_outcomes = (
        delta["wal_forwarder_committed_total"] + delta["wal_forwarder_drop_total"]
    )

    features = {
        **rates,
        "queue_growth_rate": (enqueued - processed) / window_seconds,
        "processing_ratio": safe_divide(
            processed,
            enqueued,
            default=1.0,
        ),
        "event_queue_drop_ratio": safe_divide(
            delta["ingest_event_queue_full_total"],
            decode_inputs,
        ),
        "wal_queue_full_ratio": safe_divide(
            delta["ingest_wal_queue_full_total"],
            wal_enqueue_attempts,
        ),
        "decode_failure_ratio": safe_divide(
            decode_failures,
            decode_inputs,
        ),
        "raw_validation_failure_ratio": safe_divide(
            delta["ingest_validate_raw_failed_total"],
            raw_validation_outcomes,
        ),
        "raw_validation_ignored_ratio": safe_divide(
            delta["ingest_validate_raw_ignored_total"],
            raw_validation_outcomes,
        ),
        "business_validation_failure_ratio": safe_divide(
            delta["ingest_validate_business_failed_total"],
            business_validation_outcomes,
        ),
        "transform_deserialize_failure_ratio": safe_divide(
            transform_deserialize_failed,
            transform_attempts,
        ),
        "transform_failure_ratio": safe_divide(
            transform_failed + transform_deserialize_failed,
            transform_attempts,
        ),
        "persistence_ratio": safe_divide(
            lines_written,
            transform_success,
            default=1.0,
        ),
        "influx_write_failure_ratio": safe_divide(
            delta["influx_write_failed_total"],
            delta["influx_write_success_total"] + delta["influx_write_failed_total"],
        ),
        "dlq_publish_failure_ratio": safe_divide(
            delta["dlq_publish_errors_total"],
            dlq_attempts,
        ),
        "wal_drop_ratio": safe_divide(
            delta["wal_forwarder_drop_total"],
            wal_terminal_outcomes,
        ),
        "decode_payload_average_bytes": safe_divide(
            delta["ingest_decode_payload_bytes_sum"],
            delta["ingest_decode_payload_bytes_count"],
        ),
        "decode_duration_average_seconds": safe_divide(
            delta["ingest_decode_duration_seconds_sum"],
            delta["ingest_decode_duration_seconds_count"],
        ),
        "raw_validation_duration_average_seconds": safe_divide(
            delta["ingest_validate_raw_duration_seconds_sum"],
            delta["ingest_validate_raw_duration_seconds_count"],
        ),
        "transform_duration_average_seconds": safe_divide(
            delta["ingest_transform_duration_seconds_sum"],
            delta["ingest_transform_duration_seconds_count"],
        ),
        "business_validation_duration_average_seconds": safe_divide(
            delta["ingest_validate_business_duration_seconds_sum"],
            delta["ingest_validate_business_duration_seconds_count"],
        ),
        "persist_duration_average_seconds": safe_divide(
            delta["ingest_persist_duration_seconds_sum"],
            delta["ingest_persist_duration_seconds_count"],
        ),
        "dlq_publish_duration_average_seconds": safe_divide(
            delta["ingest_dlq_publish_duration_seconds_sum"],
            delta["ingest_dlq_publish_duration_seconds_count"],
        ),
        "pipeline_duration_average_seconds": safe_divide(
            delta["ingest_pipeline_duration_seconds_sum"],
            delta["ingest_pipeline_duration_seconds_count"],
        ),
        "influx_write_duration_average_seconds": safe_divide(
            delta["influx_write_duration_seconds_sum"],
            delta["influx_write_duration_seconds_count"],
        ),
        "wal_retry_outage_duration_average_seconds": safe_divide(
            delta["wal_forwarder_retry_outage_seconds_sum"],
            delta["wal_forwarder_retry_outage_seconds_count"],
        ),
        "sensor_message_ratio": safe_divide(
            delta["ingest_sensor_messages_processed_total"],
            delta["ingest_sensor_messages_processed_total"]
            + delta["ingest_status_messages_processed_total"],
        ),
        "wal_forwarder_retry_outage_active": float(
            current_data["wal_forwarder_retry_outage_active"]
        ),
        "influxdb_healthy": float(current_data["influxdb_healthy"]),
    }

    queue_depth = current_data.get("ingest_queue_depth")
    queue_capacity = current_data.get("ingest_queue_capacity")

    if queue_depth is not None and queue_capacity is not None:
        depth = float(queue_depth)
        capacity = float(queue_capacity)

        features.update(
            {
                "queue_depth": depth,
                "queue_capacity": capacity,
                "queue_utilization_ratio": safe_divide(
                    depth,
                    capacity,
                ),
            }
        )

    return features


def build_training_record(
    current: Mapping[str, Any],
    features: Mapping[str, float],
) -> dict[str, Any]:
    """Combine snapshot provenance and extracted features into one labeled row."""
    metadata = current["metadata"]
    scenario_id = metadata.get("scenario_id")
    label = str(scenario_id) if scenario_id is not None else "normal"
    if label not in LABELS:
        raise ValueError(f"Unsupported training label: {label}")

    timestamp = metadata["captured_at"]
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat().replace("+00:00", "Z")

    return {
        "timestamp": timestamp,
        "simulation_run_id": metadata.get("simulation_run_id"),
        "simulation_seed": metadata.get("simulation_seed"),
        "scenario_id": scenario_id,
        "simulation_phase": metadata.get("simulation_phase") or "normal",
        "label": label,
        "label_id": LABELS[label],
        "features": dict(features),
    }
