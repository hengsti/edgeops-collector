from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Final

COUNTER_KEYS: Final = (
    "ingest_messages_enqueued_total",
    "ingest_messages_processed_total",
    "ingest_sensor_messages_processed_total",
    "ingest_status_messages_processed_total",
    "ingest_queue_full_total",
    "ingest_transform_success_total",
    "ingest_transform_failed_total",
    "influx_lines_written_total",
    "influx_write_success_total",
    "influx_write_failed_total",
    "ingest_pipeline_duration_seconds_sum",
    "ingest_pipeline_duration_seconds_count",
)

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
    lines_written = delta["influx_lines_written_total"]
    duration_sum = delta["ingest_pipeline_duration_seconds_sum"]
    duration_count = delta["ingest_pipeline_duration_seconds_count"]
    queue_depth = float(current_data["ingest_queue_depth"])
    queue_capacity = float(current_data["ingest_queue_capacity"])

    return {
        **rates,
        "queue_growth_rate": (enqueued - processed) / window_seconds,
        "processing_ratio": safe_divide(processed, enqueued, default=1.0),
        "queue_full_ratio": safe_divide(delta["ingest_queue_full_total"], enqueued),
        "transform_failure_ratio": safe_divide(
            transform_failed,
            transform_success + transform_failed,
        ),
        "persistence_ratio": safe_divide(lines_written, transform_success, default=1.0),
        "pipeline_duration_average_seconds": safe_divide(duration_sum, duration_count),
        "sensor_message_ratio": safe_divide(
            delta["ingest_sensor_messages_processed_total"],
            delta["ingest_sensor_messages_processed_total"]
            + delta["ingest_status_messages_processed_total"],
        ),
        "queue_depth": queue_depth,
        "queue_capacity": queue_capacity,
        "queue_utilization_ratio": safe_divide(queue_depth, queue_capacity),
        "influxdb_healthy": float(current_data["influxdb_healthy"]),
    }


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
