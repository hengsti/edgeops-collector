import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from edgeops_collector.config import Settings
from edgeops_collector.simulation.profile import (
    load_profile,
    validate_profile,
)


def test_profile_contains_required_metric_values() -> None:
    profile_path = Path(__file__).parent.parent / "config" / "simulation-profile.json"

    profile = load_profile(profile_path)

    assert "ingest_messages_enqueued_total" in profile.counter_initial_values

    assert "ingest_messages_enqueued_total" in profile.counter_rates_per_second
    assert profile.gauge_initial_values["ingest_queue_capacity"] > 0


def test_profile_contains_ingestion_service() -> None:
    profile_path = Path(__file__).parent.parent / "config" / "simulation-profile.json"

    profile = load_profile(profile_path)

    assert "ingestion-service" in profile.services


@pytest.mark.parametrize(
    ("field", "mutation"),
    [
        ("counter_initial_values", ("remove", "influx_write_success_total", None)),
        ("counter_rates_per_second", ("add", "unexpected_total", 1.0)),
        ("counter_initial_values", ("set", "ingest_queue_full_total", -1.0)),
        ("counter_rates_per_second", ("set", "ingest_queue_full_total", float("inf"))),
        ("counter_rates_per_second", ("set", "ingest_queue_full_total", float("nan"))),
        ("gauge_initial_values", ("remove", "ingest_queue_depth", None)),
        ("gauge_initial_values", ("set", "ingest_queue_capacity", 0.0)),
        ("gauge_initial_values", ("set", "influxdb_healthy", 2.0)),
    ],
)
def test_invalid_metric_contract_is_rejected(
    tmp_path: Path, field: str, mutation: tuple[str, str, float | None]
) -> None:
    source = Path(__file__).parent.parent / "config" / "simulation-profile.json"
    raw = json.loads(source.read_text(encoding="utf-8"))
    operation, metric, value = mutation
    metrics = raw[field]
    if operation == "remove":
        metrics.pop(metric)
    else:
        metrics[metric] = value
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValidationError):
        load_profile(path)


def test_profile_services_must_match_allowlist(simulation_settings: Settings) -> None:
    profile = load_profile(simulation_settings.simulation_profile_path)
    profile.services.pop("grafana")

    with pytest.raises(ValueError, match="missing services"):
        validate_profile(profile, simulation_settings)
