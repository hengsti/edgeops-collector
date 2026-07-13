from pathlib import Path

from edgeops_collector.simulation.profile import (
    load_profile,
)


def test_profile_contains_required_metric_values() -> None:
    profile_path = Path(__file__).parent.parent / "config" / "simulation-profile.json"

    profile = load_profile(profile_path)

    assert "ingest_messages_enqueued_total" in profile.counter_initial_values

    assert "ingest_messages_enqueued_total" in profile.counter_rates_per_second


def test_profile_contains_ingestion_service() -> None:
    profile_path = Path(__file__).parent.parent / "config" / "simulation-profile.json"

    profile = load_profile(profile_path)

    assert "ingestion-service" in profile.services
