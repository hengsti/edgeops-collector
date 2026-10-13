from math import isfinite

import httpx
from prometheus_client.parser import text_string_to_metric_families

from edgeops_collector.errors import (
    InvalidUpstreamResponseError,
    UpstreamUnavailableError,
)
from edgeops_collector.schemas import METRICS, IngestionMetrics

RAW_TO_CANONICAL = {
    metric.raw_name: name for name, metric in METRICS.items() if metric.raw_name is not None
}
RAW_COUNTER_METRICS = frozenset(
    metric.raw_name or name
    for name, metric in METRICS.items()
    if metric.exported and metric.kind == "counter"
)

RAW_GAUGE_METRICS = frozenset(
    metric.raw_name or name
    for name, metric in METRICS.items()
    if metric.exported and metric.kind == "gauge"
)

RAW_METRICS = RAW_COUNTER_METRICS | RAW_GAUGE_METRICS

OPTIONAL_ZERO_METRICS = frozenset(
    metric.raw_name or name
    for name, metric in METRICS.items()
    if metric.exported and metric.optional_zero
)

REQUIRED_RAW_METRICS = RAW_METRICS - OPTIONAL_ZERO_METRICS


class MetricsReader:
    def __init__(
        self,
        *,
        url: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._url = url
        self._client = client

    async def read(self) -> IngestionMetrics:
        try:
            response = await self._client.get(self._url)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError(f"Failed to fetch metrics from {self._url}") from exc

        parsed: dict[str, float] = {}

        try:
            families = text_string_to_metric_families(response.text)

            for family in families:
                for sample in family.samples:
                    if sample.name not in RAW_METRICS:
                        continue

                    value = float(sample.value)

                    if not isfinite(value) or value < 0.0:
                        raise ValueError(f"Metric {sample.name} must be finite and nonnegative")

                    if sample.name in RAW_GAUGE_METRICS:
                        # If labels are introduced later, one active outage
                        # must make the derived health value unhealthy.
                        parsed[sample.name] = max(
                            parsed.get(sample.name, 0.0),
                            value,
                        )
                    else:
                        # Counter and histogram samples may contain `kind`,
                        # `result`, or `reason` labels. The first collector
                        # contract deliberately aggregates those variants.
                        parsed[sample.name] = parsed.get(sample.name, 0.0) + value

        except (ValueError, TypeError) as exc:
            raise InvalidUpstreamResponseError(
                f"Failed to parse ingestion prometheus metrics from {self._url}"
            ) from exc

        missing = REQUIRED_RAW_METRICS.difference(parsed)

        if missing:
            missing_names = ", ".join(sorted(missing))
            raise InvalidUpstreamResponseError(
                f"Missing required ingestion prometheus metrics from {self._url}: {missing_names}"
            )

        for metric in OPTIONAL_ZERO_METRICS:
            parsed.setdefault(metric, 0.0)

        canonical = {RAW_TO_CANONICAL.get(name, name): value for name, value in parsed.items()}
        canonical["influxdb_healthy"] = (
            0.0 if parsed["wal_forwarder_retry_outage_active"] >= 1.0 else 1.0
        )

        return IngestionMetrics.model_validate(canonical)
