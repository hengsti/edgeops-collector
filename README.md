# EdgeOps Collector

A configurable telemetry collector for the EdgeOps AI portfolio project.

The collector uses the same authenticated HTTP API and Pydantic schemas in both modes:

- `production`: reads real smart-home metrics, Docker state, logs and device state.
- `simulation`: generates healthy, schema-compatible baseline telemetry.

Only the data backend changes. Downstream detectors and AI services do not need separate production and simulation clients.

## Current scope

Implemented:

- environment-based configuration;
- production and simulation modes;
- backend protocol and factory;
- API-key authentication;
- Prometheus metric parsing;
- allowlisted Docker service inspection;
- bounded Docker log retrieval;
- ingestion-cache device-state retrieval;
- typed Pydantic response models;
- deterministic simulation seed;
- monotonically increasing simulated counters;
- Docker deployment;
- unit tests.

Deferred to later phases:

- incident scenario definitions;
- runtime scenario start and stop;
- overlay mode;
- recording and replay;
- simulation-run IDs;
- collector self-observability;
- OpenTelemetry;
- persistent provenance.

## Response envelope

Every data response uses:

```json
{
  "metadata": {
    "captured_at": "2026-07-12T12:00:00Z",
    "collector_mode": "simulation",
    "simulated": true,
    "source_host": "rpi-smarthome"
  },
  "data": {}
}