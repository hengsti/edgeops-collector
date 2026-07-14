# EdgeOps Collector

EdgeOps Collector exposes authenticated, typed smart-home telemetry through the same HTTP API in production and simulation modes. Simulation is the default and requires no Docker socket, existing Docker network, or production smart-home services.

The current release includes a healthy baseline plus four deterministic incident scenarios. Production deployment and overlay mode remain deferred.

## Simulation quick start

You need Python 3.12 or newer and [uv](https://docs.astral.sh/uv/).

1. Copy the example configuration:

   ```bash
   cp .env.example .env
   ```

   In PowerShell, use `Copy-Item .env.example .env`.

2. Replace both example keys in `.env` with different random values. `COLLECTOR_API_KEY` reads telemetry; `COLLECTOR_SIMULATION_ADMIN_API_KEY` controls scenarios.

3. Install the locked dependencies and start the API:

   ```bash
   uv sync --locked --dev
   uv run uvicorn edgeops_collector.main:create_app --factory --host 0.0.0.0 --port 8095
   ```

4. Verify health and authentication in a second terminal:

   ```bash
   curl http://localhost:8095/health
   curl -i http://localhost:8095/v1/meta
   curl -H "X-EdgeOps-Key: YOUR_READ_KEY" http://localhost:8095/v1/meta
   ```

The health request returns HTTP 200, the unauthenticated metadata request returns HTTP 401, and the authenticated request returns HTTP 200.

## Run with Docker Compose

After preparing `.env`, run:

```bash
docker compose config --quiet
docker compose build
docker compose up -d --wait
docker compose ps
```

The container becomes `healthy`, runs as the non-root `collector` user (UID 10001), and listens on `http://localhost:8095`. Its dependencies come from the committed `uv.lock`. Stop it with:

```bash
docker compose down
```

The default Compose file is simulation-only. It does not mount `/var/run/docker.sock` or require an external network.

## Authentication

Every `/v1/*` request requires an `X-EdgeOps-Key` header. Read endpoints use `COLLECTOR_API_KEY`. Simulation-control endpoints use the separate `COLLECTOR_SIMULATION_ADMIN_API_KEY`. Missing, invalid, or wrong-purpose keys return HTTP 401.

`GET /health` is intentionally unauthenticated.

## API reference

| Method | Path | Key | Success | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/health` | None | 200 | Process health and version |
| `GET` | `/v1/meta` | Read | 200 | Mode, source host, and allowlisted services |
| `GET` | `/v1/metrics/ingestion` | Read | 200 | Ingestion counters |
| `GET` | `/v1/services` | Read | 200 | All allowlisted service states |
| `GET` | `/v1/services/{service}` | Read | 200 or 404 | One service state |
| `GET` | `/v1/services/{service}/logs` | Read | 200 or 404 | Bounded simulated logs |
| `GET` | `/v1/devices/{device_id}` | Read | 200 or 404 | Simulated device state |
| `GET` | `/v1/simulation/scenarios` | Admin | 200 | Available scenarios |
| `POST` | `/v1/simulation/runs` | Admin | 201, 404, or 409 | Start one scenario run |
| `GET` | `/v1/simulation/runs/current` | Admin | 200 or 404 | Current run and phase |
| `DELETE` | `/v1/simulation/runs/current` | Admin | 200 | Stop the current run; idempotent |

The logs endpoint accepts `tail` from 1 through 500 and an optional case-insensitive `contains` filter of up to 100 characters.

The checked-in profile simulates device `esp32-simulated-01` and these services: `nanomq`, `ingestion-service`, `influxdb`, `telegraf`, `grafana`, `device-management`, `control-ui`, and `homekit-api`.

## Response envelope

Data endpoints return a Pydantic-validated envelope:

```json
{
  "metadata": {
    "captured_at": "2026-07-12T12:00:00Z",
    "collector_mode": "simulation",
    "simulated": true,
    "source_host": "local-simulation",
    "scenario_id": null,
    "simulation_run_id": null,
    "simulation_seed": null,
    "simulation_phase": null
  },
  "data": {}
}
```

Scenario provenance fields are populated while a run is active.

## Run incident scenarios

Runtime control is enabled by `.env.example`. List scenarios with the admin key:

```bash
curl -H "X-EdgeOps-Key: YOUR_ADMIN_KEY" http://localhost:8095/v1/simulation/scenarios
```

Start a deterministic ingestion-backpressure run:

```bash
curl -X POST \
  -H "X-EdgeOps-Key: YOUR_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"scenario_id":"ingestion-backpressure","seed":42,"speed":1}' \
  http://localhost:8095/v1/simulation/runs
```

The available scenario IDs are:

- `ingestion-backpressure`
- `influxdb-write-failure`
- `malformed-sensor-payload`
- `missing-device-heartbeat`

Only one run may be active. Repeating a run with the same scenario, seed, call sequence, and virtual elapsed time produces the same telemetry sequence. Stop it with:

```bash
curl -X DELETE -H "X-EdgeOps-Key: YOUR_ADMIN_KEY" \
  http://localhost:8095/v1/simulation/runs/current
```

Set `COLLECTOR_SIMULATION_RUNTIME_ENABLED=false` to omit every simulation-control route.

## Configuration reference

All collector settings use the `COLLECTOR_` prefix. Unknown prefixed variables fail startup and their values are not included in the error.

| Variable | Default | Description |
| --- | --- | --- |
| `COLLECTOR_MODE` | `simulation` | `simulation` or explicit `production` opt-in |
| `COLLECTOR_API_KEY` | Required | Read-only API key |
| `COLLECTOR_SOURCE_HOST` | `rpi-smarthome` | Source label returned by metadata |
| `COLLECTOR_ALLOWED_SERVICES` | Built-in list | Comma-separated service allowlist |
| `COLLECTOR_HTTP_TIMEOUT_SECONDS` | `5` | Production upstream timeout, 0–60 seconds |
| `COLLECTOR_MAX_LOG_LINES` | `1000` | Production log bound, 1–5000 |
| `COLLECTOR_SIMULATION_SEED` | `42` | Baseline deterministic random seed |
| `COLLECTOR_SIMULATION_PROFILE_PATH` | `config/simulation-profile.json` | Baseline profile path |
| `COLLECTOR_SIMULATION_DEVICE_ID` | `esp32-simulated-01` | Known simulated device |
| `COLLECTOR_SIMULATION_RUNTIME_ENABLED` | `false` | Register scenario-control endpoints |
| `COLLECTOR_SIMULATION_ADMIN_API_KEY` | None | Required and distinct when runtime control is enabled |
| `COLLECTOR_SIMULATION_SCENARIO_PATH` | `config/scenarios` | Strict YAML scenario directory |

Production-only ingestion URLs are also available as `COLLECTOR_INGESTION_METRICS_URL` and `COLLECTOR_INGESTION_CACHE_URL`.

The simulation profile must define exactly every `IngestionMetrics` counter in both initial values and rates. Values must be finite and nonnegative, processed messages may not initially exceed enqueued messages, and profile services must exactly match the configured allowlist. Invalid JSON, missing files, invalid counters, and missing or unknown services fail before the API serves requests.

## Quality checks

Unix and CI users can run `make check`. The equivalent cross-platform commands are:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests
uv run pytest
docker compose config --quiet
```

Container build, health, authenticated API, and non-root checks run in the Docker-enabled CI job.

## Troubleshooting

**Startup reports an unsupported variable.** Use the canonical name shown above. Misspellings such as `COLLECTOR_HPPT_TIMEOUT_SECOND` are rejected deliberately.

**Startup cannot read the profile or scenario directory.** Run from the repository root or set the corresponding path explicitly. Confirm the file is readable and valid JSON/YAML.

**Startup reports a service mismatch.** Keep `COLLECTOR_ALLOWED_SERVICES` and the profile's `services` keys identical.

**Port 8095 is already in use.** Stop the conflicting process or change the host side of the Compose port mapping.

**A `/v1/*` request returns 401.** Send the correct key in `X-EdgeOps-Key`; scenario endpoints require the admin key, not the read key.

**A service or device returns 404.** Use an allowlisted service or the configured simulation device ID.

## Deferred production work

Raspberry Pi deployment, live smart-home ingestion and Docker inspection, production networking/socket access, overlay mode, recording/replay, TLS, firewall policy, key rotation, rate limiting, and production observability/hardening are not part of the simulation baseline. Production mode must be selected explicitly with `COLLECTOR_MODE=production` and is not configured by the default Compose deployment.
