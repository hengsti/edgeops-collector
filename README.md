# EdgeOps Collector

`edgeops-collector` is the production-only telemetry middleware for EdgeOps AI. It reads live
Prometheus metrics and the ingestion device cache, inspects allowlisted Docker services, and serves
the stable collector read API on port `8095`.

Simulation is intentionally not part of this project. It lives in the independent sibling project
`edgeops-collector-simulation`. The production API does not register `/v1/simulation/*`.

## Local development

You need Python 3.12 or newer and `uv`.

```powershell
Copy-Item .env.example .env
uv sync --locked --dev
uv run uvicorn edgeops_collector.main:create_app --factory --host 0.0.0.0 --port 8095
```

Replace the example `COLLECTOR_API_KEY` before starting.

## Docker Compose

`compose.yaml` defines the `edgeops-collector` service. It reads its settings from the untracked
`.env` next to it and only exposes port `8095` on the Compose network.

Standalone:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d --wait
docker compose exec -T edgeops-collector python - < scripts/container_contract.py
```

In the Server stack, the stack's `docker-compose.yaml` includes this file together with the
stack-side `edgeops-collector.override.yaml`:

```yaml
include:
  - path:
      - ./edgeops-ai/collector/compose.yaml
      - ./edgeops-collector.override.yaml
    env_file: ./.env
```

The override publishes `8095:8095` on the host and mounts the Docker socket read-only with
`group_add: ["${DOCKER_GID}"]`, so the non-root collector can inspect the allowlisted services. Set
`DOCKER_GID` in the stack `.env` (`getent group docker | cut -d: -f3`). Other stack services reach
the collector at `http://edgeops-collector:8095`.

## Authentication and routes

`GET /health` is public. Every `/v1/*` read route requires the production key in the
`X-EdgeOps-Key` HTTP header. The key is never accepted as a query parameter.

| Method | Path | Result |
| --- | --- | --- |
| `GET` | `/health` | Process health and version |
| `GET` | `/v1/meta` | Fixed production metadata |
| `GET` | `/v1/metrics/ingestion` | Canonical ingestion metrics |
| `GET` | `/v1/devices/{device_id}` | Live device-cache state |
| `GET` | `/v1/services` | Allowlisted service states |
| `GET` | `/v1/services/{service}` | One allowlisted service |
| `GET` | `/v1/services/{service}/logs` | Bounded, filtered logs |

Every data envelope has fixed provenance:

```json
{
  "metadata": {
    "captured_at": "2026-07-12T12:00:00Z",
    "collector_mode": "production",
    "simulated": false,
    "source_host": "rpi-smarthome"
  },
  "data": {}
}
```

The logs endpoint accepts `tail=1..500` and an optional case-insensitive `contains` filter of at
most 100 characters. Services remain fail-closed behind `COLLECTOR_ALLOWED_SERVICES`.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `COLLECTOR_API_KEY` | Required | Read-route API key |
| `COLLECTOR_SOURCE_HOST` | `rpi-smarthome` | Source metadata label |
| `COLLECTOR_INGESTION_METRICS_URL` | `http://ingest:9090/metrics` | Prometheus endpoint |
| `COLLECTOR_INGESTION_CACHE_URL` | `http://ingest:8085` | Device-cache base URL |
| `COLLECTOR_ALLOWED_SERVICES` | `emqx,ingest,influxdb,telegraf,grafana,device-management,control-ui,apple-homekit-api` | Docker service allowlist (Compose service names) |
| `COLLECTOR_HTTP_TIMEOUT_SECONDS` | `5` | Upstream timeout |
| `COLLECTOR_MAX_LOG_LINES` | `1000` | In-memory log bound |

Unknown `COLLECTOR_` variables fail startup without exposing their values. Removed settings such as
`COLLECTOR_MODE` and every `COLLECTOR_SIMULATION_*` variable are rejected.

## Postman

Create variables `baseUrl=http://localhost:8095` and `readKey=<production key>`. Send, for example,
`GET {{baseUrl}}/v1/meta` with header `X-EdgeOps-Key: {{readKey}}`. Assert status `200`,
`mode === "production"`, and that `/v1/simulation/scenarios` returns `404`.

## Quality checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## License

Released under the [MIT License](LICENSE).
