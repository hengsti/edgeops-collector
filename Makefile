.PHONY: install run test lint format typecheck check docker-build docker-up docker-down

install:
	uv sync --dev

run:
	uv run uvicorn edgeops_collector.main:create_app --factory --reload --host 0.0.0.0 --port 8095

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run mypy src tests

check: lint format-check typecheck test

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down