FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY config /app/config

RUN pip install --no-cache-dir .

RUN useradd \
    --create-home \
    --uid 10001 \
    collector

USER collector

EXPOSE 8095

CMD [
    "uvicorn",
    "edgeops_collector.main:create_app",
    "--factory",
    "--host",
    "0.0.0.0",
    "--port",
    "8095"
]