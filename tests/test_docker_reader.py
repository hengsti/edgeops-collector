from collections.abc import Iterator
from threading import Barrier
from unittest.mock import Mock

import pytest
from docker import DockerClient
from docker.api import APIClient
from docker.errors import DockerException
from fastapi.testclient import TestClient
from requests.exceptions import ConnectionError, Timeout

from edgeops_collector.config import Settings
from edgeops_collector.errors import ResourceNotFoundError, UpstreamUnavailableError
from edgeops_collector.main import create_app
from edgeops_collector.production.docker_reader import DockerReader

CONTAINER_ID = "a" * 64


@pytest.fixture
def docker_api(monkeypatch: pytest.MonkeyPatch) -> Iterator[Mock]:
    client = DockerClient(base_url="http://docker.test:2375", version="1.45")
    api = Mock(spec=APIClient)
    api.containers.return_value = [{"Id": CONTAINER_ID}]
    api.inspect_container.return_value = {
        "Id": CONTAINER_ID,
        "State": {"Status": "running", "Health": {"Status": "healthy"}},
        "RestartCount": 2,
    }
    api.logs.return_value = b"service healthy\nconnection failed\n"
    client.api = api
    monkeypatch.setattr(
        "edgeops_collector.production.docker_reader.docker.from_env", lambda: client
    )
    yield api
    api.close.side_effect = None
    client.close()


@pytest.fixture
def reader() -> DockerReader:
    return DockerReader(allowed_services=frozenset({"emqx"}), max_log_lines=100)


@pytest.mark.parametrize(
    ("status", "health"), [("running", "healthy"), ("running", "unhealthy"), ("exited", None)]
)
async def test_service_reads_fresh_state_with_one_inspection(
    reader: DockerReader, docker_api: Mock, status: str, health: str | None
) -> None:
    docker_api.inspect_container.return_value["State"] = {
        "Status": status,
        "Health": {"Status": health},
    }

    result = await reader.get_service("emqx")

    assert result.status == status
    assert result.health == health
    assert result.restart_count == 2
    assert docker_api.containers.call_count == 1
    assert docker_api.inspect_container.call_count == 1
    assert docker_api.containers.call_args.kwargs["all"] is True
    assert docker_api.containers.call_args.kwargs["filters"] == {
        "label": "com.docker.compose.service=emqx"
    }


async def test_disallowed_service_never_contacts_docker(
    reader: DockerReader, docker_api: Mock
) -> None:
    with pytest.raises(ResourceNotFoundError, match="not allowlisted"):
        await reader.get_service("not-allowed")
    docker_api.containers.assert_not_called()


@pytest.mark.parametrize(
    ("count", "error"), [(0, ResourceNotFoundError), (2, UpstreamUnavailableError)]
)
async def test_missing_or_ambiguous_container_is_not_reported_as_healthy(
    reader: DockerReader, docker_api: Mock, count: int, error: type[Exception]
) -> None:
    docker_api.containers.return_value = [{"Id": CONTAINER_ID}] * count
    with pytest.raises(error):
        await reader.get_service("emqx")
    docker_api.inspect_container.assert_not_called()


@pytest.mark.parametrize("operation", ["containers", "inspect_container"])
@pytest.mark.parametrize("error", [DockerException, ConnectionError, Timeout])
async def test_docker_failures_are_unavailable_evidence(
    reader: DockerReader, docker_api: Mock, operation: str, error: type[Exception]
) -> None:
    getattr(docker_api, operation).side_effect = error("Docker unavailable")
    with pytest.raises(UpstreamUnavailableError):
        await reader.get_service("emqx")
    states = await reader.get_services()
    assert len(states) == 1
    assert states[0].status == "unavailable"
    assert states[0].health is None


@pytest.mark.parametrize(
    ("attrs", "error"),
    [
        ([], Exception),
        ({"State": None}, AttributeError),
        ({"State": []}, AttributeError),
        ({"State": {"Health": None}}, AttributeError),
        ({"State": {"Health": []}}, AttributeError),
        ({"RestartCount": "invalid"}, ValueError),
        ({"RestartCount": None}, TypeError),
    ],
)
async def test_malformed_inspect_is_logged_and_propagates(
    reader: DockerReader,
    docker_api: Mock,
    caplog: pytest.LogCaptureFixture,
    attrs: object,
    error: type[Exception],
) -> None:
    docker_api.inspect_container.return_value = (
        {"Id": CONTAINER_ID, **attrs} if isinstance(attrs, dict) else attrs
    )

    with pytest.raises(error):
        await reader.get_services()

    assert any("emqx" in record.message and record.exc_info for record in caplog.records)


@pytest.mark.parametrize("error", [KeyError, TypeError])
async def test_programming_errors_are_logged_and_propagate(
    reader: DockerReader,
    docker_api: Mock,
    caplog: pytest.LogCaptureFixture,
    error: type[Exception],
) -> None:
    docker_api.inspect_container.side_effect = error("parser bug")

    with pytest.raises(error, match="parser bug"):
        await reader.get_services()

    assert any(record.exc_info for record in caplog.records)


async def test_missing_health_is_unknown(reader: DockerReader, docker_api: Mock) -> None:
    docker_api.inspect_container.return_value["State"] = {"Status": "running"}

    result = await reader.get_service("emqx")

    assert result.status == "running"
    assert result.health is None


async def test_missing_service_is_unavailable(reader: DockerReader, docker_api: Mock) -> None:
    docker_api.containers.return_value = []

    states = await reader.get_services()

    assert states[0].status == "unavailable"


async def test_failed_close_does_not_retain_the_client(
    reader: DockerReader, docker_api: Mock
) -> None:
    await reader.get_service("emqx")
    docker_api.close.side_effect = RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        await reader.close()

    await reader.close()
    docker_api.close.assert_called_once()
    docker_api.close.side_effect = None


async def test_concurrent_client_creation_does_not_leak_clients(
    docker_api: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = Barrier(2)

    def create_client() -> DockerClient:
        client = DockerClient(base_url="http://docker.test:2375", version="1.45")
        client.api = docker_api
        ready.wait(timeout=5)
        return client

    monkeypatch.setattr("edgeops_collector.production.docker_reader.docker.from_env", create_client)
    reader = DockerReader(allowed_services=frozenset({"emqx", "ingestion"}), max_log_lines=100)

    states = await reader.get_services()
    await reader.close()

    assert len(states) == 2
    assert docker_api.close.call_count == 2


def test_lifespan_closes_docker_after_a_parser_failure(docker_api: Mock) -> None:
    docker_api.inspect_container.side_effect = TypeError("parser bug")
    settings = Settings(api_key="test-key", allowed_services="emqx")

    with (
        pytest.raises(TypeError, match="parser bug"),
        TestClient(create_app(settings=settings)) as client,
    ):
        client.get("/v1/services", headers={"X-EdgeOps-Key": "test-key"})

    docker_api.close.assert_called_once()


@pytest.mark.parametrize("error", [DockerException, ConnectionError, Timeout])
async def test_client_creation_failure_is_unavailable(
    reader: DockerReader, monkeypatch: pytest.MonkeyPatch, error: type[Exception]
) -> None:
    def fail() -> DockerClient:
        raise error("Docker unavailable")

    monkeypatch.setattr("edgeops_collector.production.docker_reader.docker.from_env", fail)

    with pytest.raises(UpstreamUnavailableError):
        await reader.get_service("emqx")

    assert (await reader.get_services())[0].status == "unavailable"


@pytest.mark.parametrize("error", [DockerException, ConnectionError, Timeout])
async def test_log_request_failure_is_unavailable(
    reader: DockerReader, docker_api: Mock, error: type[Exception]
) -> None:
    docker_api.logs.side_effect = error("Docker unavailable")

    with pytest.raises(UpstreamUnavailableError):
        await reader.get_logs("emqx", tail=10, contains=None)


async def test_logs_remain_bounded_and_case_insensitively_filtered(
    reader: DockerReader, docker_api: Mock
) -> None:
    logs = await reader.get_logs("emqx", tail=999, contains="HEALTHY")
    assert logs.lines == ["service healthy"]
    assert docker_api.logs.call_args.kwargs["tail"] == 100
    assert docker_api.logs.call_args.kwargs["timestamps"] is True
