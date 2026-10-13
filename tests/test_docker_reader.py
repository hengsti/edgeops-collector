from collections.abc import Iterator
from unittest.mock import Mock

import pytest
from docker import DockerClient
from docker.api import APIClient
from docker.errors import DockerException

from edgeops_collector.errors import ResourceNotFoundError, UpstreamUnavailableError
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
async def test_docker_failures_are_unavailable_evidence(
    reader: DockerReader, docker_api: Mock, operation: str
) -> None:
    getattr(docker_api, operation).side_effect = DockerException("Docker unavailable")
    with pytest.raises(UpstreamUnavailableError):
        await reader.get_service("emqx")
    states = await reader.get_services()
    assert len(states) == 1
    assert states[0].status == "unavailable"
    assert states[0].health is None


async def test_logs_remain_bounded_and_case_insensitively_filtered(
    reader: DockerReader, docker_api: Mock
) -> None:
    logs = await reader.get_logs("emqx", tail=999, contains="HEALTHY")
    assert logs.lines == ["service healthy"]
    assert docker_api.logs.call_args.kwargs["tail"] == 100
    assert docker_api.logs.call_args.kwargs["timestamps"] is True
