import secrets
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Header, HTTPException, status
from pydantic import SecretStr

from edgeops_collector.config import Settings

ApiKeyDependency = Callable[[str | None], Awaitable[None]]


def build_api_key_dependency(settings: Settings) -> ApiKeyDependency:
    return build_secret_dependency(settings.api_key, "collector")


def build_secret_dependency(secret: SecretStr, key_name: str) -> ApiKeyDependency:
    async def require_api_key(x_edgeops_key: Annotated[str | None, Header()] = None) -> None:
        expected = secret.get_secret_value()

        if x_edgeops_key is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {key_name} API key",
            )

        if not secrets.compare_digest(x_edgeops_key, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid {key_name} API key",
            )

    return require_api_key
