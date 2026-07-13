import secrets
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Header, HTTPException, status

from edgeops_collector.config import Settings

ApiKeyDependency = Callable[[str | None], Awaitable[None]]


def build_api_key_dependency(settings: Settings) -> ApiKeyDependency:
    async def require_api_key(x_edgeops_key: Annotated[str | None, Header()] = None) -> None:
        exptected = settings.api_key.get_secret_value()

        if x_edgeops_key is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid collector API key",
            )

        if not secrets.compare_digest(x_edgeops_key, exptected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid collector API key",
            )

    return require_api_key
