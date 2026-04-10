from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter


@dataclass(frozen=True)
class HealthRouteHandlers:
    root: Callable[[], Awaitable[Any]]
    health: Callable[[], Awaitable[Any]]


def register_health_routes(router: APIRouter, handlers: HealthRouteHandlers) -> None:
    @router.get("/")
    async def root_route():
        return await handlers.root()

    @router.get("/health")
    async def health_route():
        return await handlers.health()

