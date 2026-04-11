from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class PriceBulletinsRouteHandlers:
    PriceBulletinBulkUpsert: Any
    get_price_bulletins: Callable[..., Awaitable[Any]]
    upsert_price_bulletins_bulk: Callable[[Any, Request], Awaitable[Any]]
    delete_price_bulletin: Callable[[str, Request], Awaitable[Any]]


def register_price_bulletins_routes(router: APIRouter, handlers: PriceBulletinsRouteHandlers) -> None:
    @router.get("/price-bulletins")
    async def get_price_bulletins_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        model: Optional[str] = None,
        active_only: bool = False,
        latest_per_model: bool = False,
        include_brand_defaults: bool = True,
    ):
        return await handlers.get_price_bulletins(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            model=model,
            active_only=active_only,
            latest_per_model=latest_per_model,
            include_brand_defaults=include_brand_defaults,
        )

    @router.put("/price-bulletins/bulk")
    async def upsert_price_bulletins_bulk_route(payload: handlers.PriceBulletinBulkUpsert, request: Request):
        return await handlers.upsert_price_bulletins_bulk(payload, request)

    @router.delete("/price-bulletins/{bulletin_id}")
    async def delete_price_bulletin_route(bulletin_id: str, request: Request):
        return await handlers.delete_price_bulletin(bulletin_id, request)
