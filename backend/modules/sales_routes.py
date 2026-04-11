from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class SalesRouteHandlers:
    SaleCreate: Any
    create_sale: Callable[[Any, Request], Awaitable[Any]]
    get_sales: Callable[..., Awaitable[Any]]


def register_sales_routes(router: APIRouter, handlers: SalesRouteHandlers) -> None:
    @router.post("/sales")
    async def create_sale_route(sale_data: handlers.SaleCreate, request: Request):
        return await handlers.create_sale(sale_data, request)

    @router.get("/sales")
    async def get_sales_route(
        request: Request,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        return await handlers.get_sales(
            request=request,
            agency_id=agency_id,
            seller_id=seller_id,
            month=month,
            year=year,
        )
