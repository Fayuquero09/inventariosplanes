from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class FinancialRatesRouteHandlers:
    FinancialRateCreate: Any
    FinancialRateBulkApplyRequest: Any
    create_financial_rate: Callable[[Any, Request], Awaitable[Any]]
    apply_group_default_financial_rate: Callable[[Any, Request], Awaitable[Any]]
    get_financial_rates: Callable[..., Awaitable[Any]]
    update_financial_rate: Callable[[str, Any, Request], Awaitable[Any]]
    delete_financial_rate: Callable[[str, Request], Awaitable[Any]]


def register_financial_rates_routes(router: APIRouter, handlers: FinancialRatesRouteHandlers) -> None:
    @router.post("/financial-rates")
    async def create_financial_rate_route(rate_data: handlers.FinancialRateCreate, request: Request):
        return await handlers.create_financial_rate(rate_data, request)

    @router.post("/financial-rates/apply-group-default")
    async def apply_group_default_financial_rate_route(
        payload: handlers.FinancialRateBulkApplyRequest,
        request: Request,
    ):
        return await handlers.apply_group_default_financial_rate(payload, request)

    @router.get("/financial-rates")
    async def get_financial_rates_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
    ):
        return await handlers.get_financial_rates(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
        )

    @router.put("/financial-rates/{rate_id}")
    async def update_financial_rate_route(
        rate_id: str,
        rate_data: handlers.FinancialRateCreate,
        request: Request,
    ):
        return await handlers.update_financial_rate(rate_id, rate_data, request)

    @router.delete("/financial-rates/{rate_id}")
    async def delete_financial_rate_route(rate_id: str, request: Request):
        return await handlers.delete_financial_rate(rate_id, request)

