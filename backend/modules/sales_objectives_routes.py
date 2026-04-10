from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class SalesObjectivesRouteHandlers:
    SalesObjectiveCreate: Any
    SalesObjectiveApprovalAction: Any
    create_sales_objective: Callable[[Any, Request], Awaitable[Any]]
    get_sales_objectives: Callable[..., Awaitable[Any]]
    get_sales_objective_suggestion: Callable[..., Awaitable[Any]]
    update_sales_objective: Callable[[str, Any, Request], Awaitable[Any]]
    approve_sales_objective: Callable[[str, Any, Request], Awaitable[Any]]


def register_sales_objectives_routes(router: APIRouter, handlers: SalesObjectivesRouteHandlers) -> None:
    @router.post("/sales-objectives")
    async def create_sales_objective_route(
        objective_data: handlers.SalesObjectiveCreate,
        request: Request,
    ):
        return await handlers.create_sales_objective(objective_data, request)

    @router.get("/sales-objectives")
    async def get_sales_objectives_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
        include_seller_objectives: bool = False,
    ):
        return await handlers.get_sales_objectives(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            seller_id=seller_id,
            month=month,
            year=year,
            include_seller_objectives=include_seller_objectives,
        )

    @router.get("/sales-objectives/suggestion")
    async def get_sales_objective_suggestion_route(
        request: Request,
        agency_id: str,
        seller_id: str,
        month: Optional[int] = None,
        year: Optional[int] = None,
        lookback_months: int = 6,
    ):
        return await handlers.get_sales_objective_suggestion(
            request=request,
            agency_id=agency_id,
            seller_id=seller_id,
            month=month,
            year=year,
            lookback_months=lookback_months,
        )

    @router.put("/sales-objectives/{objective_id}")
    async def update_sales_objective_route(
        objective_id: str,
        objective_data: handlers.SalesObjectiveCreate,
        request: Request,
    ):
        return await handlers.update_sales_objective(objective_id, objective_data, request)

    @router.post("/sales-objectives/{objective_id}/approval")
    async def approve_sales_objective_route(
        objective_id: str,
        approval: handlers.SalesObjectiveApprovalAction,
        request: Request,
    ):
        return await handlers.approve_sales_objective(objective_id, approval, request)

