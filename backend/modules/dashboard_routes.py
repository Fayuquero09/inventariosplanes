from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class DashboardRouteHandlers:
    DashboardMonthlyCloseUpsert: Any
    get_dashboard_monthly_close: Callable[..., Awaitable[Any]]
    get_dashboard_monthly_close_calendar: Callable[..., Awaitable[Any]]
    upsert_dashboard_monthly_close: Callable[[Any, Request], Awaitable[Any]]
    get_dashboard_kpis: Callable[..., Awaitable[Any]]
    get_sales_trends: Callable[..., Awaitable[Any]]
    get_seller_performance: Callable[..., Awaitable[Any]]
    get_vehicle_suggestions: Callable[..., Awaitable[Any]]


def register_dashboard_routes(router: APIRouter, handlers: DashboardRouteHandlers) -> None:
    @router.get("/dashboard/monthly-close")
    async def get_dashboard_monthly_close_route(
        request: Request,
        month: Optional[int] = None,
        year: Optional[int] = None,
        group_id: Optional[str] = None,
    ):
        return await handlers.get_dashboard_monthly_close(
            request=request,
            month=month,
            year=year,
            group_id=group_id,
        )

    @router.get("/dashboard/monthly-close-calendar")
    async def get_dashboard_monthly_close_calendar_route(
        request: Request,
        year: Optional[int] = None,
        from_current_month: bool = True,
    ):
        return await handlers.get_dashboard_monthly_close_calendar(
            request=request,
            year=year,
            from_current_month=from_current_month,
        )

    @router.put("/dashboard/monthly-close")
    async def upsert_dashboard_monthly_close_route(
        payload: handlers.DashboardMonthlyCloseUpsert,
        request: Request,
    ):
        return await handlers.upsert_dashboard_monthly_close(payload, request)

    @router.get("/dashboard/kpis")
    async def get_dashboard_kpis_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
    ):
        return await handlers.get_dashboard_kpis(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            seller_id=seller_id,
        )

    @router.get("/dashboard/trends")
    async def get_sales_trends_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        months: int = 6,
        granularity: str = "month",
    ):
        return await handlers.get_sales_trends(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            seller_id=seller_id,
            months=months,
            granularity=granularity,
        )

    @router.get("/dashboard/seller-performance")
    async def get_seller_performance_route(
        request: Request,
        agency_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        return await handlers.get_seller_performance(
            request=request,
            agency_id=agency_id,
            month=month,
            year=year,
        )

    @router.get("/dashboard/suggestions")
    async def get_vehicle_suggestions_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        limit: int = 20,
    ):
        return await handlers.get_vehicle_suggestions(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            limit=limit,
        )

