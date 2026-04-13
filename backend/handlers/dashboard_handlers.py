from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import HTTPException, Request


def build_dashboard_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    validate_scope_filters: Callable[..., None],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    resolve_dashboard_scope_group_id: Callable[[Dict[str, Any]], Awaitable[Optional[str]]],
    find_dashboard_monthly_close: Callable[..., Awaitable[Any]],
    build_dashboard_monthly_close_response: Callable[..., Dict[str, Any]],
    mexico_lft_holidays_by_month: Callable[[int], Dict[int, Any]],
    list_global_monthly_closes_by_year: Callable[..., Awaitable[Any]],
    build_dashboard_monthly_close_calendar: Callable[..., Dict[str, Any]],
    upsert_global_monthly_close: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    add_months_ym: Callable[[int, int, int], Any],
    log_audit_event: Callable[..., Awaitable[None]],
    app_admin_role: str,
    user_role_seller: str,
    empty_dashboard_kpis_response: Callable[[], Dict[str, Any]],
    compute_dashboard_kpis: Callable[..., Awaitable[Dict[str, Any]]],
    list_vehicles: Callable[..., Awaitable[Any]],
    list_sales: Callable[..., Awaitable[Any]],
    list_vehicles_by_ids: Callable[..., Awaitable[Any]],
    list_agencies_by_brand_id: Callable[..., Awaitable[Any]],
    list_agencies_by_group_id: Callable[..., Awaitable[Any]],
    count_users: Callable[..., Awaitable[int]],
    count_sales: Callable[..., Awaitable[int]],
    enrich_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    calculate_vehicle_financial_cost_in_period: Callable[..., Awaitable[float]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    compute_sales_trends: Callable[..., Awaitable[Any]],
    objective_approved: str,
    objective_pending: str,
    list_sales_objectives: Callable[..., Awaitable[Any]],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    decrement_month: Callable[[datetime, int], datetime],
    compute_operational_day_profile: Callable[..., Dict[str, Any]],
    resolve_effective_objective_units: Callable[..., float],
    compute_seller_performance: Callable[..., Awaitable[Any]],
    find_user_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    collect_vehicle_suggestions: Callable[..., Awaitable[Any]],
    build_vehicle_aging_suggestion: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
):
    async def get_dashboard_monthly_close(
        request: Request,
        month: Optional[int] = None,
        year: Optional[int] = None,
        group_id: Optional[str] = None,
    ):
        current_user = await get_current_user(request)
        now = datetime.now(timezone.utc)
        target_month = int(month or now.month)
        target_year = int(year or now.year)

        if target_month < 1 or target_month > 12:
            raise HTTPException(status_code=400, detail="Month must be between 1 and 12")

        validate_scope_filters(current_user, group_id=group_id)
        effective_group_id = group_id
        if not effective_group_id:
            scope_query = build_scope_query(current_user)
            effective_group_id = await resolve_dashboard_scope_group_id(scope_query)

        close_doc, close_scope = await find_dashboard_monthly_close(
            year=target_year,
            month=target_month,
            group_id=effective_group_id,
        )

        return build_dashboard_monthly_close_response(
            target_year=target_year,
            target_month=target_month,
            effective_group_id=effective_group_id,
            close_doc=close_doc,
            close_scope=close_scope,
        )

    async def get_dashboard_monthly_close_calendar(
        request: Request,
        year: Optional[int] = None,
        from_current_month: bool = True,
    ):
        await get_current_user(request)

        now = datetime.now(timezone.utc)
        target_year = int(year or now.year)
        if target_year < 2020 or target_year > 2100:
            raise HTTPException(status_code=400, detail="Year must be between 2020 and 2100")

        start_month = now.month if (from_current_month and target_year == now.year) else 1
        holidays_by_month = mexico_lft_holidays_by_month(target_year)

        docs = await list_global_monthly_closes_by_year(db, year=target_year, limit=1000)
        return build_dashboard_monthly_close_calendar(
            target_year=target_year,
            start_month=start_month,
            docs=docs,
            holidays_by_month=holidays_by_month,
        )

    async def upsert_dashboard_monthly_close(payload: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") != app_admin_role:
            raise HTTPException(status_code=403, detail="Only app_admin can update monthly close values")

        target_group_id = None
        days_in_month = monthrange(int(payload.year), int(payload.month))[1]
        if payload.fiscal_close_day is not None and payload.fiscal_close_day > days_in_month:
            raise HTTPException(status_code=400, detail=f"fiscal_close_day must be <= {days_in_month}")
        industry_target_year, industry_target_month = add_months_ym(
            int(payload.year),
            int(payload.month),
            int(payload.industry_close_month_offset or 0),
        )
        industry_days_in_target_month = monthrange(industry_target_year, industry_target_month)[1]
        if payload.industry_close_day is not None and payload.industry_close_day > industry_days_in_target_month:
            raise HTTPException(
                status_code=400,
                detail=(
                    "industry_close_day must be <= "
                    f"{industry_days_in_target_month} for target month {industry_target_year}-{industry_target_month:02d}"
                ),
            )

        now = datetime.now(timezone.utc)
        updated = await upsert_global_monthly_close(
            db,
            year=int(payload.year),
            month=int(payload.month),
            fiscal_close_day=payload.fiscal_close_day,
            industry_close_day=payload.industry_close_day,
            industry_close_month_offset=int(payload.industry_close_month_offset or 0),
            updated_by=current_user.get("id"),
            now=now,
        )
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="upsert_dashboard_monthly_close",
            entity_type="dashboard_monthly_close",
            entity_id=f"{payload.year}-{payload.month:02d}:{target_group_id or 'global'}",
            group_id=target_group_id,
            details={
                "year": payload.year,
                "month": payload.month,
                "group_id": None,
                "fiscal_close_day": payload.fiscal_close_day,
                "industry_close_day": payload.industry_close_day,
                "industry_close_month_offset": int(payload.industry_close_month_offset or 0),
            },
        )

        return {
            "year": payload.year,
            "month": payload.month,
            "group_id": None,
            "fiscal_close_day": updated.get("fiscal_close_day") if updated else payload.fiscal_close_day,
            "industry_close_day": updated.get("industry_close_day") if updated else payload.industry_close_day,
            "industry_close_month_offset": int(
                updated.get("industry_close_month_offset") or (payload.industry_close_month_offset or 0)
            ) if updated else int(payload.industry_close_month_offset or 0),
            "updated_at": updated.get("updated_at") if updated else now,
        }

    async def get_dashboard_kpis(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
    ):
        current_user = await get_current_user(request)
        if current_user["role"] == user_role_seller:
            current_seller_id = current_user.get("id")
            if seller_id and seller_id != current_seller_id:
                raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
            seller_id = current_seller_id
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return empty_dashboard_kpis_response()

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id

        return await compute_dashboard_kpis(
            db,
            query=query,
            seller_id=seller_id,
            now=datetime.now(timezone.utc),
            user_role_seller=user_role_seller,
            list_vehicles=list_vehicles,
            list_sales=list_sales,
            list_vehicles_by_ids=list_vehicles_by_ids,
            list_agencies_by_brand_id=list_agencies_by_brand_id,
            list_agencies_by_group_id=list_agencies_by_group_id,
            count_users=count_users,
            count_sales=count_sales,
            enrich_vehicle=enrich_vehicle,
            calculate_vehicle_financial_cost_in_period=calculate_vehicle_financial_cost_in_period,
            sale_effective_revenue=sale_effective_revenue,
            resolve_dashboard_scope_group_id=resolve_dashboard_scope_group_id,
            find_dashboard_monthly_close=find_dashboard_monthly_close,
        )

    async def get_sales_trends(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        months: int = 6,
        granularity: str = "month",
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id

        if current_user["role"] == user_role_seller:
            current_seller_id = current_user.get("id")
            if not current_seller_id:
                return []
            if seller_id and seller_id != current_seller_id:
                raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
            query["seller_id"] = current_seller_id
        elif seller_id:
            query["seller_id"] = seller_id

        return await compute_sales_trends(
            db,
            query=query,
            now=datetime.now(timezone.utc),
            months=months,
            granularity=granularity,
            objective_approved=objective_approved,
            objective_pending=objective_pending,
            list_sales=list_sales,
            list_sales_objectives=list_sales_objectives,
            coerce_utc_datetime=coerce_utc_datetime,
            sale_effective_revenue=sale_effective_revenue,
            decrement_month=decrement_month,
            compute_operational_day_profile=compute_operational_day_profile,
            resolve_effective_objective_units=resolve_effective_objective_units,
        )

    async def get_seller_performance(
        request: Request,
        agency_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        current_user = await get_current_user(request)

        now = datetime.now(timezone.utc)
        if not month:
            month = now.month
        if not year:
            year = now.year

        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        query = {"sale_date": {"$gte": start_date, "$lt": end_date}}
        scope_query = build_scope_query(current_user)
        if not scope_query_has_access(scope_query):
            return []
        query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

        validate_scope_filters(current_user, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id

        return await compute_seller_performance(
            db,
            query=query,
            list_sales=list_sales,
            find_user_by_id=find_user_by_id,
            sale_effective_revenue=sale_effective_revenue,
        )

    async def get_vehicle_suggestions(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        limit: int = 20,
    ):
        current_user = await get_current_user(request)

        query = {"status": "in_stock"}
        scope_query = build_scope_query(current_user)
        if not scope_query_has_access(scope_query):
            return []
        query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if group_id:
            query["group_id"] = group_id
        if brand_id:
            query["brand_id"] = brand_id
        if agency_id:
            query["agency_id"] = agency_id

        return await collect_vehicle_suggestions(
            db,
            query=query,
            limit=limit,
            list_vehicles=list_vehicles,
            enrich_vehicle=enrich_vehicle,
            build_vehicle_aging_suggestion=build_vehicle_aging_suggestion,
        )

    return {
        "get_dashboard_monthly_close": get_dashboard_monthly_close,
        "get_dashboard_monthly_close_calendar": get_dashboard_monthly_close_calendar,
        "upsert_dashboard_monthly_close": upsert_dashboard_monthly_close,
        "get_dashboard_kpis": get_dashboard_kpis,
        "get_sales_trends": get_sales_trends,
        "get_seller_performance": get_seller_performance,
        "get_vehicle_suggestions": get_vehicle_suggestions,
    }
