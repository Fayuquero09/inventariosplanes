from calendar import monthrange
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple


def empty_dashboard_kpis_response() -> Dict[str, Any]:
    return {
        "total_vehicles": 0,
        "total_value": 0,
        "total_financial_cost": 0,
        "avg_aging_days": 0,
        "aging_buckets": {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0},
        "units_sold_month": 0,
        "revenue_month": 0,
        "commissions_month": 0,
        "vehicle_cost_month": 0,
        "financial_expenses_month": 0,
        "gross_profit_month": 0,
        "gross_margin_pct_month": 0,
        "new_vehicles": 0,
        "used_vehicles": 0,
        "seller_count": 0,
        "avg_units_per_seller_month": 0,
        "benchmark_avg_units_per_seller_month": 0,
        "avg_units_per_seller_vs_benchmark_pct": None,
        "seller_challenge_tier": "Sin benchmark",
        "fiscal_close_day": None,
        "industry_close_day": None,
        "industry_close_month_offset": 0,
    }


async def resolve_dashboard_scope_group_id(
    db: Any,
    *,
    scope_query: Dict[str, Any],
    find_brand_group_id: Callable[[Any, str], Awaitable[Optional[str]]],
    find_agency_group_id: Callable[[Any, str], Awaitable[Optional[str]]],
) -> Optional[str]:
    if scope_query.get("group_id"):
        return str(scope_query.get("group_id"))

    if scope_query.get("brand_id"):
        brand_group_id = await find_brand_group_id(db, str(scope_query.get("brand_id")))
        if brand_group_id:
            return brand_group_id

    if scope_query.get("agency_id"):
        agency_group_id = await find_agency_group_id(db, str(scope_query.get("agency_id")))
        if agency_group_id:
            return agency_group_id

    return None


def build_dashboard_monthly_close_response(
    *,
    target_year: int,
    target_month: int,
    effective_group_id: Optional[str],
    close_doc: Optional[Dict[str, Any]],
    close_scope: str,
) -> Dict[str, Any]:
    return {
        "year": target_year,
        "month": target_month,
        "group_id": effective_group_id,
        "scope": close_scope,
        "fiscal_close_day": close_doc.get("fiscal_close_day") if close_doc else None,
        "industry_close_day": close_doc.get("industry_close_day") if close_doc else None,
        "industry_close_month_offset": int(close_doc.get("industry_close_month_offset") or 0) if close_doc else 0,
        "updated_at": close_doc.get("updated_at") if close_doc else None,
    }


def build_dashboard_monthly_close_calendar(
    *,
    target_year: int,
    start_month: int,
    docs: List[Dict[str, Any]],
    holidays_by_month: Dict[int, List[int]],
) -> Dict[str, Any]:
    docs_by_month = {int(doc.get("month")): doc for doc in docs if doc.get("month")}

    items: List[Dict[str, Any]] = []
    for month in range(start_month, 13):
        days_in_month = monthrange(target_year, month)[1]
        month_doc = docs_by_month.get(month) or {}

        sundays: List[int] = [
            day
            for day in range(1, days_in_month + 1)
            if datetime(target_year, month, day, tzinfo=timezone.utc).weekday() == 6
        ]

        items.append(
            {
                "year": target_year,
                "month": month,
                "days_in_month": days_in_month,
                "fiscal_close_day": month_doc.get("fiscal_close_day"),
                "industry_close_day": month_doc.get("industry_close_day"),
                "industry_close_month_offset": int(month_doc.get("industry_close_month_offset") or 0),
                "holidays": holidays_by_month.get(month, []),
                "sundays": sundays,
                "updated_at": month_doc.get("updated_at"),
            }
        )

    return {
        "year": target_year,
        "start_month": start_month,
        "items": items,
    }


async def compute_dashboard_kpis(
    db: Any,
    *,
    query: Dict[str, Any],
    seller_id: Optional[str],
    now: datetime,
    user_role_seller: str,
    list_vehicles: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_sales: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_vehicles_by_ids: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_agencies_by_brand_id: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_agencies_by_group_id: Callable[..., Awaitable[List[Dict[str, Any]]]],
    count_users: Callable[..., Awaitable[int]],
    count_sales: Callable[..., Awaitable[int]],
    enrich_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    calculate_vehicle_financial_cost_in_period: Callable[[Dict[str, Any], datetime, datetime], Awaitable[float]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    resolve_dashboard_scope_group_id: Callable[[Dict[str, Any]], Awaitable[Optional[str]]],
    find_dashboard_monthly_close: Callable[..., Awaitable[Tuple[Optional[Dict[str, Any]], str]]],
) -> Dict[str, Any]:
    in_stock_query = {**query, "status": "in_stock"}
    vehicles_in_stock = await list_vehicles(db, query=in_stock_query, limit=10000)
    start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

    total_vehicles = len(vehicles_in_stock)
    total_value = sum(v.get("purchase_price", 0) for v in vehicles_in_stock)

    total_financial_cost = 0.0
    total_aging = 0
    aging_buckets = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}

    for vehicle in vehicles_in_stock:
        enriched = await enrich_vehicle(vehicle)
        total_financial_cost += await calculate_vehicle_financial_cost_in_period(vehicle, start_of_month, now)
        aging = int(enriched.get("aging_days", 0) or 0)
        total_aging += aging
        if aging <= 30:
            aging_buckets["0-30"] += 1
        elif aging <= 60:
            aging_buckets["31-60"] += 1
        elif aging <= 90:
            aging_buckets["61-90"] += 1
        else:
            aging_buckets["90+"] += 1

    avg_aging = round(total_aging / total_vehicles, 1) if total_vehicles > 0 else 0

    sales_query = {**query, "sale_date": {"$gte": start_of_month, "$lt": now}}
    if seller_id:
        sales_query["seller_id"] = seller_id
    monthly_sales = await list_sales(db, query=sales_query, limit=10000)

    units_sold_month = len(monthly_sales)
    revenue_month = sum(sale_effective_revenue(sale) for sale in monthly_sales)
    commissions_month = sum(float(sale.get("commission", 0) or 0) for sale in monthly_sales)

    vehicle_cost_month = 0.0
    financial_expenses_month = 0.0
    sold_vehicle_ids: List[str] = []
    for sale in monthly_sales:
        vehicle_id = sale.get("vehicle_id")
        if vehicle_id:
            sold_vehicle_ids.append(str(vehicle_id))

    if sold_vehicle_ids:
        unique_vehicle_ids = list(dict.fromkeys(sold_vehicle_ids))
        sold_vehicles = await list_vehicles_by_ids(db, vehicle_ids=unique_vehicle_ids, limit=10000)
        sold_vehicle_map = {str(vehicle["_id"]): vehicle for vehicle in sold_vehicles}
        for sale in monthly_sales:
            vehicle_id = str(sale.get("vehicle_id") or "")
            if not vehicle_id:
                continue
            vehicle = sold_vehicle_map.get(vehicle_id)
            if not vehicle:
                continue
            vehicle_cost_month += float(vehicle.get("purchase_price", 0) or 0)
            financial_expenses_month += float(
                await calculate_vehicle_financial_cost_in_period(vehicle, start_of_month, now) or 0.0
            )

    gross_profit_month = revenue_month - financial_expenses_month - commissions_month - vehicle_cost_month
    gross_margin_pct_month = (gross_profit_month / revenue_month * 100) if revenue_month > 0 else 0.0

    if seller_id:
        seller_count = 1
    else:
        seller_scope_query: Dict[str, Any] = {"role": user_role_seller}
        agency_scope_ids: List[str] = []
        if query.get("agency_id"):
            seller_scope_query["agency_id"] = query["agency_id"]
        elif query.get("brand_id"):
            brand_agencies = await list_agencies_by_brand_id(db, brand_id=query["brand_id"], limit=5000)
            agency_scope_ids = [str(agency["_id"]) for agency in brand_agencies]
            brand_or_filters: List[Dict[str, Any]] = [{"brand_id": query["brand_id"]}]
            if agency_scope_ids:
                brand_or_filters.append({"agency_id": {"$in": agency_scope_ids}})
            seller_scope_query["$or"] = brand_or_filters
        elif query.get("group_id"):
            group_agencies = await list_agencies_by_group_id(db, group_id=query["group_id"], limit=10000)
            agency_scope_ids = [str(agency["_id"]) for agency in group_agencies]
            group_or_filters: List[Dict[str, Any]] = [{"group_id": query["group_id"]}]
            if agency_scope_ids:
                group_or_filters.append({"agency_id": {"$in": agency_scope_ids}})
            seller_scope_query["$or"] = group_or_filters

        seller_count = await count_users(db, query=seller_scope_query)

    avg_units_per_seller_month = round((units_sold_month / seller_count), 2) if seller_count > 0 else 0.0

    previous_year = now.year - 1
    previous_start = datetime(previous_year, now.month, 1, tzinfo=timezone.utc)
    if now.month == 12:
        previous_month_end = datetime(previous_year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        previous_month_end = datetime(previous_year, now.month + 1, 1, tzinfo=timezone.utc)

    elapsed_seconds = max((now - start_of_month).total_seconds(), 0)
    previous_period_end = previous_start + timedelta(seconds=elapsed_seconds)
    if previous_period_end > previous_month_end:
        previous_period_end = previous_month_end

    previous_sales_query = {**query, "sale_date": {"$gte": previous_start, "$lt": previous_period_end}}
    if seller_id:
        previous_sales_query["seller_id"] = seller_id
    previous_units_sold_month = await count_sales(db, query=previous_sales_query)

    benchmark_avg_units_per_seller_month = (
        round((previous_units_sold_month / seller_count), 2) if seller_count > 0 else 0.0
    )
    avg_units_per_seller_vs_benchmark_pct = None
    if benchmark_avg_units_per_seller_month > 0:
        avg_units_per_seller_vs_benchmark_pct = round(
            ((avg_units_per_seller_month - benchmark_avg_units_per_seller_month) / benchmark_avg_units_per_seller_month) * 100,
            1,
        )

    if benchmark_avg_units_per_seller_month <= 0:
        seller_challenge_tier = "Sin benchmark"
    else:
        benchmark_ratio = avg_units_per_seller_month / benchmark_avg_units_per_seller_month
        if benchmark_ratio >= 1.2:
            seller_challenge_tier = "Oro"
        elif benchmark_ratio >= 1.0:
            seller_challenge_tier = "Plata"
        elif benchmark_ratio >= 0.8:
            seller_challenge_tier = "Bronce"
        else:
            seller_challenge_tier = "Impulso"

    dashboard_scope_group_id = await resolve_dashboard_scope_group_id(query)
    close_doc, _ = await find_dashboard_monthly_close(
        year=now.year,
        month=now.month,
        group_id=dashboard_scope_group_id,
    )
    fiscal_close_day = close_doc.get("fiscal_close_day") if close_doc else None
    industry_close_day = close_doc.get("industry_close_day") if close_doc else None
    industry_close_month_offset = int(close_doc.get("industry_close_month_offset") or 0) if close_doc else 0

    return {
        "total_vehicles": total_vehicles,
        "total_value": round(total_value, 2),
        "total_financial_cost": round(total_financial_cost, 2),
        "avg_aging_days": avg_aging,
        "aging_buckets": aging_buckets,
        "units_sold_month": units_sold_month,
        "revenue_month": round(revenue_month, 2),
        "commissions_month": round(commissions_month, 2),
        "vehicle_cost_month": round(vehicle_cost_month, 2),
        "financial_expenses_month": round(financial_expenses_month, 2),
        "gross_profit_month": round(gross_profit_month, 2),
        "gross_margin_pct_month": round(gross_margin_pct_month, 2),
        "new_vehicles": len([vehicle for vehicle in vehicles_in_stock if vehicle.get("vehicle_type") == "new"]),
        "used_vehicles": len([vehicle for vehicle in vehicles_in_stock if vehicle.get("vehicle_type") == "used"]),
        "seller_count": int(seller_count),
        "avg_units_per_seller_month": avg_units_per_seller_month,
        "benchmark_avg_units_per_seller_month": benchmark_avg_units_per_seller_month,
        "avg_units_per_seller_vs_benchmark_pct": avg_units_per_seller_vs_benchmark_pct,
        "seller_challenge_tier": seller_challenge_tier,
        "fiscal_close_day": fiscal_close_day,
        "industry_close_day": industry_close_day,
        "industry_close_month_offset": industry_close_month_offset,
    }


async def compute_seller_performance(
    db: Any,
    *,
    query: Dict[str, Any],
    list_sales: Callable[..., Awaitable[List[Dict[str, Any]]]],
    find_user_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
) -> List[Dict[str, Any]]:
    sales = await list_sales(db, query=query, limit=10000)
    seller_stats: Dict[str, Dict[str, float]] = {}
    for sale in sales:
        seller_id = str(sale.get("seller_id") or "")
        if not seller_id:
            continue
        if seller_id not in seller_stats:
            seller_stats[seller_id] = {"units": 0, "revenue": 0.0, "commission": 0.0}
        seller_stats[seller_id]["units"] += 1
        seller_stats[seller_id]["revenue"] += sale_effective_revenue(sale)
        seller_stats[seller_id]["commission"] += float(sale.get("commission", 0) or 0)

    result: List[Dict[str, Any]] = []
    for seller_id, stats in seller_stats.items():
        seller = await find_user_by_id(db, seller_id)
        result.append(
            {
                "seller_id": seller_id,
                "seller_name": seller["name"] if seller else "Unknown",
                "units": int(stats["units"]),
                "revenue": round(float(stats["revenue"]), 2),
                "commission": round(float(stats["commission"]), 2),
            }
        )

    return sorted(result, key=lambda item: item["units"], reverse=True)


async def build_vehicle_aging_suggestion(
    db: Any,
    *,
    vehicle: Dict[str, Any],
    enriched_vehicle: Optional[Dict[str, Any]],
    list_similar_sold_vehicles: Callable[..., Awaitable[List[Dict[str, Any]]]],
    to_non_negative_float: Callable[[Any, float], float],
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    enriched = enriched_vehicle or {}
    aging = int(enriched.get("aging_days", 0) or 0)
    now_dt = now or datetime.now(timezone.utc)

    similar_sold = await list_similar_sold_vehicles(
        db,
        model=vehicle.get("model"),
        trim=vehicle.get("trim"),
        color=vehicle.get("color"),
        group_id=vehicle.get("group_id"),
        limit=100,
    )

    if similar_sold:
        avg_days = sum(
            (candidate.get("exit_date", now_dt) - candidate.get("entry_date", now_dt)).days
            if isinstance(candidate.get("exit_date"), datetime) and isinstance(candidate.get("entry_date"), datetime)
            else 60
            for candidate in similar_sold
        ) / len(similar_sold)
    else:
        avg_days = 60

    if aging <= avg_days:
        return None

    extra_aging_days = float(aging) - float(avg_days)
    purchase_price = to_non_negative_float(vehicle.get("purchase_price"), 0.0)
    if purchase_price <= 0:
        return None

    projected_additional_cost = extra_aging_days * (purchase_price * 0.12 / 365)
    suggested_bonus = min(projected_additional_cost * 0.5, purchase_price * 0.02)
    if suggested_bonus <= 0:
        return None

    return {
        "vehicle_id": enriched.get("id"),
        "vehicle_info": {
            "model": vehicle.get("model"),
            "year": vehicle.get("year"),
            "trim": vehicle.get("trim"),
            "color": vehicle.get("color"),
            "vin": vehicle.get("vin"),
            "purchase_price": purchase_price,
        },
        "avg_days_to_sell": round(avg_days),
        "current_aging": aging,
        "financial_cost": to_non_negative_float(enriched.get("financial_cost"), 0.0),
        "suggested_bonus": round(suggested_bonus, 2),
        "reason": (
            f"Este vehículo lleva {aging} días en inventario. "
            f"Vehículos similares se venden en promedio en {round(avg_days)} días."
        ),
    }


async def collect_vehicle_suggestions(
    db: Any,
    *,
    query: Dict[str, Any],
    limit: int,
    list_vehicles: Callable[..., Awaitable[List[Dict[str, Any]]]],
    enrich_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    build_vehicle_aging_suggestion: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
) -> List[Dict[str, Any]]:
    vehicles = await list_vehicles(db, query=query, limit=1000)
    suggestions: List[Dict[str, Any]] = []
    for vehicle in vehicles:
        enriched = await enrich_vehicle(vehicle)
        suggestion = await build_vehicle_aging_suggestion(vehicle=vehicle, enriched_vehicle=enriched)
        if suggestion:
            suggestions.append(suggestion)

    safe_limit = max(1, min(int(limit or 20), 1000))
    return sorted(suggestions, key=lambda item: item["current_aging"], reverse=True)[:safe_limit]
