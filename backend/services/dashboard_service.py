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


async def compute_sales_trends(
    db: Any,
    *,
    query: Dict[str, Any],
    now: datetime,
    months: int,
    granularity: str,
    objective_approved: str,
    objective_pending: str,
    list_sales: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_sales_objectives: Callable[..., Awaitable[List[Dict[str, Any]]]],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    decrement_month: Callable[[int, int], Tuple[int, int]],
    compute_operational_day_profile: Callable[[int, int], Dict[str, Any]],
    resolve_effective_objective_units: Callable[..., Tuple[float, str]],
) -> List[Dict[str, Any]]:
    async def _compute_history_cumulative_profile(
        scope_query: Dict[str, Any],
        target_year: int,
        target_month: int,
        target_days: int,
        lookback_months: int = 18,
    ) -> Dict[str, Any]:
        cumulative_weighted_sum: Dict[int, float] = {day: 0.0 for day in range(1, target_days + 1)}
        cumulative_weight: Dict[int, float] = {day: 0.0 for day in range(1, target_days + 1)}

        months_used = 0
        total_history_units = 0
        cursor_year, cursor_month = decrement_month(target_year, target_month)

        for _ in range(max(1, lookback_months)):
            month_start = datetime(cursor_year, cursor_month, 1, tzinfo=timezone.utc)
            if cursor_month == 12:
                month_end = datetime(cursor_year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                month_end = datetime(cursor_year, cursor_month + 1, 1, tzinfo=timezone.utc)

            monthly_query = {**scope_query, "sale_date": {"$gte": month_start, "$lt": month_end}}
            month_sales = await list_sales(db, query=monthly_query, limit=25000)
            if not month_sales:
                cursor_year, cursor_month = decrement_month(cursor_year, cursor_month)
                continue

            units_by_day: Dict[int, int] = {}
            valid_units = 0
            for sale in month_sales:
                sale_date = coerce_utc_datetime(sale.get("sale_date"))
                if not sale_date or sale_date < month_start or sale_date >= month_end:
                    continue
                day = sale_date.day
                units_by_day[day] = units_by_day.get(day, 0) + 1
                valid_units += 1

            if valid_units <= 0:
                cursor_year, cursor_month = decrement_month(cursor_year, cursor_month)
                continue

            hist_days_in_month = (month_end - month_start).days or 1
            running_units = 0
            cumulative_share_for_month: Dict[int, float] = {}
            for day in range(1, hist_days_in_month + 1):
                running_units += units_by_day.get(day, 0)
                cumulative_share_for_month[day] = min(1.0, running_units / valid_units)

            for target_day in range(1, target_days + 1):
                source_day = min(target_day, hist_days_in_month)
                share = cumulative_share_for_month.get(source_day, 1.0)
                cumulative_weighted_sum[target_day] += share * valid_units
                cumulative_weight[target_day] += valid_units

            months_used += 1
            total_history_units += valid_units
            cursor_year, cursor_month = decrement_month(cursor_year, cursor_month)

        if months_used == 0:
            return {
                "available": False,
                "months_used": 0,
                "history_total_units": 0,
                "cumulative_shares": {},
            }

        cumulative_shares: Dict[int, float] = {}
        previous_share = 0.0
        for day in range(1, target_days + 1):
            if cumulative_weight[day] > 0:
                raw_share = cumulative_weighted_sum[day] / cumulative_weight[day]
            else:
                raw_share = previous_share
            normalized_share = max(previous_share, min(1.0, raw_share))
            cumulative_shares[day] = round(normalized_share, 6)
            previous_share = normalized_share

        cumulative_shares[target_days] = 1.0
        return {
            "available": True,
            "months_used": months_used,
            "history_total_units": total_history_units,
            "cumulative_shares": cumulative_shares,
        }

    safe_months = max(1, min(int(months), 24))
    granularity_mode = (granularity or "month").strip().lower()
    if granularity_mode not in {"month", "day", "daily"}:
        granularity_mode = "month"
    trends: List[Dict[str, Any]] = []

    if granularity_mode in {"day", "daily"} and safe_months == 1:
        year = now.year
        month = now.month
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        sales_query = {**query, "sale_date": {"$gte": start_date, "$lt": now}}
        sales = await list_sales(db, query=sales_query, limit=20000)

        prev_year = year - 1
        prev_start_date = datetime(prev_year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            prev_month_end = datetime(prev_year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            prev_month_end = datetime(prev_year, month + 1, 1, tzinfo=timezone.utc)
        elapsed_seconds = max((now - start_date).total_seconds(), 0)
        prev_period_end = prev_start_date + timedelta(seconds=elapsed_seconds)
        if prev_period_end > prev_month_end:
            prev_period_end = prev_month_end

        previous_year_sales_query = {**query, "sale_date": {"$gte": prev_start_date, "$lt": prev_period_end}}
        previous_year_sales = await list_sales(db, query=previous_year_sales_query, limit=20000)

        objective_scope_query: Dict[str, Any] = {"month": month, "year": year}
        for scope_key in ("group_id", "brand_id", "agency_id"):
            if query.get(scope_key):
                objective_scope_query[scope_key] = query[scope_key]

        role_scoped_to_seller = "seller_id" in query
        if role_scoped_to_seller:
            objective_scope_query["seller_id"] = query["seller_id"]

        objectives = await list_sales_objectives(db, query=objective_scope_query, limit=5000)
        approved_objectives = [
            objective
            for objective in objectives
            if str(objective.get("approval_status") or objective_approved).strip().lower()
            in {objective_approved, objective_pending}
        ]

        if role_scoped_to_seller:
            objective_units = sum(int(objective.get("units_target", 0) or 0) for objective in approved_objectives)
        else:
            objective_units = sum(
                int(objective.get("units_target", 0) or 0)
                for objective in approved_objectives
                if not objective.get("seller_id")
            )

        units_by_day: Dict[int, int] = {}
        revenue_by_day: Dict[int, float] = {}
        commission_by_day: Dict[int, float] = {}
        for sale in sales:
            sale_date = coerce_utc_datetime(sale.get("sale_date"))
            if not sale_date or sale_date < start_date or sale_date >= now:
                continue
            day = sale_date.day
            units_by_day[day] = units_by_day.get(day, 0) + 1
            revenue_by_day[day] = revenue_by_day.get(day, 0.0) + sale_effective_revenue(sale)
            commission_by_day[day] = commission_by_day.get(day, 0.0) + float(sale.get("commission", 0) or 0)

        last_year_units_by_day: Dict[int, int] = {}
        for sale in previous_year_sales:
            sale_date = coerce_utc_datetime(sale.get("sale_date"))
            if not sale_date or sale_date < prev_start_date or sale_date >= prev_period_end:
                continue
            day = sale_date.day
            last_year_units_by_day[day] = last_year_units_by_day.get(day, 0) + 1

        days_in_month = (month_end - start_date).days or 1
        elapsed_days = max(min(now.day, days_in_month), 1)
        actual_units_to_date = sum(units_by_day.values())
        objective_units_effective, objective_source = resolve_effective_objective_units(
            configured_units=float(objective_units or 0),
            previous_year_units_observed=len(previous_year_sales),
            days_in_month=days_in_month,
            elapsed_days=elapsed_days,
        )

        operational_profile = compute_operational_day_profile(year, month)
        history_profile = await _compute_history_cumulative_profile(
            scope_query=query,
            target_year=year,
            target_month=month,
            target_days=days_in_month,
            lookback_months=18,
        )

        if history_profile.get("available"):
            blended_cumulative_shares: Dict[int, float] = {}
            prev_blended = 0.0
            history_shares = history_profile.get("cumulative_shares", {})
            operational_shares = operational_profile.get("cumulative_shares", {})
            for day in range(1, days_in_month + 1):
                history_share = float(history_shares.get(day, 0.0))
                operational_share = float(operational_shares.get(day, day / days_in_month))
                raw_share = (history_share * 0.8) + (operational_share * 0.2)
                normalized_share = max(prev_blended, min(1.0, raw_share))
                blended_cumulative_shares[day] = round(normalized_share, 6)
                prev_blended = normalized_share

            blended_cumulative_shares[days_in_month] = 1.0
            projection_cumulative_shares = blended_cumulative_shares
            projection_profile_source = "history_blended_operational"
        else:
            projection_cumulative_shares = operational_profile.get("cumulative_shares", {})
            projection_profile_source = "operational_days_only"

        elapsed_share = float(projection_cumulative_shares.get(elapsed_days, 0.0) or 0.0)
        if elapsed_share <= 0:
            elapsed_share = max(elapsed_days / days_in_month, 1.0 / days_in_month)
        projected_month_units = round(actual_units_to_date / elapsed_share, 2)

        cumulative_units = 0
        cumulative_last_year = 0
        cumulative_revenue = 0.0
        cumulative_commission = 0.0
        for day in range(1, days_in_month + 1):
            is_elapsed_day = day <= elapsed_days
            if is_elapsed_day:
                cumulative_units += units_by_day.get(day, 0)
                cumulative_last_year += last_year_units_by_day.get(day, 0)
                cumulative_revenue += revenue_by_day.get(day, 0.0)
                cumulative_commission += commission_by_day.get(day, 0.0)

            cumulative_share = float(projection_cumulative_shares.get(day, day / days_in_month))
            previous_share = float(projection_cumulative_shares.get(day - 1, 0.0)) if day > 1 else 0.0
            daily_share = max(0.0, cumulative_share - previous_share)

            weighted_objective_units = round(objective_units_effective * cumulative_share, 2)
            daily_objective_units = round(objective_units_effective * daily_share, 2)
            forecast_units = round(projected_month_units * cumulative_share, 2)

            trends.append(
                {
                    "month": f"{year}-{month:02d}",
                    "date": f"{year}-{month:02d}-{day:02d}",
                    "day_of_month": day,
                    "day_label": f"{day:02d}",
                    "is_elapsed_day": is_elapsed_day,
                    "units": cumulative_units if is_elapsed_day else None,
                    "last_year_units": cumulative_last_year if is_elapsed_day else None,
                    "objective_units": objective_units_effective,
                    "configured_objective_units": float(objective_units or 0),
                    "objective_source": objective_source,
                    "weighted_objective_units": weighted_objective_units,
                    "daily_objective_units": daily_objective_units,
                    "revenue": round(cumulative_revenue, 2) if is_elapsed_day else None,
                    "commission": round(cumulative_commission, 2) if is_elapsed_day else None,
                    "forecast_units": forecast_units,
                    "operational_day_weight": float(operational_profile.get("day_weights", {}).get(day, 1.0)),
                    "operational_cumulative_share": float(
                        operational_profile.get("cumulative_shares", {}).get(day, day / days_in_month)
                    ),
                    "projection_cumulative_share": cumulative_share,
                    "projection_profile_source": projection_profile_source,
                    "projection_months_used": int(history_profile.get("months_used", 0) or 0),
                }
            )

        return trends

    cursor = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    month_window: List[Tuple[int, int]] = []
    for _ in range(safe_months):
        month_window.append((cursor.year, cursor.month))
        if cursor.month == 1:
            cursor = datetime(cursor.year - 1, 12, 1, tzinfo=timezone.utc)
        else:
            cursor = datetime(cursor.year, cursor.month - 1, 1, tzinfo=timezone.utc)
    month_window.reverse()

    for year, month in month_window:
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        current_period_end = now if (year == now.year and month == now.month) else end_date

        sales_query = {**query, "sale_date": {"$gte": start_date, "$lt": current_period_end}}
        sales = await list_sales(db, query=sales_query, limit=10000)

        prev_year = year - 1
        prev_start_date = datetime(prev_year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            prev_end_date = datetime(prev_year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            prev_end_date = datetime(prev_year, month + 1, 1, tzinfo=timezone.utc)
        if year == now.year and month == now.month:
            elapsed_seconds = max((current_period_end - start_date).total_seconds(), 0)
            prev_period_end = prev_start_date + timedelta(seconds=elapsed_seconds)
            if prev_period_end > prev_end_date:
                prev_period_end = prev_end_date
        else:
            prev_period_end = prev_end_date

        previous_year_sales_query = {**query, "sale_date": {"$gte": prev_start_date, "$lt": prev_period_end}}
        previous_year_sales = await list_sales(db, query=previous_year_sales_query, limit=10000)

        objective_scope_query: Dict[str, Any] = {"month": month, "year": year}
        for scope_key in ("group_id", "brand_id", "agency_id"):
            if query.get(scope_key):
                objective_scope_query[scope_key] = query[scope_key]

        role_scoped_to_seller = "seller_id" in query
        if role_scoped_to_seller:
            objective_scope_query["seller_id"] = query["seller_id"]

        objectives = await list_sales_objectives(db, query=objective_scope_query, limit=5000)
        approved_objectives = [
            objective
            for objective in objectives
            if str(objective.get("approval_status") or objective_approved).strip().lower()
            in {objective_approved, objective_pending}
        ]

        if role_scoped_to_seller:
            objective_units = sum(int(objective.get("units_target", 0) or 0) for objective in approved_objectives)
        else:
            objective_units = sum(
                int(objective.get("units_target", 0) or 0)
                for objective in approved_objectives
                if not objective.get("seller_id")
            )

        days_in_month = (end_date - start_date).days or 1
        elapsed_days_for_objective = now.day if (year == now.year and month == now.month) else None
        objective_units_effective, objective_source = resolve_effective_objective_units(
            configured_units=float(objective_units or 0),
            previous_year_units_observed=len(previous_year_sales),
            days_in_month=days_in_month,
            elapsed_days=elapsed_days_for_objective,
        )
        if year == now.year and month == now.month:
            weighted_objective_units = round(objective_units_effective * (now.day / days_in_month), 2)
        else:
            weighted_objective_units = float(objective_units_effective)

        trends.append(
            {
                "month": f"{year}-{month:02d}",
                "units": len(sales),
                "last_year_units": len(previous_year_sales),
                "objective_units": objective_units_effective,
                "configured_objective_units": float(objective_units or 0),
                "objective_source": objective_source,
                "weighted_objective_units": weighted_objective_units,
                "revenue": round(sum(sale_effective_revenue(sale) for sale in sales), 2),
                "commission": round(sum(float(sale.get("commission", 0) or 0) for sale in sales), 2),
            }
        )

    if trends:
        points = len(trends)
        units_series = [float(point.get("units", 0)) for point in trends]
        if points == 1:
            forecast_series = units_series
        else:
            sum_x = sum(range(points))
            sum_y = sum(units_series)
            sum_xx = sum(idx * idx for idx in range(points))
            sum_xy = sum(idx * units_series[idx] for idx in range(points))
            denominator = points * sum_xx - (sum_x * sum_x)
            slope = ((points * sum_xy - sum_x * sum_y) / denominator) if denominator else 0.0
            intercept = (sum_y - slope * sum_x) / points if points else 0.0
            forecast_series = [max(0.0, round(intercept + slope * idx, 2)) for idx in range(points)]

        for idx, point in enumerate(trends):
            point["forecast_units"] = forecast_series[idx]

    return trends


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
