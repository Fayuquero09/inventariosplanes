import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple


async def list_sales_objectives_with_progress(
    db: Any,
    *,
    objectives_query: Dict[str, Any],
    objective_approved: str,
    objective_pending: str,
    serialize_doc: Callable[[Dict[str, Any]], Dict[str, Any]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    list_sales_objectives: Callable[..., Awaitable[List[Dict[str, Any]]]],
    find_user_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_group_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    list_sales: Callable[..., Awaitable[List[Dict[str, Any]]]],
) -> List[Dict[str, Any]]:
    objectives = await list_sales_objectives(db, query=objectives_query, limit=1000)

    user_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    agency_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    brand_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    group_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    result: List[Dict[str, Any]] = []

    for objective in objectives:
        serialized = serialize_doc(objective)
        if not serialized.get("approval_status"):
            serialized["approval_status"] = objective_approved

        seller_id = str(objective.get("seller_id") or "").strip()
        agency_id = str(objective.get("agency_id") or "").strip()
        brand_id = str(objective.get("brand_id") or "").strip()
        group_id = str(objective.get("group_id") or "").strip()

        if seller_id:
            if seller_id not in user_cache:
                user_cache[seller_id] = await find_user_by_id(db, seller_id)
            seller = user_cache[seller_id]
            if seller:
                serialized["seller_name"] = seller.get("name")

        if agency_id:
            if agency_id not in agency_cache:
                agency_cache[agency_id] = await find_agency_by_id(db, agency_id)
            agency = agency_cache[agency_id]
            if agency:
                serialized["agency_name"] = agency.get("name")

        if brand_id:
            if brand_id not in brand_cache:
                brand_cache[brand_id] = await find_brand_by_id(db, brand_id)
            brand = brand_cache[brand_id]
            if brand:
                serialized["brand_name"] = brand.get("name")

        if group_id:
            if group_id not in group_cache:
                group_cache[group_id] = await find_group_by_id(db, group_id)
            group = group_cache[group_id]
            if group:
                serialized["group_name"] = group.get("name")

        for actor_field, actor_name_field in (
            ("created_by", "created_by_name"),
            ("approved_by", "approved_by_name"),
            ("rejected_by", "rejected_by_name"),
        ):
            actor_id = str(objective.get(actor_field) or "").strip()
            if not actor_id:
                continue
            if actor_id not in user_cache:
                user_cache[actor_id] = await find_user_by_id(db, actor_id)
            actor = user_cache[actor_id]
            if actor:
                serialized[actor_name_field] = actor.get("name")

        start_date = datetime(int(objective["year"]), int(objective["month"]), 1, tzinfo=timezone.utc)
        if int(objective["month"]) == 12:
            end_date = datetime(int(objective["year"]) + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(int(objective["year"]), int(objective["month"]) + 1, 1, tzinfo=timezone.utc)

        sales_query: Dict[str, Any] = {"sale_date": {"$gte": start_date, "$lt": end_date}}
        if objective.get("seller_id"):
            sales_query["seller_id"] = objective["seller_id"]
        elif objective.get("agency_id"):
            sales_query["agency_id"] = objective["agency_id"]
        model_scope = str(objective.get("vehicle_line") or "").strip()
        if model_scope:
            sales_query["model"] = {"$regex": f"^{re.escape(model_scope)}$", "$options": "i"}

        sales = await list_sales(db, query=sales_query, limit=1000)
        units_sold = len(sales)
        revenue_achieved = sum(sale_effective_revenue(sale) for sale in sales)
        commissions_achieved = sum(float(sale.get("commission", 0) or 0) for sale in sales)

        units_target = float(objective.get("units_target", 0) or 0)
        revenue_target = float(objective.get("revenue_target", 0) or 0)
        serialized["units_sold"] = units_sold
        serialized["revenue_achieved"] = revenue_achieved
        serialized["commissions_achieved"] = commissions_achieved
        serialized["progress_units"] = round((units_sold / units_target * 100) if units_target > 0 else 0, 1)
        serialized["progress_revenue"] = round((revenue_achieved / revenue_target * 100) if revenue_target > 0 else 0, 1)

        result.append(serialized)

    return result


async def build_sales_objective_suggestion(
    db: Any,
    *,
    agency_id: str,
    seller_id: str,
    target_month: int,
    target_year: int,
    safe_lookback: int,
    agency: Dict[str, Any],
    seller: Dict[str, Any],
    add_months_ym: Callable[[int, int, int], Tuple[int, int]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    to_non_negative_float: Callable[[Any, float], float],
    is_price_bulletin_active: Callable[[Dict[str, Any], str], bool],
    build_catalog_tree_from_source: Callable[..., Dict[str, Any]],
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_catalog_price: Callable[[Any], float],
    list_sales: Callable[..., Awaitable[List[Dict[str, Any]]]],
    list_price_bulletins: Callable[..., Awaitable[List[Dict[str, Any]]]],
    find_brand_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
) -> Dict[str, Any]:
    def _month_bounds(year_value: int, month_value: int) -> Tuple[datetime, datetime]:
        start = datetime(year_value, month_value, 1, tzinfo=timezone.utc)
        if month_value == 12:
            end = datetime(year_value + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(year_value, month_value + 1, 1, tzinfo=timezone.utc)
        return start, end

    query_base = {"agency_id": agency_id, "seller_id": seller_id}

    prev_start, prev_end = _month_bounds(target_year - 1, target_month)
    previous_year_sales = await list_sales(
        db,
        query={**query_base, "sale_date": {"$gte": prev_start, "$lt": prev_end}},
        limit=20000,
    )

    month_totals_recent: List[int] = []
    model_recent_totals: Dict[str, int] = {}
    model_previous_year_totals: Dict[str, int] = {}
    model_price_totals: Dict[str, float] = {}
    model_price_counts: Dict[str, int] = {}

    for sale in previous_year_sales:
        model_name = str(sale.get("model") or "").strip()
        if model_name:
            model_previous_year_totals[model_name] = model_previous_year_totals.get(model_name, 0) + 1
        sale_price = sale_effective_revenue(sale)
        if model_name and sale_price > 0:
            model_price_totals[model_name] = model_price_totals.get(model_name, 0.0) + sale_price
            model_price_counts[model_name] = model_price_counts.get(model_name, 0) + 1

    for offset in range(-safe_lookback, 0):
        cursor_year, cursor_month = add_months_ym(target_year, target_month, offset)
        recent_start, recent_end = _month_bounds(cursor_year, cursor_month)
        sales_in_month = await list_sales(
            db,
            query={**query_base, "sale_date": {"$gte": recent_start, "$lt": recent_end}},
            limit=20000,
        )
        month_totals_recent.append(len(sales_in_month))
        for sale in sales_in_month:
            model_name = str(sale.get("model") or "").strip()
            if model_name:
                model_recent_totals[model_name] = model_recent_totals.get(model_name, 0) + 1
            sale_price = sale_effective_revenue(sale)
            if model_name and sale_price > 0:
                model_price_totals[model_name] = model_price_totals.get(model_name, 0.0) + sale_price
                model_price_counts[model_name] = model_price_counts.get(model_name, 0) + 1

    previous_year_units = len(previous_year_sales)
    recent_avg_units = (sum(month_totals_recent) / safe_lookback) if safe_lookback > 0 else 0.0
    suggested_total_units = int(round((previous_year_units * 0.6) + (recent_avg_units * 0.4)))
    if suggested_total_units <= 0:
        blended_hist = previous_year_units + sum(month_totals_recent)
        suggested_total_units = int(round(blended_hist / max(1, safe_lookback + 1))) if blended_hist > 0 else 0

    brand_name = str(agency.get("brand_name") or "").strip()
    if not brand_name and agency.get("brand_id"):
        brand_doc = await find_brand_by_id(db, str(agency.get("brand_id")))
        if brand_doc:
            brand_name = str(brand_doc.get("name") or "").strip()

    catalog_min_msrp_by_model: Dict[str, float] = {}
    if brand_name:
        try:
            catalog = build_catalog_tree_from_source(all_years=True)
            make_entry = find_catalog_make(catalog, brand_name)
            if make_entry:
                for model_entry in make_entry.get("models", []):
                    model_name = str(model_entry.get("name") or "").strip()
                    min_msrp = parse_catalog_price(model_entry.get("min_msrp"))
                    if model_name and min_msrp:
                        catalog_min_msrp_by_model[model_name.casefold()] = float(min_msrp)
        except Exception:
            pass

    bulletin_price_by_model: Dict[str, float] = {}
    agency_group_id = str(agency.get("group_id") or "").strip()
    agency_brand_id = str(agency.get("brand_id") or "").strip()
    reference_date_ymd = f"{target_year:04d}-{target_month:02d}-01"
    if agency_group_id and agency_brand_id:
        bulletin_docs = await list_price_bulletins(
            db,
            query={
                "group_id": agency_group_id,
                "brand_id": agency_brand_id,
                "$or": [{"agency_id": agency_id}, {"agency_id": None}],
            },
            limit=3000,
        )
        for doc in bulletin_docs:
            if not is_price_bulletin_active(doc, reference_date_ymd):
                continue
            model_name = str(doc.get("model") or "").strip()
            if not model_name:
                continue
            key = model_name.casefold()
            is_agency_specific = str(doc.get("agency_id") or "") == agency_id
            existing_price = bulletin_price_by_model.get(key)
            if existing_price is not None and not is_agency_specific:
                continue

            transaction_price = to_non_negative_float(doc.get("transaction_price"), 0.0)
            msrp_price = to_non_negative_float(doc.get("msrp"), 0.0)
            effective_price = transaction_price if transaction_price > 0 else msrp_price
            if effective_price <= 0:
                continue
            bulletin_price_by_model[key] = effective_price

    model_keys = set(model_previous_year_totals.keys()) | set(model_recent_totals.keys())
    model_scores: List[Dict[str, Any]] = []
    for model_name in model_keys:
        previous_units_model = int(model_previous_year_totals.get(model_name, 0) or 0)
        recent_avg_model = float(model_recent_totals.get(model_name, 0) or 0) / safe_lookback
        blended_score = (previous_units_model * 0.65) + (recent_avg_model * 0.35)
        if blended_score <= 0:
            continue
        model_scores.append(
            {
                "model": model_name,
                "score": blended_score,
                "previous_year_units": previous_units_model,
                "recent_avg_units": round(recent_avg_model, 2),
            }
        )

    score_total = sum(float(item["score"]) for item in model_scores)
    if suggested_total_units <= 0 and score_total > 0:
        suggested_total_units = int(round(score_total))

    raw_allocations: List[Dict[str, Any]] = []
    for item in model_scores:
        raw_units = ((float(item["score"]) / score_total) * suggested_total_units) if score_total > 0 and suggested_total_units > 0 else float(item["score"])
        floor_units = int(raw_units)
        raw_allocations.append({**item, "raw_units": raw_units, "units": floor_units, "fraction": raw_units - floor_units})

    target_total_units = max(0, suggested_total_units)
    current_total_units = sum(int(item["units"]) for item in raw_allocations)
    if target_total_units > current_total_units:
        pending = target_total_units - current_total_units
        ranked = sorted(raw_allocations, key=lambda item: (item["fraction"], item["score"]), reverse=True)
        if ranked:
            idx = 0
            while pending > 0:
                ranked[idx % len(ranked)]["units"] += 1
                pending -= 1
                idx += 1
    elif current_total_units > target_total_units:
        overflow = current_total_units - target_total_units
        ranked = sorted(raw_allocations, key=lambda item: (item["units"], item["fraction"]), reverse=True)
        for item in ranked:
            if overflow <= 0:
                break
            removable = min(item["units"], overflow)
            item["units"] -= removable
            overflow -= removable

    suggestion_items: List[Dict[str, Any]] = []
    suggestion_total_revenue = 0.0
    for item in sorted(raw_allocations, key=lambda candidate: (candidate["units"], candidate["score"]), reverse=True):
        units = int(item["units"])
        if units <= 0:
            continue
        model_name = str(item["model"])
        avg_sale_price = (
            (model_price_totals.get(model_name, 0.0) / model_price_counts.get(model_name, 1))
            if model_price_counts.get(model_name, 0) > 0
            else 0.0
        )
        bulletin_price = float(bulletin_price_by_model.get(model_name.casefold(), 0.0) or 0.0)
        catalog_min_msrp = float(catalog_min_msrp_by_model.get(model_name.casefold(), 0.0) or 0.0)
        effective_price = avg_sale_price if avg_sale_price > 0 else (bulletin_price if bulletin_price > 0 else catalog_min_msrp)
        suggested_revenue = round(units * effective_price, 2) if effective_price > 0 else 0.0
        suggestion_total_revenue += suggested_revenue
        suggestion_items.append(
            {
                "model": model_name,
                "suggested_units": units,
                "suggested_revenue": suggested_revenue,
                "previous_year_units": int(item["previous_year_units"]),
                "recent_avg_units": float(item["recent_avg_units"]),
                "avg_sale_price": round(avg_sale_price, 2) if avg_sale_price > 0 else None,
                "min_msrp": round(catalog_min_msrp, 2) if catalog_min_msrp > 0 else None,
            }
        )

    return {
        "agency_id": agency_id,
        "agency_name": agency.get("name"),
        "seller_id": seller_id,
        "seller_name": seller.get("name"),
        "month": target_month,
        "year": target_year,
        "lookback_months": safe_lookback,
        "baseline": {
            "previous_year_same_month_units": int(previous_year_units),
            "recent_avg_units": round(recent_avg_units, 2),
            "suggested_total_units": int(sum(item["suggested_units"] for item in suggestion_items)),
        },
        "totals": {
            "suggested_units": int(sum(item["suggested_units"] for item in suggestion_items)),
            "suggested_revenue": round(suggestion_total_revenue, 2),
        },
        "items": suggestion_items,
    }
