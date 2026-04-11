from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from bson import ObjectId
from fastapi import HTTPException

from repositories.pricing_repository import (
    delete_price_bulletin_by_id,
    find_agency_by_id,
    find_brand_by_id,
    find_group_by_id,
    find_price_bulletin_by_id,
    list_price_bulletins,
    list_sales_for_repricing,
    list_vehicles_by_ids,
    update_sale_fields,
    upsert_price_bulletin,
)


def normalize_iso_date_string(value: Optional[str], *, field_name: str, required: bool = False) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail=f"{field_name} is required")
        return None
    if len(raw) == 10:
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d")
            return parsed.date().isoformat()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid {field_name}. Use YYYY-MM-DD")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}. Use YYYY-MM-DD")


async def resolve_price_bulletin_scope(
    db: Any,
    *,
    current_user: Dict[str, Any],
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    validate_scope_filters: Callable[..., None],
) -> Dict[str, Optional[str]]:
    normalized_group_id = str(group_id or "").strip()
    normalized_brand_id = str(brand_id or "").strip()
    normalized_agency_id = str(agency_id or "").strip() or None

    if not normalized_group_id:
        raise HTTPException(status_code=400, detail="group_id is required")
    if not normalized_brand_id:
        raise HTTPException(status_code=400, detail="brand_id is required")
    if not ObjectId.is_valid(normalized_group_id):
        raise HTTPException(status_code=400, detail="Invalid group_id")
    if not ObjectId.is_valid(normalized_brand_id):
        raise HTTPException(status_code=400, detail="Invalid brand_id")

    group = await find_group_by_id(db, normalized_group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    brand = await find_brand_by_id(db, normalized_brand_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")
    if str(brand.get("group_id") or "") != normalized_group_id:
        raise HTTPException(status_code=400, detail="Brand does not belong to selected group")

    validate_scope_filters(
        current_user,
        group_id=normalized_group_id,
        brand_id=normalized_brand_id,
        agency_id=normalized_agency_id,
    )

    agency = None
    if normalized_agency_id:
        if not ObjectId.is_valid(normalized_agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")
        agency = await find_agency_by_id(db, normalized_agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        if str(agency.get("group_id") or "") != normalized_group_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected group")
        if str(agency.get("brand_id") or "") != normalized_brand_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected brand")

    return {
        "group_id": normalized_group_id,
        "group_name": str(group.get("name") or ""),
        "brand_id": normalized_brand_id,
        "brand_name": str(brand.get("name") or ""),
        "agency_id": normalized_agency_id,
        "agency_name": str(agency.get("name") or "") if agency else "",
    }


async def list_price_bulletins_with_enrichment(
    db: Any,
    *,
    query: Dict[str, Any],
    normalized_agency_id: Optional[str],
    active_only: bool,
    latest_per_model: bool,
    serialize_doc: Callable[[Dict[str, Any]], Dict[str, Any]],
    is_price_bulletin_active: Callable[[Dict[str, Any], str], bool],
) -> List[Dict[str, Any]]:
    docs = await list_price_bulletins(db, query=query, limit=5000)
    current_date_ymd = datetime.now(timezone.utc).date().isoformat()
    if active_only:
        docs = [doc for doc in docs if is_price_bulletin_active(doc, current_date_ymd)]

    if latest_per_model:
        deduped: Dict[str, Dict[str, Any]] = {}
        sorted_docs = docs
        if normalized_agency_id:
            sorted_docs = sorted(
                docs,
                key=lambda d: (
                    0 if str(d.get("agency_id") or "") == normalized_agency_id else 1,
                    str(d.get("effective_from") or ""),
                    d.get("updated_at") or datetime.min.replace(tzinfo=timezone.utc),
                    d.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
                ),
                reverse=True,
            )
        for doc in sorted_docs:
            key = f"{str(doc.get('model') or '').strip().casefold()}::{str(doc.get('version') or '').strip().casefold()}"
            if not key.strip(":"):
                continue
            if key in deduped:
                continue
            deduped[key] = doc
        docs = list(deduped.values())

    group_cache: Dict[str, str] = {}
    brand_cache: Dict[str, str] = {}
    agency_cache: Dict[str, str] = {}
    output: List[Dict[str, Any]] = []
    for doc in docs:
        serialized = serialize_doc(doc)
        gid = str(doc.get("group_id") or "")
        bid = str(doc.get("brand_id") or "")
        aid = str(doc.get("agency_id") or "")

        if gid and gid not in group_cache:
            group_doc = await find_group_by_id(db, gid)
            group_cache[gid] = str(group_doc.get("name") or "") if group_doc else ""
        if bid and bid not in brand_cache:
            brand_doc = await find_brand_by_id(db, bid)
            brand_cache[bid] = str(brand_doc.get("name") or "") if brand_doc else ""
        if aid and aid not in agency_cache:
            agency_doc = await find_agency_by_id(db, aid)
            agency_cache[aid] = str(agency_doc.get("name") or "") if agency_doc else ""

        serialized["group_name"] = group_cache.get(gid, "")
        serialized["brand_name"] = brand_cache.get(bid, "")
        serialized["agency_name"] = agency_cache.get(aid, "")
        serialized["is_active"] = is_price_bulletin_active(doc, current_date_ymd)
        output.append(serialized)

    return output


async def upsert_price_bulletins_items(
    db: Any,
    *,
    scope: Dict[str, Optional[str]],
    items: List[Any],
    effective_from: str,
    effective_to: Optional[str],
    bulletin_name: str,
    notes: Optional[str],
    current_user_id: Optional[str],
    now: datetime,
    to_non_negative_float: Callable[[Any, float], float],
) -> Tuple[int, List[Any]]:
    updated_count = 0
    valid_items: List[Any] = []

    for item in items:
        model_name = str(getattr(item, "model", None) or "").strip()
        if not model_name:
            continue

        valid_items.append(item)
        version_name = str(getattr(item, "version", None) or "").strip() or None
        query = {
            "group_id": scope["group_id"],
            "brand_id": scope["brand_id"],
            "agency_id": scope["agency_id"],
            "model": model_name,
            "version": version_name,
            "effective_from": effective_from,
        }
        transaction_price = getattr(item, "transaction_price", None)
        set_fields = {
            "group_id": scope["group_id"],
            "brand_id": scope["brand_id"],
            "agency_id": scope["agency_id"],
            "model": model_name,
            "version": version_name,
            "msrp": to_non_negative_float(getattr(item, "msrp", None), 0.0),
            "transaction_price": to_non_negative_float(transaction_price, 0.0) if transaction_price is not None else None,
            "brand_bonus_amount": to_non_negative_float(getattr(item, "brand_bonus_amount", None), 0.0),
            "brand_bonus_percentage": to_non_negative_float(getattr(item, "brand_bonus_percentage", None), 0.0),
            "dealer_bonus_amount": to_non_negative_float(getattr(item, "dealer_bonus_amount", None), 0.0),
            "dealer_share_percentage": min(100.0, to_non_negative_float(getattr(item, "dealer_share_percentage", None), 0.0)),
            "bulletin_name": bulletin_name,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "notes": notes,
            "source": "manual",
            "updated_at": now,
            "updated_by": current_user_id,
        }
        await upsert_price_bulletin(
            db,
            query=query,
            set_fields=set_fields,
            created_at=now,
            created_by=current_user_id,
        )
        updated_count += 1

    return updated_count, valid_items


async def reprice_sales_for_price_bulletin(
    db: Any,
    *,
    scope: Dict[str, Optional[str]],
    effective_from: Optional[str],
    effective_to: Optional[str],
    items: List[Any],
    price_item_applies_to_sale: Callable[..., bool],
    resolve_effective_sale_pricing_for_model: Callable[..., Any],
    apply_manual_sale_price_override: Callable[[Dict[str, Any], Optional[float]], Dict[str, Any]],
    calculate_commission: Callable[..., Any],
    to_non_negative_float: Callable[[Any, float], float],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
) -> Dict[str, int]:
    affected_exact_keys: set[str] = set()
    affected_model_keys: set[str] = set()
    for item in items:
        model_name = str(getattr(item, "model", None) or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        version_key = str(getattr(item, "version", None) or "").strip().casefold()
        affected_exact_keys.add(f"{model_key}::{version_key}")
        affected_model_keys.add(model_key)

    if not affected_exact_keys and not affected_model_keys:
        return {"checked": 0, "repriced": 0}

    sales_query: Dict[str, Any] = {
        "group_id": scope.get("group_id"),
        "brand_id": scope.get("brand_id"),
    }
    if scope.get("agency_id"):
        sales_query["agency_id"] = scope.get("agency_id")

    start_dt = None
    if effective_from:
        try:
            start_dt = datetime.fromisoformat(f"{effective_from}T00:00:00+00:00")
        except ValueError:
            start_dt = None

    end_dt_exclusive = None
    if effective_to:
        try:
            end_dt = datetime.fromisoformat(f"{effective_to}T00:00:00+00:00")
            end_dt_exclusive = end_dt + timedelta(days=1)
        except ValueError:
            end_dt_exclusive = None

    if start_dt or end_dt_exclusive:
        date_query: Dict[str, Any] = {}
        if start_dt:
            date_query["$gte"] = start_dt
        if end_dt_exclusive:
            date_query["$lt"] = end_dt_exclusive
        sales_query["sale_date"] = date_query

    sales = await list_sales_for_repricing(db, query=sales_query, limit=50000)
    if not sales:
        return {"checked": 0, "repriced": 0}

    vehicle_ids = [str(sale.get("vehicle_id") or "") for sale in sales if ObjectId.is_valid(str(sale.get("vehicle_id") or ""))]
    vehicles = await list_vehicles_by_ids(db, vehicle_ids=vehicle_ids, limit=len(vehicle_ids) or 1)
    vehicle_map: Dict[str, Dict[str, Any]] = {str(vehicle["_id"]): vehicle for vehicle in vehicles}

    checked = 0
    repriced = 0

    for sale in sales:
        vehicle_id = str(sale.get("vehicle_id") or "")
        vehicle = vehicle_map.get(vehicle_id)
        sale_model = str(sale.get("model") or (vehicle or {}).get("model") or "").strip()
        sale_version = str(
            sale.get("version")
            or (vehicle or {}).get("version")
            or (vehicle or {}).get("trim")
            or ""
        ).strip()
        if not price_item_applies_to_sale(
            sale_model=sale_model,
            sale_version=sale_version,
            affected_exact_keys=affected_exact_keys,
            affected_model_keys=affected_model_keys,
        ):
            continue

        checked += 1
        sale_date = coerce_utc_datetime(sale.get("sale_date")) or datetime.now(timezone.utc)
        reference_date_ymd = sale_date.date().isoformat()
        fallback_price = to_non_negative_float(
            sale.get("sale_price"),
            to_non_negative_float((vehicle or {}).get("msrp"), 0.0),
        )
        configured_pricing = await resolve_effective_sale_pricing_for_model(
            group_id=sale.get("group_id") or (vehicle or {}).get("group_id"),
            brand_id=sale.get("brand_id") or (vehicle or {}).get("brand_id"),
            agency_id=sale.get("agency_id") or (vehicle or {}).get("agency_id"),
            model=sale_model,
            version=sale_version,
            reference_date_ymd=reference_date_ymd,
            fallback_msrp=fallback_price,
        )
        effective_pricing = apply_manual_sale_price_override(configured_pricing, sale.get("sale_price"))
        effective_sale_price = to_non_negative_float(effective_pricing.get("transaction_price"), 0.0)
        if effective_sale_price <= 0:
            continue

        seller_id = str(sale.get("seller_id") or "")
        agency_id = str(sale.get("agency_id") or (vehicle or {}).get("agency_id") or "")
        if not seller_id or not agency_id:
            continue

        recalculated_commission = await calculate_commission(
            {
                "sale_price": effective_sale_price,
                "commission_base_price": to_non_negative_float(
                    effective_pricing.get("commission_base_price"),
                    effective_sale_price,
                ),
                "fi_revenue": to_non_negative_float(sale.get("fi_revenue"), 0.0),
                "plant_incentive": to_non_negative_float(sale.get("plant_incentive"), 0.0),
                "model": sale_model,
            },
            agency_id,
            seller_id,
            vehicle=vehicle,
            sale_date=sale_date,
        )

        current_sale_price = to_non_negative_float(sale.get("sale_price"), 0.0)
        current_commission = to_non_negative_float(sale.get("commission"), 0.0)
        current_commission_base = to_non_negative_float(sale.get("commission_base_price"), current_sale_price)
        current_brand_incentive = to_non_negative_float(sale.get("brand_incentive_amount"), 0.0)
        current_dealer_incentive = to_non_negative_float(sale.get("dealer_incentive_amount"), 0.0)
        if (
            abs(current_sale_price - effective_sale_price) < 0.01
            and abs(current_commission - recalculated_commission) < 0.01
            and abs(
                current_commission_base
                - to_non_negative_float(effective_pricing.get("commission_base_price"), effective_sale_price)
            ) < 0.01
            and abs(current_brand_incentive - to_non_negative_float(effective_pricing.get("brand_incentive_amount"), 0.0)) < 0.01
            and abs(current_dealer_incentive - to_non_negative_float(effective_pricing.get("dealer_incentive_amount"), 0.0)) < 0.01
        ):
            continue

        await update_sale_fields(
            db,
            sale_id=sale.get("_id"),
            set_fields={
                "sale_price": round(effective_sale_price, 2),
                "commission": round(recalculated_commission, 2),
                "commission_base_price": round(
                    to_non_negative_float(effective_pricing.get("commission_base_price"), effective_sale_price),
                    2,
                ),
                "effective_revenue": round(
                    to_non_negative_float(effective_pricing.get("effective_revenue"), effective_sale_price),
                    2,
                ),
                "brand_incentive_amount": round(to_non_negative_float(effective_pricing.get("brand_incentive_amount"), 0.0), 2),
                "dealer_incentive_amount": round(to_non_negative_float(effective_pricing.get("dealer_incentive_amount"), 0.0), 2),
                "undocumented_dealer_incentive_amount": round(
                    to_non_negative_float(effective_pricing.get("undocumented_dealer_incentive_amount"), 0.0),
                    2,
                ),
                "model": sale_model or None,
                "version": sale_version or None,
                "price_source": str(effective_pricing.get("price_source") or "price_bulletin"),
                "updated_at": datetime.now(timezone.utc),
            },
        )
        repriced += 1

    return {"checked": checked, "repriced": repriced}


async def remove_price_bulletin(
    db: Any,
    *,
    bulletin_id: str,
) -> Optional[Dict[str, Any]]:
    previous = await find_price_bulletin_by_id(db, bulletin_id)
    if not previous:
        return None
    await delete_price_bulletin_by_id(db, bulletin_id)
    return previous
