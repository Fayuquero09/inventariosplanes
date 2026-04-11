from typing import Any, Awaitable, Callable, Dict, Optional


def annual_to_monthly(rate_annual_pct: float) -> float:
    return float(rate_annual_pct) / 12.0


def monthly_to_annual(rate_monthly_pct: float) -> float:
    return float(rate_monthly_pct) * 12.0


def _parse_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def extract_rate_components_from_doc(rate_doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    if not rate_doc:
        return {
            "tiie_rate_monthly": None,
            "spread_monthly": None,
            "grace_days": None,
        }

    rate_period = str(rate_doc.get("rate_period") or "").strip().lower()

    raw_tiie = rate_doc.get("tiie_rate")
    raw_spread = rate_doc.get("spread")
    raw_tiie_annual = rate_doc.get("tiie_rate_annual")
    raw_spread_annual = rate_doc.get("spread_annual")

    tiie_monthly: Optional[float] = None
    spread_monthly: Optional[float] = None

    if raw_tiie_annual is not None:
        tiie_annual_value = _parse_optional_float(raw_tiie_annual)
        tiie_monthly = annual_to_monthly(tiie_annual_value) if tiie_annual_value is not None else None
    elif raw_tiie is not None:
        tiie_value = _parse_optional_float(raw_tiie)
        if tiie_value is not None:
            if rate_period == "monthly":
                tiie_monthly = tiie_value
            elif rate_period == "annual":
                tiie_monthly = annual_to_monthly(tiie_value)
            else:
                tiie_monthly = tiie_value if tiie_value <= 3.0 else annual_to_monthly(tiie_value)

    if raw_spread_annual is not None:
        spread_annual_value = _parse_optional_float(raw_spread_annual)
        spread_monthly = annual_to_monthly(spread_annual_value) if spread_annual_value is not None else None
    elif raw_spread is not None:
        spread_value = _parse_optional_float(raw_spread)
        if spread_value is not None:
            if rate_period == "monthly":
                spread_monthly = spread_value
            elif rate_period == "annual":
                spread_monthly = annual_to_monthly(spread_value)
            else:
                spread_monthly = spread_value if spread_value <= 3.0 else annual_to_monthly(spread_value)

    grace_days_value = rate_doc.get("grace_days")
    grace_days = int(grace_days_value) if grace_days_value is not None else None

    return {
        "tiie_rate_monthly": tiie_monthly,
        "spread_monthly": spread_monthly,
        "grace_days": grace_days,
    }


async def resolve_effective_rate_components(
    db: Any,
    *,
    group_id: Any,
    brand_id: Any,
    agency_id: Any,
    find_latest_financial_rate: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    extract_rate_components_from_doc: Callable[[Optional[Dict[str, Any]]], Dict[str, Optional[float]]],
) -> Dict[str, float]:
    group_rate = await find_latest_financial_rate(
        db,
        group_id=group_id,
        brand_id=None,
        agency_id=None,
    )
    brand_rate = None
    if brand_id:
        brand_rate = await find_latest_financial_rate(
            db,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=None,
        )
    agency_rate = None
    if agency_id:
        agency_rate = await find_latest_financial_rate(
            db,
            group_id=group_id,
            brand_id=None,
            agency_id=agency_id,
        )

    group_components = extract_rate_components_from_doc(group_rate)
    brand_components = extract_rate_components_from_doc(brand_rate)
    agency_components = extract_rate_components_from_doc(agency_rate)

    tiie_monthly = (
        agency_components["tiie_rate_monthly"]
        if agency_components["tiie_rate_monthly"] is not None
        else brand_components["tiie_rate_monthly"]
        if brand_components["tiie_rate_monthly"] is not None
        else group_components["tiie_rate_monthly"]
        if group_components["tiie_rate_monthly"] is not None
        else None
    )
    spread_monthly = (
        agency_components["spread_monthly"]
        if agency_components["spread_monthly"] is not None
        else brand_components["spread_monthly"]
        if brand_components["spread_monthly"] is not None
        else group_components["spread_monthly"]
        if group_components["spread_monthly"] is not None
        else None
    )
    grace_days = (
        agency_components["grace_days"]
        if agency_components["grace_days"] is not None
        else brand_components["grace_days"]
        if brand_components["grace_days"] is not None
        else group_components["grace_days"]
        if group_components["grace_days"] is not None
        else 0
    )

    total_monthly = (tiie_monthly + spread_monthly) if tiie_monthly is not None and spread_monthly is not None else None
    return {
        "tiie_rate_monthly": tiie_monthly,
        "spread_monthly": spread_monthly,
        "total_rate_monthly": total_monthly,
        "grace_days": int(grace_days),
    }


async def build_default_financial_rate_name(
    db: Any,
    *,
    group_id: Optional[str],
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    find_group_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
) -> str:
    group_name = "Grupo"
    brand_name = None
    agency_name = None

    if group_id:
        group = await find_group_by_id(db, group_id)
        if group:
            group_name = str(group.get("name") or group_name)

    if brand_id:
        brand = await find_brand_by_id(db, brand_id)
        if brand:
            brand_name = str(brand.get("name") or "").strip() or None

    if agency_id:
        agency = await find_agency_by_id(db, agency_id)
        if agency:
            agency_name = str(agency.get("name") or "").strip() or None
            agency_brand_id = str(agency.get("brand_id") or "").strip() or None
            if not brand_name and agency_brand_id:
                agency_brand = await find_brand_by_id(db, agency_brand_id)
                if agency_brand:
                    brand_name = str(agency_brand.get("name") or "").strip() or None

    if agency_name:
        return f"Tasa {group_name} - {agency_name}"
    if brand_name:
        return f"Tasa {group_name} - {brand_name}"
    return f"Tasa General {group_name}"


async def enrich_financial_rate(
    db: Any,
    *,
    rate_doc: Dict[str, Any],
    serialize_doc: Callable[[Dict[str, Any]], Dict[str, Any]],
    extract_rate_components_from_doc: Callable[[Optional[Dict[str, Any]]], Dict[str, Optional[float]]],
    monthly_to_annual: Callable[[float], float],
    resolve_effective_rate_components_for_scope: Callable[..., Awaitable[Dict[str, float]]],
    find_group_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[[Any, str], Awaitable[Optional[Dict[str, Any]]]],
) -> Dict[str, Any]:
    result = serialize_doc(rate_doc)
    configured_components = extract_rate_components_from_doc(rate_doc)
    result["tiie_rate"] = (
        round(configured_components["tiie_rate_monthly"], 4)
        if configured_components["tiie_rate_monthly"] is not None
        else None
    )
    result["spread"] = (
        round(configured_components["spread_monthly"], 4)
        if configured_components["spread_monthly"] is not None
        else None
    )
    result["total_rate"] = (
        round((result["tiie_rate"] + result["spread"]), 4)
        if result["tiie_rate"] is not None and result["spread"] is not None
        else None
    )
    result["rate_period"] = "monthly"
    result["tiie_rate_annual"] = round(monthly_to_annual(result["tiie_rate"]), 4) if result["tiie_rate"] is not None else None
    result["spread_annual"] = round(monthly_to_annual(result["spread"]), 4) if result["spread"] is not None else None
    result["total_rate_annual"] = round(monthly_to_annual(result["total_rate"]), 4) if result["total_rate"] is not None else None

    effective_components = await resolve_effective_rate_components_for_scope(
        group_id=rate_doc.get("group_id"),
        brand_id=rate_doc.get("brand_id"),
        agency_id=rate_doc.get("agency_id"),
    )
    result["effective_tiie_rate"] = (
        round(effective_components["tiie_rate_monthly"], 4)
        if effective_components["tiie_rate_monthly"] is not None
        else None
    )
    result["effective_spread"] = (
        round(effective_components["spread_monthly"], 4)
        if effective_components["spread_monthly"] is not None
        else None
    )
    result["effective_total_rate"] = (
        round(effective_components["total_rate_monthly"], 4)
        if effective_components["total_rate_monthly"] is not None
        else None
    )
    result["effective_grace_days"] = int(effective_components["grace_days"])

    group_id = str(rate_doc.get("group_id") or "").strip()
    brand_id = str(rate_doc.get("brand_id") or "").strip()
    agency_id = str(rate_doc.get("agency_id") or "").strip()

    if group_id:
        group = await find_group_by_id(db, group_id)
        if group:
            result["group_name"] = group.get("name")
    if brand_id:
        brand = await find_brand_by_id(db, brand_id)
        if brand:
            result["brand_name"] = brand.get("name")
    if agency_id:
        agency = await find_agency_by_id(db, agency_id)
        if agency:
            result["agency_name"] = agency.get("name")

    return result
