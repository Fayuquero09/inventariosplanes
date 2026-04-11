from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from repositories.commission_repository import (
    count_seller_sales_since,
    find_commission_matrix_by_agency,
    list_active_rules_by_agency,
)


def calculate_matrix_commission_for_sale(
    *,
    matrix_doc: Optional[Dict[str, Any]],
    sale: Dict[str, Any],
    vehicle: Optional[Dict[str, Any]],
    sale_date: Optional[datetime],
    seller_month_units: int,
    normalize_general: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    normalize_models: Callable[[Optional[List[Dict[str, Any]]]], List[Dict[str, Any]]],
    resolve_volume_bonus_per_unit: Callable[[Optional[List[Dict[str, Any]]], int], float],
    to_non_negative_float: Callable[[Any, float], float],
    sale_commission_base_price: Callable[[Dict[str, Any]], float],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    default_plant_share_pct: float,
) -> float:
    if not matrix_doc:
        return 0.0

    general = normalize_general((matrix_doc or {}).get("general"))
    models = normalize_models((matrix_doc or {}).get("models"))
    model_map: Dict[str, Dict[str, Any]] = {
        str(item.get("model") or "").strip().casefold(): item
        for item in models
        if str(item.get("model") or "").strip()
    }

    sale_price = sale_commission_base_price(sale or {})
    plant_incentive = to_non_negative_float((sale or {}).get("plant_incentive"), 0.0)
    model_name = str((sale or {}).get("model") or (vehicle or {}).get("model") or "").strip()
    model_rule = model_map.get(model_name.casefold()) if model_name else None

    total = 0.0
    effective_percentage = to_non_negative_float(general.get("global_percentage"), 0.0)
    if model_rule and model_rule.get("model_percentage") is not None:
        effective_percentage = to_non_negative_float(model_rule.get("model_percentage"), effective_percentage)
    total += sale_price * (effective_percentage / 100.0)
    total += general["global_per_unit_bonus"]
    total += resolve_volume_bonus_per_unit(general.get("volume_tiers"), seller_month_units)
    if model_rule:
        total += to_non_negative_float(model_rule.get("model_bonus"), 0.0)

    reference_sale_date = coerce_utc_datetime(sale_date) or datetime.now(timezone.utc)
    entry_date = coerce_utc_datetime((vehicle or {}).get("entry_date"))
    aging_days = 0
    if entry_date and reference_sale_date > entry_date:
        aging_days = max(0, int((reference_sale_date - entry_date).days))

    if 61 <= aging_days <= 90:
        total += general["global_aged_61_90_bonus"]
        if model_rule:
            total += to_non_negative_float(model_rule.get("aged_61_90_bonus"), 0.0)
    elif aging_days > 90:
        total += general["global_aged_90_plus_bonus"]
        if model_rule:
            total += to_non_negative_float(model_rule.get("aged_90_plus_bonus"), 0.0)

    plant_share_pct = (
        min(
            100.0,
            to_non_negative_float(model_rule.get("plant_incentive_share_pct"), default_plant_share_pct),
        )
        if model_rule
        else default_plant_share_pct
    )
    total += plant_incentive * (plant_share_pct / 100.0)

    return round(total, 2)


async def calculate_commission(
    db: Any,
    *,
    sale: Dict[str, Any],
    agency_id: str,
    seller_id: str,
    vehicle: Optional[Dict[str, Any]],
    sale_date: Optional[datetime],
    approved_status: str,
    normalize_general: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    normalize_models: Callable[[Optional[List[Dict[str, Any]]]], List[Dict[str, Any]]],
    resolve_volume_bonus_per_unit: Callable[[Optional[List[Dict[str, Any]]], int], float],
    to_non_negative_float: Callable[[Any, float], float],
    sale_commission_base_price: Callable[[Dict[str, Any]], float],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    default_plant_share_pct: float,
) -> float:
    rules = await list_active_rules_by_agency(
        db,
        agency_id=agency_id,
        approved_status=approved_status,
        limit=100,
    )

    reference_date = coerce_utc_datetime(sale_date) or datetime.now(timezone.utc)
    start_of_month = datetime(reference_date.year, reference_date.month, 1, tzinfo=timezone.utc)
    seller_sales = await count_seller_sales_since(
        db,
        seller_id=seller_id,
        agency_id=agency_id,
        since=start_of_month,
    )

    total_commission = 0.0
    for rule in rules:
        rule_type = rule.get("rule_type")
        if rule_type == "per_unit":
            total_commission += rule.get("value", 0)
        elif rule_type == "percentage":
            total_commission += sale_commission_base_price(sale) * (rule.get("value", 0) / 100)
        elif rule_type == "volume_bonus":
            if rule.get("min_units") and seller_sales >= rule.get("min_units"):
                if not rule.get("max_units") or seller_sales <= rule.get("max_units"):
                    total_commission += rule.get("value", 0)
        elif rule_type == "fi_bonus":
            total_commission += sale.get("fi_revenue", 0) * (rule.get("value", 0) / 100)

    matrix_doc = await find_commission_matrix_by_agency(db, agency_id=agency_id)
    seller_month_units = int(seller_sales or 0) + 1
    total_commission += calculate_matrix_commission_for_sale(
        matrix_doc=matrix_doc,
        sale=sale,
        vehicle=vehicle,
        sale_date=reference_date,
        seller_month_units=seller_month_units,
        normalize_general=normalize_general,
        normalize_models=normalize_models,
        resolve_volume_bonus_per_unit=resolve_volume_bonus_per_unit,
        to_non_negative_float=to_non_negative_float,
        sale_commission_base_price=sale_commission_base_price,
        coerce_utc_datetime=coerce_utc_datetime,
        default_plant_share_pct=default_plant_share_pct,
    )

    return round(total_commission, 2)
