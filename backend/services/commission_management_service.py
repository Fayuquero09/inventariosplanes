from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple


def to_non_negative_float(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float(default)
    if numeric != numeric:  # NaN
        return float(default)
    return max(0.0, numeric)


def sale_effective_revenue(sale: Dict[str, Any]) -> float:
    explicit = sale.get("effective_revenue")
    if explicit is not None:
        return to_non_negative_float(explicit, 0.0)
    sale_price = to_non_negative_float(sale.get("sale_price"), 0.0)
    brand_incentive = to_non_negative_float(sale.get("brand_incentive_amount"), 0.0)
    return round(sale_price + brand_incentive, 2)


def sale_commission_base_price(sale: Dict[str, Any]) -> float:
    explicit = sale.get("commission_base_price")
    if explicit is not None:
        return to_non_negative_float(explicit, 0.0)
    sale_price = to_non_negative_float(sale.get("sale_price"), 0.0)
    brand_incentive = to_non_negative_float(sale.get("brand_incentive_amount"), 0.0)
    return round(sale_price + brand_incentive, 2)


def normalize_commission_matrix_volume_tiers(
    tiers: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in tiers or []:
        payload = row or {}
        try:
            min_units = int(payload.get("min_units"))
        except (TypeError, ValueError):
            continue
        min_units = max(1, min_units)

        max_units_raw = payload.get("max_units")
        max_units: Optional[int] = None
        if max_units_raw not in (None, ""):
            try:
                parsed_max = int(max_units_raw)
                if parsed_max > 0:
                    max_units = max(min_units, parsed_max)
            except (TypeError, ValueError):
                max_units = None

        bonus_per_unit = to_non_negative_float(payload.get("bonus_per_unit"), 0.0)
        if bonus_per_unit <= 0:
            continue

        dedupe_key = f"{min_units}:{max_units if max_units is not None else 'inf'}:{round(bonus_per_unit, 6)}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(
            {
                "min_units": min_units,
                "max_units": max_units,
                "bonus_per_unit": bonus_per_unit,
            }
        )

    normalized.sort(key=lambda row: (int(row.get("min_units") or 0), int(row.get("max_units") or 10**9)))
    return normalized


def normalize_commission_matrix_general(general: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = general or {}
    return {
        "global_percentage": to_non_negative_float(payload.get("global_percentage"), 0.0),
        "global_per_unit_bonus": to_non_negative_float(payload.get("global_per_unit_bonus"), 0.0),
        "global_aged_61_90_bonus": to_non_negative_float(payload.get("global_aged_61_90_bonus"), 0.0),
        "global_aged_90_plus_bonus": to_non_negative_float(payload.get("global_aged_90_plus_bonus"), 0.0),
        "volume_tiers": normalize_commission_matrix_volume_tiers(payload.get("volume_tiers")),
    }


def normalize_commission_matrix_models(
    models: Optional[List[Dict[str, Any]]],
    *,
    default_plant_share_pct: float,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in models or []:
        model_name = str((row or {}).get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        if model_key in seen:
            continue
        seen.add(model_key)
        raw_model_percentage = (row or {}).get("model_percentage")
        model_percentage = None
        if raw_model_percentage is not None and str(raw_model_percentage).strip() != "":
            model_percentage = to_non_negative_float(raw_model_percentage, 0.0)
        normalized.append(
            {
                "model": model_name,
                "model_percentage": model_percentage,
                "model_bonus": to_non_negative_float((row or {}).get("model_bonus"), 0.0),
                "aged_61_90_bonus": to_non_negative_float((row or {}).get("aged_61_90_bonus"), 0.0),
                "aged_90_plus_bonus": to_non_negative_float((row or {}).get("aged_90_plus_bonus"), 0.0),
                "plant_incentive_share_pct": min(
                    100.0,
                    to_non_negative_float(
                        (row or {}).get("plant_incentive_share_pct"),
                        default_plant_share_pct,
                    ),
                ),
            }
        )
    return normalized


def get_catalog_models_for_brand(
    brand_name: Optional[str],
    *,
    build_catalog_tree_from_source: Callable[..., Dict[str, Any]],
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_catalog_price: Callable[[Any], float],
) -> List[Dict[str, Any]]:
    make_name = str(brand_name or "").strip()
    if not make_name:
        return []
    try:
        catalog = build_catalog_tree_from_source(all_years=True)
        make_entry = find_catalog_make(catalog, make_name)
        if not make_entry:
            return []
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for model_entry in make_entry.get("models", []):
            model_name = str(model_entry.get("name") or "").strip()
            if not model_name:
                continue
            key = model_name.casefold()
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "model": model_name,
                    "min_msrp": to_non_negative_float(parse_catalog_price(model_entry.get("min_msrp")), 0.0),
                }
            )
        rows.sort(key=lambda item: item["model"].lower())
        return rows
    except Exception:
        return []


def build_matrix_models_response(
    *,
    catalog_models: List[Dict[str, Any]],
    overrides: List[Dict[str, Any]],
    default_percentage: float,
    default_plant_share_pct: float,
) -> List[Dict[str, Any]]:
    override_map: Dict[str, Dict[str, Any]] = {
        str(item.get("model") or "").strip().casefold(): item
        for item in overrides
        if str(item.get("model") or "").strip()
    }
    rows: List[Dict[str, Any]] = []
    used: set[str] = set()
    for item in catalog_models:
        model_name = str(item.get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        used.add(model_key)
        override = override_map.get(model_key) or {}
        override_model_percentage = override.get("model_percentage")
        model_percentage = (
            to_non_negative_float(override_model_percentage, default_percentage)
            if override_model_percentage is not None
            else to_non_negative_float(default_percentage, 0.0)
        )
        rows.append(
            {
                "model": model_name,
                "min_msrp": to_non_negative_float(item.get("min_msrp"), 0.0),
                "model_percentage": model_percentage,
                "model_bonus": to_non_negative_float(override.get("model_bonus"), 0.0),
                "aged_61_90_bonus": to_non_negative_float(override.get("aged_61_90_bonus"), 0.0),
                "aged_90_plus_bonus": to_non_negative_float(override.get("aged_90_plus_bonus"), 0.0),
                "plant_incentive_share_pct": min(
                    100.0,
                    to_non_negative_float(
                        override.get("plant_incentive_share_pct"),
                        default_plant_share_pct,
                    ),
                ),
                "source": "catalog",
            }
        )

    custom_rows: List[Dict[str, Any]] = []
    for override in overrides:
        model_name = str(override.get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        if model_key in used:
            continue
        custom_rows.append(
            {
                "model": model_name,
                "min_msrp": 0.0,
                "model_percentage": to_non_negative_float(
                    override.get("model_percentage"),
                    to_non_negative_float(default_percentage, 0.0),
                ),
                "model_bonus": to_non_negative_float(override.get("model_bonus"), 0.0),
                "aged_61_90_bonus": to_non_negative_float(override.get("aged_61_90_bonus"), 0.0),
                "aged_90_plus_bonus": to_non_negative_float(override.get("aged_90_plus_bonus"), 0.0),
                "plant_incentive_share_pct": min(
                    100.0,
                    to_non_negative_float(
                        override.get("plant_incentive_share_pct"),
                        default_plant_share_pct,
                    ),
                ),
                "source": "custom",
            }
        )
    custom_rows.sort(key=lambda item: item["model"].lower())
    rows.extend(custom_rows)
    return rows


def resolve_matrix_volume_bonus_per_unit(
    volume_tiers: Optional[List[Dict[str, Any]]],
    seller_month_units: int,
) -> float:
    units = max(0, int(seller_month_units or 0))
    if units <= 0:
        return 0.0
    best_match: Optional[Dict[str, Any]] = None
    for tier in volume_tiers or []:
        min_units = int(tier.get("min_units") or 0)
        max_units = tier.get("max_units")
        if units < min_units:
            continue
        if max_units is not None and units > int(max_units):
            continue
        if best_match is None or min_units > int(best_match.get("min_units") or 0):
            best_match = tier
    if not best_match:
        return 0.0
    return to_non_negative_float(best_match.get("bonus_per_unit"), 0.0)


def normalize_commission_status(
    status: Any,
    *,
    pending_status: str,
    approved_status: str,
    rejected_status: str,
) -> str:
    normalized = str(status or "").strip().lower()
    if normalized not in {pending_status, approved_status, rejected_status}:
        return approved_status
    return normalized


def build_commission_matrix_upsert_fields(
    *,
    agency_id: str,
    brand_id: Any,
    group_id: Any,
    normalized_general: Dict[str, Any],
    normalized_models: List[Dict[str, Any]],
    current_user_id: Optional[str],
    now: datetime,
) -> Dict[str, Dict[str, Any]]:
    return {
        "set_fields": {
            "agency_id": agency_id,
            "brand_id": brand_id,
            "group_id": group_id,
            "general": normalized_general,
            "models": normalized_models,
            "updated_at": now,
            "updated_by": current_user_id,
        },
        "set_on_insert": {
            "created_at": now,
            "created_by": current_user_id,
        },
    }


def build_commission_rule_doc(
    *,
    agency_id: str,
    brand_id: Any,
    group_id: Any,
    name: str,
    rule_type: str,
    value: float,
    min_units: int,
    max_units: Optional[int],
    current_user_id: Optional[str],
    now: datetime,
    pending_status: str,
) -> Dict[str, Any]:
    return {
        "agency_id": agency_id,
        "brand_id": brand_id,
        "group_id": group_id,
        "name": name,
        "rule_type": rule_type,
        "value": value,
        "min_units": min_units,
        "max_units": max_units,
        "approval_status": pending_status,
        "approval_comment": None,
        "created_by": current_user_id,
        "submitted_by": current_user_id,
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "created_at": now,
        "updated_at": now,
        "updated_by": current_user_id,
    }


def build_commission_rule_update_fields(
    *,
    name: str,
    rule_type: str,
    value: float,
    min_units: int,
    max_units: Optional[int],
    current_user_id: Optional[str],
    now: datetime,
    pending_status: str,
) -> Dict[str, Any]:
    return {
        "name": name,
        "rule_type": rule_type,
        "value": value,
        "min_units": min_units,
        "max_units": max_units,
        "approval_status": pending_status,
        "approval_comment": None,
        "submitted_by": current_user_id,
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "updated_at": now,
        "updated_by": current_user_id,
    }


def build_commission_approval_update_fields(
    *,
    decision: str,
    comment: Optional[str],
    current_user_id: Optional[str],
    now: datetime,
    approved_status: str,
    rejected_status: str,
) -> Dict[str, Any]:
    normalized_decision = str(decision or "").strip().lower()
    if normalized_decision not in {approved_status, rejected_status}:
        raise ValueError("Decision must be approved or rejected")

    update_fields: Dict[str, Any] = {
        "approval_status": normalized_decision,
        "updated_at": now,
        "updated_by": current_user_id,
    }

    if normalized_decision == approved_status:
        update_fields.update(
            {
                "approved_by": current_user_id,
                "approved_at": now,
                "rejected_by": None,
                "rejected_at": None,
                "approval_comment": None,
            }
        )
        return update_fields

    rejection_comment = str(comment or "").strip()
    if not rejection_comment:
        raise ValueError("Rejection requires a comment")
    update_fields.update(
        {
            "approved_by": None,
            "approved_at": None,
            "rejected_by": current_user_id,
            "rejected_at": now,
            "approval_comment": rejection_comment,
        }
    )
    return update_fields


def build_commission_simulator_projection(
    *,
    rules: List[Dict[str, Any]],
    units: int,
    average_ticket: float,
    average_fi_revenue: float,
    target_commission: float,
    calculate_commission_from_rules: Callable[..., float],
) -> Dict[str, Any]:
    estimated_commission = calculate_commission_from_rules(
        rules,
        units=units,
        average_ticket=average_ticket,
        average_fi_revenue=average_fi_revenue,
    )
    difference = round(estimated_commission - target_commission, 2)

    suggested_units: Optional[int] = None
    max_units_limit = max(units + 300, 500)
    for candidate_units in range(0, max_units_limit + 1):
        candidate_commission = calculate_commission_from_rules(
            rules,
            units=candidate_units,
            average_ticket=average_ticket,
            average_fi_revenue=average_fi_revenue,
        )
        if candidate_commission >= target_commission:
            suggested_units = candidate_units
            break

    return {
        "estimated_commission": estimated_commission,
        "difference_vs_target": difference,
        "suggested_units_to_target": suggested_units,
    }


def build_month_bounds(year: int, month: int) -> Tuple[datetime, datetime]:
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start_date, end_date


def build_commission_closure_snapshot(
    *,
    sales: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Any]:
    return {
        "sales_count": len(sales),
        "sales_total": round(sum(sale_effective_revenue(s) for s in sales), 2),
        "fi_revenue_total": round(sum(float(s.get("fi_revenue", 0) or 0) for s in sales), 2),
        "commission_total": round(sum(float(s.get("commission", 0) or 0) for s in sales), 2),
        "generated_at": now,
    }


def build_commission_closure_doc(
    *,
    seller_id: str,
    agency_id: str,
    brand_id: Any,
    group_id: Any,
    month: int,
    year: int,
    snapshot: Dict[str, Any],
    current_user_id: Optional[str],
    now: datetime,
    pending_status: str,
) -> Dict[str, Any]:
    return {
        "seller_id": seller_id,
        "agency_id": agency_id,
        "brand_id": brand_id,
        "group_id": group_id,
        "month": month,
        "year": year,
        "snapshot": snapshot,
        "approval_status": pending_status,
        "approval_comment": None,
        "created_by": current_user_id,
        "submitted_by": current_user_id,
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "created_at": now,
        "updated_at": now,
        "updated_by": current_user_id,
    }
