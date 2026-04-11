from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import HTTPException

from repositories.sales_repository import (
    find_user_by_id,
    find_vehicle_by_id,
    insert_sale,
    list_sales,
    update_vehicle_fields,
)


async def create_sale_record(
    db: Any,
    *,
    sale_data: Dict[str, Any],
    vehicle: Dict[str, Any],
    calculate_commission: Callable[..., Awaitable[float]],
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override: Callable[[Dict[str, Any], Any], Dict[str, Any]],
    extract_active_aging_incentive_plan: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    apply_aging_plan_to_effective_pricing: Callable[
        [Dict[str, Any], Optional[Dict[str, Any]]],
        Tuple[Dict[str, Any], Dict[str, float]],
    ],
    to_non_negative_float: Callable[[Any, float], float],
) -> Dict[str, Any]:
    sale_date_input = sale_data.get("sale_date")
    if sale_date_input:
        if isinstance(sale_date_input, str):
            sale_date = datetime.fromisoformat(sale_date_input.replace("Z", "+00:00"))
        else:
            sale_date = sale_date_input
    else:
        sale_date = datetime.now(timezone.utc)

    reference_date_ymd = sale_date.date().isoformat() if sale_date else None
    configured_pricing = await resolve_effective_sale_pricing_for_model(
        group_id=vehicle.get("group_id"),
        brand_id=vehicle.get("brand_id"),
        agency_id=vehicle.get("agency_id"),
        model=vehicle.get("model"),
        version=vehicle.get("version") or vehicle.get("trim"),
        reference_date_ymd=reference_date_ymd,
        fallback_msrp=to_non_negative_float(
            sale_data.get("sale_price"),
            to_non_negative_float(vehicle.get("msrp"), 0.0),
        ),
    )
    effective_pricing = apply_manual_sale_price_override(configured_pricing, sale_data.get("sale_price"))
    aging_plan = extract_active_aging_incentive_plan(vehicle)
    effective_pricing, applied_aging = apply_aging_plan_to_effective_pricing(effective_pricing, aging_plan)
    resolved_sale_price = to_non_negative_float(effective_pricing.get("transaction_price"), 0.0)
    if resolved_sale_price <= 0:
        raise HTTPException(
            status_code=400,
            detail="Sale price is required. Configure Precio Boletín/Incentivos in Precios or provide sale_price.",
        )

    base_commission = await calculate_commission(
        {
            "sale_price": resolved_sale_price,
            "commission_base_price": to_non_negative_float(
                effective_pricing.get("commission_base_price"),
                resolved_sale_price,
            ),
            "fi_revenue": sale_data.get("fi_revenue", 0),
            "plant_incentive": sale_data.get("plant_incentive", 0),
            "model": vehicle.get("model"),
        },
        vehicle["agency_id"],
        sale_data.get("seller_id"),
        vehicle=vehicle,
        sale_date=sale_date,
    )
    commission = round(base_commission + to_non_negative_float(applied_aging.get("seller_bonus_amount"), 0.0), 2)

    sale_doc = {
        "vehicle_id": sale_data.get("vehicle_id"),
        "seller_id": sale_data.get("seller_id"),
        "agency_id": vehicle.get("agency_id"),
        "brand_id": vehicle.get("brand_id"),
        "group_id": vehicle.get("group_id"),
        "sale_price": resolved_sale_price,
        "commission_base_price": round(
            to_non_negative_float(effective_pricing.get("commission_base_price"), resolved_sale_price),
            2,
        ),
        "effective_revenue": round(
            to_non_negative_float(effective_pricing.get("effective_revenue"), resolved_sale_price),
            2,
        ),
        "brand_incentive_amount": round(
            to_non_negative_float(effective_pricing.get("brand_incentive_amount"), 0.0),
            2,
        ),
        "dealer_incentive_amount": round(
            to_non_negative_float(effective_pricing.get("dealer_incentive_amount"), 0.0),
            2,
        ),
        "undocumented_dealer_incentive_amount": round(
            to_non_negative_float(effective_pricing.get("undocumented_dealer_incentive_amount"), 0.0),
            2,
        ),
        "aging_incentive_sale_discount_amount": round(
            to_non_negative_float(applied_aging.get("sale_discount_amount"), 0.0),
            2,
        ),
        "aging_incentive_seller_bonus_amount": round(
            to_non_negative_float(applied_aging.get("seller_bonus_amount"), 0.0),
            2,
        ),
        "aging_incentive_total_amount": round(
            to_non_negative_float(applied_aging.get("total_amount"), 0.0),
            2,
        ),
        "sale_date": sale_date,
        "fi_revenue": sale_data.get("fi_revenue", 0),
        "plant_incentive": sale_data.get("plant_incentive", 0),
        "model": vehicle.get("model"),
        "version": vehicle.get("version") or vehicle.get("trim"),
        "price_source": str(effective_pricing.get("price_source") or "price_bulletin"),
        "commission": commission,
        "created_at": datetime.now(timezone.utc),
    }
    sale_id = await insert_sale(db, sale_doc)

    vehicle_update: Dict[str, Any] = {"status": "sold", "exit_date": sale_date}
    if aging_plan:
        vehicle_update.update(
            {
                "aging_incentive_plan.active": False,
                "aging_incentive_plan.applied_sale_id": sale_id,
                "aging_incentive_plan.applied_at": datetime.now(timezone.utc),
                "aging_incentive_plan.applied_sale_discount_amount": round(
                    to_non_negative_float(applied_aging.get("sale_discount_amount"), 0.0),
                    2,
                ),
                "aging_incentive_plan.applied_seller_bonus_amount": round(
                    to_non_negative_float(applied_aging.get("seller_bonus_amount"), 0.0),
                    2,
                ),
            }
        )
    await update_vehicle_fields(db, sale_data.get("vehicle_id"), vehicle_update)

    return {
        "sale_id": sale_id,
        "sale_doc": sale_doc,
        "base_commission": base_commission,
        "effective_pricing": effective_pricing,
        "applied_aging": applied_aging,
        "resolved_sale_price": resolved_sale_price,
    }


async def list_sales_with_enrichment(
    db: Any,
    *,
    query: Dict[str, Any],
    serialize_doc: Callable[[Dict[str, Any]], Dict[str, Any]],
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    sales = await list_sales(db, query, limit=limit)
    result: List[Dict[str, Any]] = []

    for sale in sales:
        serialized = serialize_doc(sale)

        vehicle_id = sale.get("vehicle_id")
        if vehicle_id:
            vehicle = await find_vehicle_by_id(db, vehicle_id)
            if vehicle:
                serialized["vehicle_info"] = {
                    "model": vehicle.get("model"),
                    "year": vehicle.get("year"),
                    "trim": vehicle.get("trim"),
                    "color": vehicle.get("color"),
                    "vin": vehicle.get("vin"),
                }

        seller_id = sale.get("seller_id")
        if seller_id:
            seller = await find_user_by_id(db, seller_id)
            if seller:
                serialized["seller_name"] = seller.get("name")

        result.append(serialized)

    return result
