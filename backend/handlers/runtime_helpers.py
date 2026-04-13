from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple


@dataclass(frozen=True)
class RuntimeHelperBundle:
    calculate_commission: Callable[..., Awaitable[float]]
    extract_active_aging_incentive_plan: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]]
    apply_aging_plan_to_effective_pricing: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Tuple[Dict[str, Any], Dict[str, float]]]
    resolve_dashboard_scope_group_id: Callable[[Dict[str, Any]], Awaitable[Optional[str]]]
    find_dashboard_monthly_close: Callable[..., Awaitable[Tuple[Optional[Dict[str, Any]], str]]]
    build_vehicle_aging_suggestion: Callable[..., Awaitable[Optional[Dict[str, Any]]]]


def build_runtime_helper_bundle(
    *,
    db: Any,
    calculate_commission_service: Callable[..., Awaitable[float]],
    commission_approved: str,
    normalize_general: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    normalize_models: Callable[[Optional[Dict[str, Any]]], Dict[str, Dict[str, Any]]],
    resolve_volume_bonus_per_unit: Callable[[Optional[list], int], float],
    to_non_negative_float: Callable[[Any, float], float],
    sale_commission_base_price: Callable[[Dict[str, Any]], float],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    default_plant_share_pct: float,
    resolve_dashboard_scope_group_id_service: Callable[..., Awaitable[Optional[str]]],
    find_brand_group_id: Callable[..., Awaitable[Optional[str]]],
    find_agency_group_id: Callable[..., Awaitable[Optional[str]]],
    find_monthly_close: Callable[..., Awaitable[Tuple[Optional[Dict[str, Any]], str]]],
    enrich_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    build_vehicle_aging_suggestion_service: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    list_similar_sold_vehicles: Callable[..., Awaitable[Any]],
) -> RuntimeHelperBundle:
    async def calculate_commission(
        sale: dict,
        agency_id: str,
        seller_id: str,
        *,
        vehicle: Optional[Dict[str, Any]] = None,
        sale_date: Optional[datetime] = None,
    ) -> float:
        return await calculate_commission_service(
            db,
            sale=sale,
            agency_id=agency_id,
            seller_id=seller_id,
            vehicle=vehicle,
            sale_date=sale_date,
            approved_status=commission_approved,
            normalize_general=normalize_general,
            normalize_models=normalize_models,
            resolve_volume_bonus_per_unit=resolve_volume_bonus_per_unit,
            to_non_negative_float=to_non_negative_float,
            sale_commission_base_price=sale_commission_base_price,
            coerce_utc_datetime=coerce_utc_datetime,
            default_plant_share_pct=default_plant_share_pct,
        )

    def extract_active_aging_incentive_plan(vehicle: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        plan = (vehicle or {}).get("aging_incentive_plan")
        if not isinstance(plan, dict):
            return None
        if not bool(plan.get("active")):
            return None

        sale_discount_amount = round(to_non_negative_float(plan.get("sale_discount_amount"), 0.0), 2)
        seller_bonus_amount = round(to_non_negative_float(plan.get("seller_bonus_amount"), 0.0), 2)
        total_amount = round(sale_discount_amount + seller_bonus_amount, 2)
        if total_amount <= 0:
            return None

        return {
            "sale_discount_amount": sale_discount_amount,
            "seller_bonus_amount": seller_bonus_amount,
            "total_amount": total_amount,
            "suggested_amount": round(to_non_negative_float(plan.get("suggested_amount"), 0.0), 2),
            "configured_by": plan.get("configured_by"),
            "configured_by_name": plan.get("configured_by_name"),
            "configured_at": plan.get("configured_at"),
        }

    def apply_aging_plan_to_effective_pricing(
        effective_pricing: Dict[str, Any],
        aging_plan: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, float]]:
        pricing = dict(effective_pricing or {})
        applied_sale_discount = 0.0
        applied_seller_bonus = 0.0

        if aging_plan:
            transaction_price = to_non_negative_float(pricing.get("transaction_price"), 0.0)
            planned_sale_discount = to_non_negative_float(aging_plan.get("sale_discount_amount"), 0.0)
            applied_sale_discount = round(min(transaction_price, planned_sale_discount), 2)
            transaction_price = round(max(0.0, transaction_price - applied_sale_discount), 2)
            pricing["transaction_price"] = transaction_price

            brand_incentive_amount = to_non_negative_float(pricing.get("brand_incentive_amount"), 0.0)
            pricing["commission_base_price"] = round(transaction_price + brand_incentive_amount, 2)
            pricing["effective_revenue"] = round(transaction_price + brand_incentive_amount, 2)

            applied_seller_bonus = round(to_non_negative_float(aging_plan.get("seller_bonus_amount"), 0.0), 2)

        return pricing, {
            "sale_discount_amount": applied_sale_discount,
            "seller_bonus_amount": applied_seller_bonus,
            "total_amount": round(applied_sale_discount + applied_seller_bonus, 2),
        }

    async def resolve_dashboard_scope_group_id(scope_query: Dict[str, Any]) -> Optional[str]:
        return await resolve_dashboard_scope_group_id_service(
            db,
            scope_query=scope_query,
            find_brand_group_id=find_brand_group_id,
            find_agency_group_id=find_agency_group_id,
        )

    async def find_dashboard_monthly_close_wrapper(
        year: int,
        month: int,
        group_id: Optional[str] = None,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        return await find_monthly_close(
            db,
            year=int(year),
            month=int(month),
            group_id=str(group_id) if group_id else None,
        )

    async def build_vehicle_aging_suggestion(
        vehicle: Dict[str, Any],
        *,
        enriched_vehicle: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if enriched_vehicle is None:
            enriched_vehicle = await enrich_vehicle(vehicle)
        return await build_vehicle_aging_suggestion_service(
            db,
            vehicle=vehicle,
            enriched_vehicle=enriched_vehicle,
            list_similar_sold_vehicles=list_similar_sold_vehicles,
            to_non_negative_float=to_non_negative_float,
            now=datetime.now(timezone.utc),
        )

    return RuntimeHelperBundle(
        calculate_commission=calculate_commission,
        extract_active_aging_incentive_plan=extract_active_aging_incentive_plan,
        apply_aging_plan_to_effective_pricing=apply_aging_plan_to_effective_pricing,
        resolve_dashboard_scope_group_id=resolve_dashboard_scope_group_id,
        find_dashboard_monthly_close=find_dashboard_monthly_close_wrapper,
        build_vehicle_aging_suggestion=build_vehicle_aging_suggestion,
    )
