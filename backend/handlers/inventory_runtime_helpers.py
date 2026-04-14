from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict


@dataclass(frozen=True)
class InventoryRuntimeHelperBundle:
    calculate_vehicle_financial_cost_in_period: Callable[[dict, datetime, datetime], Awaitable[float]]
    calculate_vehicle_financial_cost: Callable[[dict], Awaitable[float]]
    enrich_vehicle: Callable[[dict], Awaitable[dict]]


def build_inventory_runtime_helper_bundle(
    *,
    db: Any,
    object_id_cls: Any,
    days_per_month_for_rate: int,
    resolve_effective_rate_components_for_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, float]]],
    calculate_vehicle_financial_cost_in_period_service: Callable[..., Awaitable[float]],
    calculate_vehicle_financial_cost_service: Callable[..., Awaitable[float]],
    enrich_vehicle_service: Callable[..., Awaitable[dict]],
    serialize_doc: Callable[[dict], dict],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
) -> InventoryRuntimeHelperBundle:
    async def calculate_vehicle_financial_cost_in_period(
        vehicle: dict,
        period_start: datetime,
        period_end: datetime,
    ) -> float:
        return await calculate_vehicle_financial_cost_in_period_service(
            vehicle,
            period_start,
            period_end,
            resolve_effective_rate_components_for_vehicle=resolve_effective_rate_components_for_vehicle,
            days_per_month_for_rate=days_per_month_for_rate,
        )

    async def calculate_vehicle_financial_cost(vehicle: dict) -> float:
        return await calculate_vehicle_financial_cost_service(
            vehicle,
            resolve_effective_rate_components_for_vehicle=resolve_effective_rate_components_for_vehicle,
            days_per_month_for_rate=days_per_month_for_rate,
        )

    async def enrich_vehicle(vehicle: dict) -> dict:
        return await enrich_vehicle_service(
            vehicle,
            serialize_doc=serialize_doc,
            find_agency_by_id=lambda agency_id: db.agencies.find_one({"_id": object_id_cls(agency_id)}),
            find_brand_by_id=lambda brand_id: db.brands.find_one({"_id": object_id_cls(brand_id)}),
            calculate_vehicle_financial_cost=calculate_vehicle_financial_cost,
            find_latest_sale_for_vehicle=lambda vehicle_id: db.sales.find_one({"vehicle_id": vehicle_id}, sort=[("sale_date", -1)]),
            sale_effective_revenue=sale_effective_revenue,
            is_valid_object_id=object_id_cls.is_valid,
            find_user_by_id=lambda user_id: db.users.find_one({"_id": object_id_cls(user_id)}),
        )

    return InventoryRuntimeHelperBundle(
        calculate_vehicle_financial_cost_in_period=calculate_vehicle_financial_cost_in_period,
        calculate_vehicle_financial_cost=calculate_vehicle_financial_cost,
        enrich_vehicle=enrich_vehicle,
    )
