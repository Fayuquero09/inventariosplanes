from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class InventoryRouteHandlers:
    VehicleCreate: Any
    VehicleAgingIncentiveApply: Any
    create_vehicle: Callable[[Any, Request], Awaitable[Any]]
    get_vehicles: Callable[..., Awaitable[Any]]
    get_vehicle: Callable[[str, Request], Awaitable[Any]]
    apply_vehicle_aging_incentive: Callable[[str, Any, Request], Awaitable[Any]]
    update_vehicle: Callable[[str, Request], Awaitable[Any]]


def register_inventory_routes(router: APIRouter, handlers: InventoryRouteHandlers) -> None:
    @router.post("/vehicles")
    async def create_vehicle_route(vehicle_data: handlers.VehicleCreate, request: Request):
        return await handlers.create_vehicle(vehicle_data, request)

    @router.get("/vehicles")
    async def get_vehicles_route(
        request: Request,
        agency_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        group_id: Optional[str] = None,
        status: Optional[str] = None,
        vehicle_type: Optional[str] = None,
        sold_current_month_only: bool = False,
    ):
        return await handlers.get_vehicles(
            request=request,
            agency_id=agency_id,
            brand_id=brand_id,
            group_id=group_id,
            status=status,
            vehicle_type=vehicle_type,
            sold_current_month_only=sold_current_month_only,
        )

    @router.get("/vehicles/{vehicle_id}")
    async def get_vehicle_route(vehicle_id: str, request: Request):
        return await handlers.get_vehicle(vehicle_id, request)

    @router.post("/vehicles/{vehicle_id}/aging-incentive")
    async def apply_vehicle_aging_incentive_route(
        vehicle_id: str,
        payload: handlers.VehicleAgingIncentiveApply,
        request: Request,
    ):
        return await handlers.apply_vehicle_aging_incentive(vehicle_id, payload, request)

    @router.put("/vehicles/{vehicle_id}")
    async def update_vehicle_route(vehicle_id: str, request: Request):
        return await handlers.update_vehicle(vehicle_id, request)

