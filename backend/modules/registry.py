from dataclasses import dataclass

from fastapi import APIRouter

from .health_routes import HealthRouteHandlers, register_health_routes
from .inventory_routes import InventoryRouteHandlers, register_inventory_routes


@dataclass(frozen=True)
class RouteModuleHandlers:
    inventory: InventoryRouteHandlers
    health: HealthRouteHandlers


def register_route_modules(router: APIRouter, handlers: RouteModuleHandlers) -> None:
    register_inventory_routes(router, handlers.inventory)
    register_health_routes(router, handlers.health)

