from dataclasses import dataclass

from fastapi import APIRouter

from .dashboard_routes import DashboardRouteHandlers, register_dashboard_routes
from .health_routes import HealthRouteHandlers, register_health_routes
from .inventory_routes import InventoryRouteHandlers, register_inventory_routes
from .sales_objectives_routes import SalesObjectivesRouteHandlers, register_sales_objectives_routes


@dataclass(frozen=True)
class RouteModuleHandlers:
    inventory: InventoryRouteHandlers
    health: HealthRouteHandlers
    sales_objectives: SalesObjectivesRouteHandlers
    dashboard: DashboardRouteHandlers


def register_route_modules(router: APIRouter, handlers: RouteModuleHandlers) -> None:
    register_inventory_routes(router, handlers.inventory)
    register_health_routes(router, handlers.health)
    register_sales_objectives_routes(router, handlers.sales_objectives)
    register_dashboard_routes(router, handlers.dashboard)
