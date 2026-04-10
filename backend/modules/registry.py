from dataclasses import dataclass

from fastapi import APIRouter

from .commissions_routes import CommissionsRouteHandlers, register_commissions_routes
from .dashboard_routes import DashboardRouteHandlers, register_dashboard_routes
from .financial_rates_routes import FinancialRatesRouteHandlers, register_financial_rates_routes
from .health_routes import HealthRouteHandlers, register_health_routes
from .inventory_routes import InventoryRouteHandlers, register_inventory_routes
from .sales_objectives_routes import SalesObjectivesRouteHandlers, register_sales_objectives_routes


@dataclass(frozen=True)
class RouteModuleHandlers:
    inventory: InventoryRouteHandlers
    health: HealthRouteHandlers
    sales_objectives: SalesObjectivesRouteHandlers
    dashboard: DashboardRouteHandlers
    financial_rates: FinancialRatesRouteHandlers
    commissions: CommissionsRouteHandlers


def register_route_modules(router: APIRouter, handlers: RouteModuleHandlers) -> None:
    register_inventory_routes(router, handlers.inventory)
    register_health_routes(router, handlers.health)
    register_sales_objectives_routes(router, handlers.sales_objectives)
    register_dashboard_routes(router, handlers.dashboard)
    register_financial_rates_routes(router, handlers.financial_rates)
    register_commissions_routes(router, handlers.commissions)
