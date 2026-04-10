from dataclasses import dataclass

from fastapi import APIRouter

from .auth_users_routes import AuthUsersRouteHandlers, register_auth_users_routes
from .commissions_routes import CommissionsRouteHandlers, register_commissions_routes
from .dashboard_routes import DashboardRouteHandlers, register_dashboard_routes
from .financial_rates_routes import FinancialRatesRouteHandlers, register_financial_rates_routes
from .health_routes import HealthRouteHandlers, register_health_routes
from .inventory_routes import InventoryRouteHandlers, register_inventory_routes
from .organization_catalog_routes import OrganizationCatalogRouteHandlers, register_organization_catalog_routes
from .sales_objectives_routes import SalesObjectivesRouteHandlers, register_sales_objectives_routes


@dataclass(frozen=True)
class RouteModuleHandlers:
    auth_users: AuthUsersRouteHandlers
    organization_catalog: OrganizationCatalogRouteHandlers
    inventory: InventoryRouteHandlers
    health: HealthRouteHandlers
    sales_objectives: SalesObjectivesRouteHandlers
    dashboard: DashboardRouteHandlers
    financial_rates: FinancialRatesRouteHandlers
    commissions: CommissionsRouteHandlers


def register_route_modules(router: APIRouter, handlers: RouteModuleHandlers) -> None:
    register_auth_users_routes(router, handlers.auth_users)
    register_organization_catalog_routes(router, handlers.organization_catalog)
    register_inventory_routes(router, handlers.inventory)
    register_health_routes(router, handlers.health)
    register_sales_objectives_routes(router, handlers.sales_objectives)
    register_dashboard_routes(router, handlers.dashboard)
    register_financial_rates_routes(router, handlers.financial_rates)
    register_commissions_routes(router, handlers.commissions)
