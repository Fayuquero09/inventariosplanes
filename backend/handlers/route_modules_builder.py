from dataclasses import dataclass
from typing import Any

from modules.auth_users_routes import AuthUsersRouteHandlers
from modules.commissions_routes import CommissionsRouteHandlers
from modules.dashboard_routes import DashboardRouteHandlers
from modules.financial_rates_routes import FinancialRatesRouteHandlers
from modules.health_routes import HealthRouteHandlers
from modules.import_routes import ImportRouteHandlers
from modules.inventory_routes import InventoryRouteHandlers
from modules.organization_catalog_routes import OrganizationCatalogRouteHandlers
from modules.price_bulletins_routes import PriceBulletinsRouteHandlers
from modules.registry import RouteModuleHandlers, register_route_modules
from modules.sales_routes import SalesRouteHandlers
from modules.sales_objectives_routes import SalesObjectivesRouteHandlers
from schemas.api_models import (
    AgencyCreate,
    BrandCreate,
    CommissionApprovalAction,
    CommissionClosureApprovalAction,
    CommissionClosureCreate,
    CommissionMatrixUpsert,
    CommissionRuleCreate,
    CommissionSimulatorInput,
    DashboardMonthlyCloseUpsert,
    FinancialRateBulkApplyRequest,
    FinancialRateCreate,
    GroupCreate,
    PriceBulletinBulkUpsert,
    SaleCreate,
    SalesObjectiveApprovalAction,
    SalesObjectiveCreate,
    VehicleAgingIncentiveApply,
    VehicleCreate,
)


@dataclass(frozen=True)
class RouteHandlerBundles:
    auth_users: Any
    organization_catalog: Any
    catalog: Any
    inventory: Any
    imports: Any
    sales: Any
    price_bulletins: Any
    sales_objectives: Any
    dashboard: Any
    financial_rates: Any
    commissions: Any


@dataclass(frozen=True)
class AppRouteHandlers:
    root: Any
    health: Any


def register_all_route_modules(api_router, *, bundles: RouteHandlerBundles, app_handlers: AppRouteHandlers) -> None:
    dashboard_handlers = bundles.dashboard
    if isinstance(dashboard_handlers, dict):
        get_dashboard_monthly_close = dashboard_handlers["get_dashboard_monthly_close"]
        get_dashboard_monthly_close_calendar = dashboard_handlers["get_dashboard_monthly_close_calendar"]
        upsert_dashboard_monthly_close = dashboard_handlers["upsert_dashboard_monthly_close"]
        get_dashboard_kpis = dashboard_handlers["get_dashboard_kpis"]
        get_sales_trends = dashboard_handlers["get_sales_trends"]
        get_seller_performance = dashboard_handlers["get_seller_performance"]
        get_vehicle_suggestions = dashboard_handlers["get_vehicle_suggestions"]
    else:
        get_dashboard_monthly_close = dashboard_handlers.get_dashboard_monthly_close
        get_dashboard_monthly_close_calendar = dashboard_handlers.get_dashboard_monthly_close_calendar
        upsert_dashboard_monthly_close = dashboard_handlers.upsert_dashboard_monthly_close
        get_dashboard_kpis = dashboard_handlers.get_dashboard_kpis
        get_sales_trends = dashboard_handlers.get_sales_trends
        get_seller_performance = dashboard_handlers.get_seller_performance
        get_vehicle_suggestions = dashboard_handlers.get_vehicle_suggestions

    register_route_modules(
        api_router,
        RouteModuleHandlers(
            auth_users=AuthUsersRouteHandlers(
                register=bundles.auth_users.register,
                login=bundles.auth_users.login,
                logout=bundles.auth_users.logout,
                reset_password=bundles.auth_users.reset_password,
                get_me=bundles.auth_users.get_me,
                google_auth=bundles.auth_users.google_auth,
                get_users=bundles.auth_users.get_users,
                update_user=bundles.auth_users.update_user,
                delete_user=bundles.auth_users.delete_user,
                get_audit_logs=bundles.auth_users.get_audit_logs,
                get_sellers=bundles.auth_users.get_sellers,
            ),
            organization_catalog=OrganizationCatalogRouteHandlers(
                GroupCreate=GroupCreate,
                BrandCreate=BrandCreate,
                AgencyCreate=AgencyCreate,
                create_group=bundles.organization_catalog.create_group,
                get_groups=bundles.organization_catalog.get_groups,
                get_group=bundles.organization_catalog.get_group,
                update_group=bundles.organization_catalog.update_group,
                delete_group=bundles.organization_catalog.delete_group,
                create_brand=bundles.organization_catalog.create_brand,
                get_brands=bundles.organization_catalog.get_brands,
                update_brand=bundles.organization_catalog.update_brand,
                delete_brand=bundles.organization_catalog.delete_brand,
                create_agency=bundles.organization_catalog.create_agency,
                get_agencies=bundles.organization_catalog.get_agencies,
                update_agency=bundles.organization_catalog.update_agency,
                get_catalog_makes=bundles.catalog.get_catalog_makes,
                get_catalog_models=bundles.catalog.get_catalog_models,
                get_catalog_versions=bundles.catalog.get_catalog_versions,
            ),
            inventory=InventoryRouteHandlers(
                VehicleCreate=VehicleCreate,
                VehicleAgingIncentiveApply=VehicleAgingIncentiveApply,
                create_vehicle=bundles.inventory.create_vehicle,
                get_vehicles=bundles.inventory.get_vehicles,
                get_vehicle=bundles.inventory.get_vehicle,
                apply_vehicle_aging_incentive=bundles.inventory.apply_vehicle_aging_incentive,
                update_vehicle=bundles.inventory.update_vehicle,
            ),
            health=HealthRouteHandlers(
                root=app_handlers.root,
                health=app_handlers.health,
            ),
            imports=ImportRouteHandlers(
                import_organization=bundles.imports.import_organization,
                import_vehicles=bundles.imports.import_vehicles,
                import_sales=bundles.imports.import_sales,
            ),
            sales=SalesRouteHandlers(
                SaleCreate=SaleCreate,
                create_sale=bundles.sales.create_sale,
                get_sales=bundles.sales.get_sales,
            ),
            price_bulletins=PriceBulletinsRouteHandlers(
                PriceBulletinBulkUpsert=PriceBulletinBulkUpsert,
                get_price_bulletins=bundles.price_bulletins.get_price_bulletins,
                upsert_price_bulletins_bulk=bundles.price_bulletins.upsert_price_bulletins_bulk,
                delete_price_bulletin=bundles.price_bulletins.delete_price_bulletin,
            ),
            sales_objectives=SalesObjectivesRouteHandlers(
                SalesObjectiveCreate=SalesObjectiveCreate,
                SalesObjectiveApprovalAction=SalesObjectiveApprovalAction,
                create_sales_objective=bundles.sales_objectives.create_sales_objective,
                get_sales_objectives=bundles.sales_objectives.get_sales_objectives,
                get_sales_objective_suggestion=bundles.sales_objectives.get_sales_objective_suggestion,
                update_sales_objective=bundles.sales_objectives.update_sales_objective,
                approve_sales_objective=bundles.sales_objectives.approve_sales_objective,
            ),
            dashboard=DashboardRouteHandlers(
                DashboardMonthlyCloseUpsert=DashboardMonthlyCloseUpsert,
                get_dashboard_monthly_close=get_dashboard_monthly_close,
                get_dashboard_monthly_close_calendar=get_dashboard_monthly_close_calendar,
                upsert_dashboard_monthly_close=upsert_dashboard_monthly_close,
                get_dashboard_kpis=get_dashboard_kpis,
                get_sales_trends=get_sales_trends,
                get_seller_performance=get_seller_performance,
                get_vehicle_suggestions=get_vehicle_suggestions,
            ),
            financial_rates=FinancialRatesRouteHandlers(
                FinancialRateCreate=FinancialRateCreate,
                FinancialRateBulkApplyRequest=FinancialRateBulkApplyRequest,
                create_financial_rate=bundles.financial_rates.create_financial_rate,
                apply_group_default_financial_rate=bundles.financial_rates.apply_group_default_financial_rate,
                get_financial_rates=bundles.financial_rates.get_financial_rates,
                update_financial_rate=bundles.financial_rates.update_financial_rate,
                delete_financial_rate=bundles.financial_rates.delete_financial_rate,
            ),
            commissions=CommissionsRouteHandlers(
                CommissionMatrixUpsert=CommissionMatrixUpsert,
                CommissionRuleCreate=CommissionRuleCreate,
                CommissionApprovalAction=CommissionApprovalAction,
                CommissionSimulatorInput=CommissionSimulatorInput,
                CommissionClosureCreate=CommissionClosureCreate,
                CommissionClosureApprovalAction=CommissionClosureApprovalAction,
                get_commission_matrix=bundles.commissions.get_commission_matrix,
                upsert_commission_matrix=bundles.commissions.upsert_commission_matrix,
                create_commission_rule=bundles.commissions.create_commission_rule,
                get_commission_rules=bundles.commissions.get_commission_rules,
                update_commission_rule=bundles.commissions.update_commission_rule,
                approve_commission_rule=bundles.commissions.approve_commission_rule,
                delete_commission_rule=bundles.commissions.delete_commission_rule,
                commission_simulator=bundles.commissions.commission_simulator,
                create_commission_closure=bundles.commissions.create_commission_closure,
                get_commission_closures=bundles.commissions.get_commission_closures,
                approve_commission_closure=bundles.commissions.approve_commission_closure,
            ),
        ),
    )


__all__ = [
    "AppRouteHandlers",
    "RouteHandlerBundles",
    "register_all_route_modules",
]
