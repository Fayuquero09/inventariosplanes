from dotenv import load_dotenv
load_dotenv()

from fastapi import APIRouter, Request
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import os
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
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
from handlers.auth_users_handlers import build_auth_users_route_handlers
from handlers.app_runtime_helpers import (
    configure_cors,
    create_app,
    include_api_router,
    mount_brand_logos,
    run_shutdown,
    run_startup,
)
from handlers.app_config_helpers import build_app_config_helper_bundle
from handlers.auth_runtime_helpers import build_auth_runtime_helper_bundle
from handlers.catalog_handlers import build_catalog_route_handlers
from handlers.catalog_runtime_helpers import build_catalog_runtime_helper_bundle
from handlers.commission_catalog_helpers import build_commission_catalog_helper_bundle
from handlers.commissions_handlers import build_commissions_route_handlers
from handlers.core_helpers import build_core_helper_bundle
from handlers.dashboard_handlers import build_dashboard_route_handlers
from handlers.financial_rates_handlers import build_financial_rates_route_handlers
from handlers.import_handlers import build_import_route_handlers
from handlers.inventory_runtime_helpers import build_inventory_runtime_helper_bundle
from handlers.organization_catalog_handlers import build_organization_catalog_route_handlers
from handlers.pricing_financial_helpers import build_pricing_financial_helper_bundle
from handlers.price_bulletins_handlers import build_price_bulletins_route_handlers
from handlers.runtime_helpers import build_runtime_helper_bundle
from handlers.sales_handlers import build_sales_route_handlers
from handlers.sales_objectives_handlers import build_sales_objectives_route_handlers
from handlers.vehicles_handlers import build_vehicles_route_handlers
from schemas.api_models import (
    AgencyCreate,
    AgencyResponse,
    AuditLogResponse,
    BrandCreate,
    BrandResponse,
    CommissionApprovalAction,
    CommissionClosureApprovalAction,
    CommissionClosureCreate,
    CommissionMatrixGeneralConfig,
    CommissionMatrixModelConfig,
    CommissionMatrixUpsert,
    CommissionMatrixVolumeTierConfig,
    CommissionRuleCreate,
    CommissionRuleResponse,
    CommissionSimulatorInput,
    DashboardMonthlyCloseUpsert,
    FinancialRateBulkApplyRequest,
    FinancialRateCreate,
    FinancialRateResponse,
    GroupCreate,
    GroupResponse,
    PasswordResetRequest,
    PriceBulletinBulkUpsert,
    PriceBulletinItem,
    SaleCreate,
    SaleResponse,
    SalesObjectiveApprovalAction,
    SalesObjectiveCreate,
    SalesObjectiveResponse,
    UserCreate,
    UserLogin,
    UserResponse,
    UserRole,
    VehicleAgingIncentiveApply,
    VehicleCreate,
    VehicleResponse,
    VehicleSuggestion,
)
from repositories.commission_repository import (
    delete_commission_rule_by_id as _delete_commission_rule_by_id_repo,
    find_agency_by_id as _find_agency_by_id_commission_repo,
    find_brand_by_id as _find_brand_by_id_commission_repo,
    find_commission_closure_by_id as _find_commission_closure_by_id_repo,
    find_commission_closure_by_scope as _find_commission_closure_by_scope_repo,
    find_commission_matrix_by_agency as _find_commission_matrix_by_agency_repo,
    find_commission_rule_by_id as _find_commission_rule_by_id_repo,
    find_group_by_id as _find_group_by_id_commission_repo,
    find_user_by_id as _find_user_by_id_commission_repo,
    insert_commission_closure as _insert_commission_closure_repo,
    insert_commission_rule as _insert_commission_rule_repo,
    list_active_rules_by_agency,
    list_commission_closures as _list_commission_closures_repo,
    list_commission_rules as _list_commission_rules_repo,
    list_sales_for_closure as _list_sales_for_closure_repo,
    update_commission_closure_by_id as _update_commission_closure_by_id_repo,
    update_commission_rule_by_id as _update_commission_rule_by_id_repo,
    upsert_commission_matrix_by_agency as _upsert_commission_matrix_by_agency_repo,
)
from repositories.dashboard_repository import (
    count_sales as _count_sales_dashboard_repo,
    count_users as _count_users_dashboard_repo,
    find_agency_group_id,
    find_brand_group_id,
    find_user_by_id as _find_user_by_id_dashboard_repo,
    find_monthly_close,
    list_agencies_by_brand_id as _list_agencies_by_brand_id_dashboard_repo,
    list_agencies_by_group_id as _list_agencies_by_group_id_dashboard_repo,
    list_global_monthly_closes_by_year,
    list_sales as _list_sales_dashboard_repo,
    list_sales_objectives as _list_sales_objectives_dashboard_repo,
    list_similar_sold_vehicles as _list_similar_sold_vehicles_dashboard_repo,
    list_vehicles as _list_vehicles_dashboard_repo,
    list_vehicles_by_ids as _list_vehicles_by_ids_dashboard_repo,
    upsert_global_monthly_close,
)
from repositories.financial_rates_repository import (
    delete_financial_rate_by_id as _delete_financial_rate_by_id_repo,
    find_financial_rate_by_id as _find_financial_rate_by_id_repo,
    find_latest_financial_rate as _find_latest_financial_rate_repo,
    insert_financial_rate as _insert_financial_rate_repo,
    insert_many_financial_rates as _insert_many_financial_rates_repo,
    list_brand_financial_rates_for_group as _list_brand_financial_rates_for_group_repo,
    list_brands_for_group as _list_brands_for_group_repo,
    list_financial_rates as _list_financial_rates_repo,
    update_financial_rate_by_id as _update_financial_rate_by_id_repo,
)
from repositories.organization_repository import (
    delete_brand_by_id,
    delete_group_by_id,
    find_agency_by_id,
    find_brand_by_id,
    find_group_by_id,
    insert_agency,
    insert_brand,
    insert_group,
    list_agencies,
    list_brands,
    list_brands_by_ids,
    list_groups,
    update_agency_by_id,
    update_brand_by_id,
    update_group_by_id,
)
from repositories.sales_repository import find_vehicle_by_id as find_sales_vehicle_by_id
from repositories.sales_objectives_repository import (
    find_agency_by_id as _find_agency_by_id_sales_objectives_repo,
    find_brand_by_id as _find_brand_by_id_sales_objectives_repo,
    find_group_by_id as _find_group_by_id_sales_objectives_repo,
    find_user_by_id as _find_user_by_id_sales_objectives_repo,
    list_price_bulletins as _list_price_bulletins_sales_objectives_repo,
    list_sales as _list_sales_sales_objectives_repo,
    list_sales_objectives as _list_sales_objectives_repo,
)
from repositories.user_repository import (
    create_user,
    delete_user_by_id,
    find_user_by_email,
    find_user_by_id,
    list_audit_logs,
    list_users,
    update_user_by_id,
    update_user_password_hash,
)
from services.commission_service import calculate_commission_from_rules as _calculate_commission_from_rules
from services.commission_calculation_service import (
    calculate_commission as _calculate_commission_service,
)
from services.commission_management_service import (
    build_commission_approval_update_fields as _build_commission_approval_update_fields_service,
    build_commission_closure_doc as _build_commission_closure_doc_service,
    build_commission_closure_snapshot as _build_commission_closure_snapshot_service,
    build_commission_matrix_upsert_fields as _build_commission_matrix_upsert_fields_service,
    build_commission_rule_doc as _build_commission_rule_doc_service,
    build_commission_rule_update_fields as _build_commission_rule_update_fields_service,
    build_commission_simulator_projection as _build_commission_simulator_projection_service,
    build_matrix_models_response as _build_matrix_models_response_service,
    build_month_bounds as _build_month_bounds_service,
    get_catalog_models_for_brand as _get_catalog_models_for_brand_service,
    normalize_commission_matrix_general as _normalize_commission_matrix_general_service,
    normalize_commission_matrix_models as _normalize_commission_matrix_models_service,
    normalize_commission_matrix_volume_tiers as _normalize_commission_matrix_volume_tiers_service,
    normalize_commission_status as _normalize_commission_status_service,
    resolve_matrix_volume_bonus_per_unit as _resolve_matrix_volume_bonus_per_unit_service,
    sale_commission_base_price as _sale_commission_base_price_service,
    sale_effective_revenue as _sale_effective_revenue_service,
    to_non_negative_float as _to_non_negative_float_service,
)
from services.financial_rates_service import (
    build_financial_rate_record as _build_financial_rate_record_service,
    build_financial_rate_update_fields as _build_financial_rate_update_fields_service,
    build_default_financial_rate_name as _build_default_financial_rate_name_service,
    enrich_financial_rate as _enrich_financial_rate_service,
    extract_rate_components_from_doc as _extract_rate_components_from_doc_service,
    monthly_to_annual as _monthly_to_annual_service,
    plan_group_default_rate_docs as _plan_group_default_rate_docs_service,
    resolve_effective_rate_components as _resolve_effective_rate_components_service,
)
from services.dashboard_service import (
    build_dashboard_monthly_close_calendar as _build_dashboard_monthly_close_calendar_service,
    build_dashboard_monthly_close_response as _build_dashboard_monthly_close_response_service,
    build_vehicle_aging_suggestion as _build_vehicle_aging_suggestion_service,
    collect_vehicle_suggestions as _collect_vehicle_suggestions_service,
    compute_dashboard_kpis as _compute_dashboard_kpis_service,
    compute_sales_trends as _compute_sales_trends_service,
    compute_seller_performance as _compute_seller_performance_service,
    empty_dashboard_kpis_response as _empty_dashboard_kpis_response_service,
    resolve_dashboard_scope_group_id as _resolve_dashboard_scope_group_id_service,
)
from services.import_service import (
    import_organization_from_excel,
    import_sales_from_file,
    import_vehicles_from_file,
)
from services.logo_assets_service import (
    resolve_logo_directory as _resolve_logo_directory_service,
    resolve_logo_url_for_brand as _resolve_logo_url_for_brand_service,
)
from services.sales_objectives_service import (
    build_sales_objective_suggestion as _build_sales_objective_suggestion_service,
    list_sales_objectives_with_progress as _list_sales_objectives_with_progress_service,
)
from services.operational_calendar_service import (
    add_months_ym as _add_months_ym,
    compute_operational_day_profile as _compute_operational_day_profile,
    decrement_month as _decrement_month,
    mexico_lft_holidays_by_month as _mexico_lft_holidays_by_month,
    resolve_effective_objective_units as _resolve_effective_objective_units,
)
from services.organization_cleanup_service import (
    build_brand_delete_context,
    build_group_delete_context,
    execute_brand_cascade_delete,
    execute_group_cascade_delete,
    format_dependency_messages,
    summarize_brand_dependencies,
    summarize_group_dependencies,
)
from services.pricing_service import (
    apply_manual_sale_price_override as _apply_manual_sale_price_override_service,
    is_price_bulletin_active as _is_price_bulletin_active_service,
    price_item_applies_to_sale as _price_item_applies_to_sale_service,
    resolve_effective_sale_pricing_for_model as _resolve_effective_sale_pricing_for_model_service,
)
from services.price_bulletins_service import (
    list_price_bulletins_with_enrichment as _list_price_bulletins_with_enrichment_service,
    normalize_iso_date_string as _normalize_iso_date_string_service,
    remove_price_bulletin as _remove_price_bulletin_service,
    reprice_sales_for_price_bulletin as _reprice_sales_for_price_bulletin_service,
    resolve_price_bulletin_scope as _resolve_price_bulletin_scope_service,
    upsert_price_bulletins_items as _upsert_price_bulletins_items_service,
)
from services.sales_service import create_sale_record, list_sales_with_enrichment
from services.auth_users_service import (
    build_audit_logs_query_for_actor as _build_audit_logs_query_for_actor_service,
    build_users_query_for_actor as _build_users_query_for_actor_service,
    google_auth_flow as _google_auth_flow_service,
    login_user as _login_user_service,
    reset_password_flow as _reset_password_flow_service,
    resolve_register_hierarchy_scope as _resolve_register_hierarchy_scope_service,
)
from services.auth_session_service import (
    create_access_token as _create_access_token_service,
    create_refresh_token as _create_refresh_token_service,
    get_current_user as _get_current_user_service,
    get_optional_user as _get_optional_user_service,
    hash_password as _hash_password_service,
    verify_password as _verify_password_service,
)
from services.agency_location_service import (
    backfill_agency_locations as _backfill_agency_locations_service,
    compose_structured_agency_address as _compose_structured_agency_address_service,
    merge_optional_float as _merge_optional_float_service,
    merge_optional_text as _merge_optional_text_service,
    resolve_agency_location as _resolve_agency_location_service,
)
from services.financial_cost_service import (
    calculate_vehicle_financial_cost as _calculate_vehicle_financial_cost_service,
    calculate_vehicle_financial_cost_in_period as _calculate_vehicle_financial_cost_in_period_service,
    coerce_utc_datetime as _coerce_utc_datetime_service,
)
from services.vehicle_enrichment_service import enrich_vehicle as _enrich_vehicle_service
from services.catalog_utils_service import (
    find_catalog_make as _find_catalog_make_service,
    find_catalog_model as _find_catalog_model_service,
    normalize_catalog_text as _normalize_catalog_text_service,
    parse_catalog_price as _parse_catalog_price_service,
    parse_catalog_year as _parse_catalog_year_service,
)
from services.catalog_service import (
    build_catalog_tree_from_source as _build_catalog_tree_from_source_service,
    ensure_allowed_model_year as _ensure_allowed_model_year_service,
    get_catalog_model_year as _get_catalog_model_year_service,
    get_catalog_source_path as _get_catalog_source_path_service,
)
from services.cors_service import build_allowed_origins as _build_allowed_origins_service
from services.bootstrap_service import (
    create_core_indexes as _create_core_indexes_service,
    seed_admin_user as _seed_admin_user_service,
)
from services.rbac_service import (
    ACTION_AUDIT_LOGS_READ,
    ACTION_USERS_MANAGE,
    AGENCY_SCOPED_ROLES,
    APP_LEVEL_ROLES,
    BRAND_SCOPED_ROLES,
    COMMISSION_APPROVER_ROLES,
    COMMISSION_MATRIX_EDITOR_ROLES,
    COMMISSION_PROPOSER_ROLES,
    CORP_FINANCE_ROLES,
    CORP_STRUCTURE_ROLES,
    DEALER_GENERAL_ASSIGNABLE_ROLES,
    DEALER_GENERAL_EFFECTIVE_ROLES,
    DEALER_LEGACY_READONLY_ROLE,
    DEALER_SALES_ASSIGNABLE_ROLES,
    DEALER_SALES_EFFECTIVE_ROLES,
    DEALER_SELLER_ROLE,
    DEALER_USER_MANAGER_ROLES,
    FINANCIAL_RATE_MANAGER_ROLES,
    OBJECTIVE_APPROVER_ROLES,
    OBJECTIVE_EDITOR_ROLES,
    PRICE_BULLETIN_EDITOR_ROLES,
    WRITE_AUDIT_ROLES,
    build_scope_query as _build_scope_query,
    ensure_doc_scope_access as _ensure_doc_scope_access,
    get_dealer_assignable_roles as _get_dealer_assignable_roles,
    is_agency_scoped_role as _is_agency_scoped_role,
    is_app_level_role as _is_app_level_role,
    is_brand_scoped_role as _is_brand_scoped_role,
    is_corp_finance_role as _is_corp_finance_role,
    is_corp_structure_role as _is_corp_structure_role,
    is_dealer_general_effective_role as _is_dealer_general_effective_role,
    is_dealer_sales_effective_role as _is_dealer_sales_effective_role,
    is_dealer_user_manager_role as _is_dealer_user_manager_role,
    require_action_role as _require_action_role,
    same_scope_id as _same_scope_id,
    scope_query_has_access as _scope_query_has_access,
    validate_scope_filters as _validate_scope_filters,
)
from services.user_management_service import (
    apply_register_scope_constraints as _apply_register_scope_constraints_service,
    build_user_document as _build_user_document_service,
    build_user_update_audit_changes as _build_user_update_audit_changes_service,
    enforce_delete_scope_permissions as _enforce_delete_scope_permissions_service,
    enforce_update_scope_permissions as _enforce_update_scope_permissions_service,
    extract_new_password_and_payload as _extract_new_password_and_payload_service,
    normalize_optional_position as _normalize_optional_position_service,
    normalize_user_email as _normalize_user_email_service,
    sanitize_user_update_data as _sanitize_user_update_data_service,
    validate_role_scope_requirements as _validate_role_scope_requirements_service,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# JWT Configuration
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours
REFRESH_TOKEN_EXPIRE_DAYS = 7

# External vehicle catalog (Strapi/JATO)
CATALOG_DEFAULT_SOURCE_PATH = "/Users/Fernando.Molina/cortex-automotriz/strapi/data/jato/latest-jato.es-mx.all-2026-2024.json"
CATALOG_DEFAULT_MODEL_YEAR = 2026
CORTEX_ROOT_DEFAULT_PATH = "/Users/Fernando.Molina/cortex-automotriz"
LOGO_DIRECTORY_ENV = "STRAPI_LOGOS_DIR"
_app_config_helpers = build_app_config_helper_bundle(
    env=os.environ,
    jwt_secret_default="default-secret-change-me",
    catalog_default_source_path=CATALOG_DEFAULT_SOURCE_PATH,
    catalog_default_model_year=CATALOG_DEFAULT_MODEL_YEAR,
    logo_directory_env=LOGO_DIRECTORY_ENV,
    cortex_root_default_path=CORTEX_ROOT_DEFAULT_PATH,
    get_catalog_source_path_service=_get_catalog_source_path_service,
    get_catalog_model_year_service=_get_catalog_model_year_service,
    resolve_logo_url_for_brand_service=_resolve_logo_url_for_brand_service,
    resolve_logo_directory_service=_resolve_logo_directory_service,
)
get_jwt_secret = _app_config_helpers.get_jwt_secret
get_catalog_source_path = _app_config_helpers.get_catalog_source_path
get_catalog_model_year = _app_config_helpers.get_catalog_model_year
_resolve_logo_url_for_brand = _app_config_helpers.resolve_logo_url_for_brand
_resolve_logo_directory = _app_config_helpers.resolve_logo_directory

# Auth runtime helpers (hashing, token generation, current user resolution)
_auth_runtime_helper_bundle = build_auth_runtime_helper_bundle(
    db=db,
    get_jwt_secret=get_jwt_secret,
    jwt_algorithm=JWT_ALGORITHM,
    access_token_expires_minutes=ACCESS_TOKEN_EXPIRE_MINUTES,
    refresh_token_expires_days=REFRESH_TOKEN_EXPIRE_DAYS,
    hash_password_service=_hash_password_service,
    verify_password_service=_verify_password_service,
    create_access_token_service=_create_access_token_service,
    create_refresh_token_service=_create_refresh_token_service,
    get_current_user_service=_get_current_user_service,
    get_optional_user_service=_get_optional_user_service,
)
hash_password = _auth_runtime_helper_bundle.hash_password
verify_password = _auth_runtime_helper_bundle.verify_password
create_access_token = _auth_runtime_helper_bundle.create_access_token
create_refresh_token = _auth_runtime_helper_bundle.create_refresh_token
get_current_user = _auth_runtime_helper_bundle.get_current_user
get_optional_user = _auth_runtime_helper_bundle.get_optional_user

# Create the main app
app = create_app(
    title="AutoConnect - Vehicle Inventory Management",
    session_secret=get_jwt_secret(),
)

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

COMMISSION_PENDING = "pending"
COMMISSION_APPROVED = "approved"
COMMISSION_REJECTED = "rejected"
COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT = 100.0

OBJECTIVE_PENDING = "pending"
OBJECTIVE_DRAFT = "draft"
OBJECTIVE_APPROVED = "approved"
OBJECTIVE_REJECTED = "rejected"

_core_helper_bundle = build_core_helper_bundle(
    db=db,
    object_id_cls=ObjectId,
    write_audit_roles=WRITE_AUDIT_ROLES,
)
serialize_doc = _core_helper_bundle.serialize_doc
log_audit_event = _core_helper_bundle.log_audit_event

_catalog_runtime_helper_bundle = build_catalog_runtime_helper_bundle(
    db=db,
    get_catalog_source_path=get_catalog_source_path,
    get_catalog_model_year=get_catalog_model_year,
    normalize_catalog_text_service=_normalize_catalog_text_service,
    parse_catalog_year_service=_parse_catalog_year_service,
    parse_catalog_price_service=_parse_catalog_price_service,
    resolve_agency_location_service=_resolve_agency_location_service,
    compose_structured_agency_address_service=_compose_structured_agency_address_service,
    merge_optional_text_service=_merge_optional_text_service,
    merge_optional_float_service=_merge_optional_float_service,
    backfill_agency_locations_service=_backfill_agency_locations_service,
    build_catalog_tree_from_source_service=_build_catalog_tree_from_source_service,
    find_catalog_make_service=_find_catalog_make_service,
    find_catalog_model_service=_find_catalog_model_service,
    ensure_allowed_model_year_service=_ensure_allowed_model_year_service,
)
_normalize_catalog_text = _catalog_runtime_helper_bundle.normalize_catalog_text
_parse_catalog_year = _catalog_runtime_helper_bundle.parse_catalog_year
_parse_catalog_price = _catalog_runtime_helper_bundle.parse_catalog_price
_resolve_agency_location = _catalog_runtime_helper_bundle.resolve_agency_location
_compose_structured_agency_address = _catalog_runtime_helper_bundle.compose_structured_agency_address
_merge_optional_text = _catalog_runtime_helper_bundle.merge_optional_text
_merge_optional_float = _catalog_runtime_helper_bundle.merge_optional_float
backfill_agency_locations = _catalog_runtime_helper_bundle.backfill_agency_locations
_build_catalog_tree_from_source = _catalog_runtime_helper_bundle.build_catalog_tree_from_source
_find_catalog_make = _catalog_runtime_helper_bundle.find_catalog_make
_find_catalog_model = _catalog_runtime_helper_bundle.find_catalog_model
_ensure_allowed_model_year = _catalog_runtime_helper_bundle.ensure_allowed_model_year

# ============== PRICING / FINANCIAL HELPER BUNDLE ==============

_pricing_financial_helper_bundle = build_pricing_financial_helper_bundle(
    db=db,
    object_id_cls=ObjectId,
    validate_scope_filters=_validate_scope_filters,
    normalize_iso_date_string_service=_normalize_iso_date_string_service,
    resolve_effective_sale_pricing_for_model_service=_resolve_effective_sale_pricing_for_model_service,
    apply_manual_sale_price_override_service=_apply_manual_sale_price_override_service,
    to_non_negative_float=_to_non_negative_float_service,
    monthly_to_annual_service=_monthly_to_annual_service,
    extract_rate_components_from_doc_service=_extract_rate_components_from_doc_service,
    resolve_effective_rate_components_service=_resolve_effective_rate_components_service,
    find_latest_financial_rate=_find_latest_financial_rate_repo,
    build_default_financial_rate_name_service=_build_default_financial_rate_name_service,
    find_group_by_id=find_group_by_id,
    find_brand_by_id=find_brand_by_id,
    find_agency_by_id=find_agency_by_id,
)
_resolve_financial_rate_scope = _pricing_financial_helper_bundle.resolve_financial_rate_scope
_normalize_iso_date_string = _pricing_financial_helper_bundle.normalize_iso_date_string
_resolve_effective_sale_pricing_for_model = _pricing_financial_helper_bundle.resolve_effective_sale_pricing_for_model
_apply_manual_sale_price_override = _pricing_financial_helper_bundle.apply_manual_sale_price_override
_resolve_effective_transaction_price_for_model = _pricing_financial_helper_bundle.resolve_effective_transaction_price_for_model
_monthly_to_annual = _pricing_financial_helper_bundle.monthly_to_annual
_extract_rate_components_from_doc = _pricing_financial_helper_bundle.extract_rate_components_from_doc
_resolve_effective_rate_components_for_scope = _pricing_financial_helper_bundle.resolve_effective_rate_components_for_scope
_resolve_effective_rate_components_for_vehicle = _pricing_financial_helper_bundle.resolve_effective_rate_components_for_vehicle
_build_default_financial_rate_name = _pricing_financial_helper_bundle.build_default_financial_rate_name

# ============== AUTH USERS ROUTE HANDLERS ==============

_auth_users_route_handlers = build_auth_users_route_handlers(
    db=db,
    object_id_cls=ObjectId,
    get_current_user=get_current_user,
    app_admin_role=UserRole.APP_ADMIN,
    app_user_role=UserRole.APP_USER,
    group_admin_role=UserRole.GROUP_ADMIN,
    group_finance_role=UserRole.GROUP_FINANCE_MANAGER,
    seller_role=UserRole.SELLER,
    brand_admin_role=UserRole.BRAND_ADMIN,
    brand_user_role=UserRole.BRAND_USER,
    agency_admin_role=UserRole.AGENCY_ADMIN,
    agency_sales_manager_role=UserRole.AGENCY_SALES_MANAGER,
    agency_general_manager_role=UserRole.AGENCY_GENERAL_MANAGER,
    agency_commercial_manager_role=UserRole.AGENCY_COMMERCIAL_MANAGER,
    agency_user_role=UserRole.AGENCY_USER,
    action_users_manage=ACTION_USERS_MANAGE,
    action_audit_logs_read=ACTION_AUDIT_LOGS_READ,
    require_action_role=_require_action_role,
    is_dealer_user_manager_role=_is_dealer_user_manager_role,
    get_dealer_assignable_roles=_get_dealer_assignable_roles,
    same_scope_id=_same_scope_id,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    apply_register_scope_constraints=_apply_register_scope_constraints_service,
    normalize_user_email=_normalize_user_email_service,
    resolve_register_hierarchy_scope=_resolve_register_hierarchy_scope_service,
    validate_role_scope_requirements=_validate_role_scope_requirements_service,
    normalize_optional_position=_normalize_optional_position_service,
    build_user_document=_build_user_document_service,
    find_user_by_email=find_user_by_email,
    create_user=create_user,
    hash_password=hash_password,
    login_user=_login_user_service,
    verify_password=verify_password,
    create_access_token=create_access_token,
    create_refresh_token=create_refresh_token,
    reset_password_flow=_reset_password_flow_service,
    google_auth_flow=_google_auth_flow_service,
    build_users_query_for_actor=_build_users_query_for_actor_service,
    list_users=list_users,
    extract_new_password_and_payload=_extract_new_password_and_payload_service,
    sanitize_user_update_data=_sanitize_user_update_data_service,
    find_user_by_id=find_user_by_id,
    enforce_update_scope_permissions=_enforce_update_scope_permissions_service,
    update_user_by_id=update_user_by_id,
    build_user_update_audit_changes=_build_user_update_audit_changes_service,
    enforce_delete_scope_permissions=_enforce_delete_scope_permissions_service,
    delete_user_by_id=delete_user_by_id,
    build_audit_logs_query_for_actor=_build_audit_logs_query_for_actor_service,
    list_audit_logs=list_audit_logs,
    find_agency_by_id=find_agency_by_id,
    serialize_doc=serialize_doc,
    log_audit_event=log_audit_event,
    update_user_password_hash=update_user_password_hash,
    find_brand_by_id=find_brand_by_id,
    find_agency_by_id_register=find_agency_by_id,
)
register = _auth_users_route_handlers.register
login = _auth_users_route_handlers.login
logout = _auth_users_route_handlers.logout
reset_password = _auth_users_route_handlers.reset_password
get_me = _auth_users_route_handlers.get_me
google_auth = _auth_users_route_handlers.google_auth
get_users = _auth_users_route_handlers.get_users
update_user = _auth_users_route_handlers.update_user
delete_user = _auth_users_route_handlers.delete_user
get_audit_logs = _auth_users_route_handlers.get_audit_logs
get_sellers = _auth_users_route_handlers.get_sellers

# ============== VEHICLE CATALOG ROUTES ==============

_catalog_route_handlers = build_catalog_route_handlers(
    get_current_user=get_current_user,
    build_catalog_tree_from_source=_build_catalog_tree_from_source,
    find_catalog_make=_find_catalog_make,
    find_catalog_model=_find_catalog_model,
    parse_catalog_price=_parse_catalog_price,
    resolve_logo_url_for_brand=_resolve_logo_url_for_brand,
)
get_catalog_makes = _catalog_route_handlers.get_catalog_makes
get_catalog_models = _catalog_route_handlers.get_catalog_models
get_catalog_versions = _catalog_route_handlers.get_catalog_versions

# ============== ORGANIZATION ROUTE HANDLERS ==============

_organization_catalog_route_handlers = build_organization_catalog_route_handlers(
    db=db,
    get_current_user=get_current_user,
    app_admin_role=UserRole.APP_ADMIN,
    app_user_role=UserRole.APP_USER,
    group_admin_role=UserRole.GROUP_ADMIN,
    brand_admin_role=UserRole.BRAND_ADMIN,
    agency_admin_role=UserRole.AGENCY_ADMIN,
    serialize_doc=serialize_doc,
    object_id_cls=ObjectId,
    insert_group=insert_group,
    list_groups=list_groups,
    find_group_by_id=find_group_by_id,
    update_group_by_id=update_group_by_id,
    delete_group_by_id=delete_group_by_id,
    build_group_delete_context=build_group_delete_context,
    summarize_group_dependencies=summarize_group_dependencies,
    format_dependency_messages=format_dependency_messages,
    execute_group_cascade_delete=execute_group_cascade_delete,
    insert_brand=insert_brand,
    list_brands=list_brands,
    find_brand_by_id=find_brand_by_id,
    update_brand_by_id=update_brand_by_id,
    delete_brand_by_id=delete_brand_by_id,
    build_brand_delete_context=build_brand_delete_context,
    summarize_brand_dependencies=summarize_brand_dependencies,
    execute_brand_cascade_delete=execute_brand_cascade_delete,
    insert_agency=insert_agency,
    list_agencies=list_agencies,
    list_brands_by_ids=list_brands_by_ids,
    find_agency_by_id=find_agency_by_id,
    update_agency_by_id=update_agency_by_id,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    same_scope_id=_same_scope_id,
    is_brand_scoped_role=_is_brand_scoped_role,
    is_agency_scoped_role=_is_agency_scoped_role,
    resolve_logo_url_for_brand=_resolve_logo_url_for_brand,
    normalize_catalog_text=_normalize_catalog_text,
    resolve_agency_location=_resolve_agency_location,
    compose_structured_agency_address=_compose_structured_agency_address,
    merge_optional_text=_merge_optional_text,
    merge_optional_float=_merge_optional_float,
    log_audit_event=log_audit_event,
)
create_group = _organization_catalog_route_handlers.create_group
get_groups = _organization_catalog_route_handlers.get_groups
get_group = _organization_catalog_route_handlers.get_group
update_group = _organization_catalog_route_handlers.update_group
delete_group = _organization_catalog_route_handlers.delete_group
create_brand = _organization_catalog_route_handlers.create_brand
get_brands = _organization_catalog_route_handlers.get_brands
update_brand = _organization_catalog_route_handlers.update_brand
delete_brand = _organization_catalog_route_handlers.delete_brand
create_agency = _organization_catalog_route_handlers.create_agency
get_agencies = _organization_catalog_route_handlers.get_agencies
update_agency = _organization_catalog_route_handlers.update_agency

# ============== FINANCIAL RATES ROUTES ==============

DAYS_PER_MONTH_FOR_RATE = 30


# ============== VEHICLES ROUTES ==============

_coerce_utc_datetime = _coerce_utc_datetime_service


_inventory_runtime_helper_bundle = build_inventory_runtime_helper_bundle(
    db=db,
    object_id_cls=ObjectId,
    days_per_month_for_rate=DAYS_PER_MONTH_FOR_RATE,
    resolve_effective_rate_components_for_vehicle=_resolve_effective_rate_components_for_vehicle,
    calculate_vehicle_financial_cost_in_period_service=_calculate_vehicle_financial_cost_in_period_service,
    calculate_vehicle_financial_cost_service=_calculate_vehicle_financial_cost_service,
    enrich_vehicle_service=_enrich_vehicle_service,
    serialize_doc=serialize_doc,
    sale_effective_revenue=_sale_effective_revenue_service,
)
calculate_vehicle_financial_cost_in_period = _inventory_runtime_helper_bundle.calculate_vehicle_financial_cost_in_period
calculate_vehicle_financial_cost = _inventory_runtime_helper_bundle.calculate_vehicle_financial_cost
enrich_vehicle = _inventory_runtime_helper_bundle.enrich_vehicle

# ============== COMMISSION RULES ROUTES ==============

_to_non_negative_float = _to_non_negative_float_service
_sale_effective_revenue = _sale_effective_revenue_service
_sale_commission_base_price = _sale_commission_base_price_service
_normalize_commission_matrix_volume_tiers = _normalize_commission_matrix_volume_tiers_service
_normalize_commission_matrix_general = _normalize_commission_matrix_general_service

_commission_catalog_helper_bundle = build_commission_catalog_helper_bundle(
    default_plant_share_pct=COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT,
    normalize_commission_matrix_models_service=_normalize_commission_matrix_models_service,
    get_catalog_models_for_brand_service=_get_catalog_models_for_brand_service,
    build_matrix_models_response_service=_build_matrix_models_response_service,
    build_catalog_tree_from_source=_build_catalog_tree_from_source,
    find_catalog_make=_find_catalog_make,
    parse_catalog_price=_parse_catalog_price,
)
_normalize_commission_matrix_models = _commission_catalog_helper_bundle.normalize_commission_matrix_models
_get_catalog_models_for_brand = _commission_catalog_helper_bundle.get_catalog_models_for_brand
_build_matrix_models_response = _commission_catalog_helper_bundle.build_matrix_models_response

_resolve_matrix_volume_bonus_per_unit = _resolve_matrix_volume_bonus_per_unit_service

# ============== RUNTIME HELPER BUNDLE ==============

_runtime_helper_bundle = build_runtime_helper_bundle(
    db=db,
    calculate_commission_service=_calculate_commission_service,
    commission_approved=COMMISSION_APPROVED,
    normalize_general=_normalize_commission_matrix_general,
    normalize_models=_normalize_commission_matrix_models,
    resolve_volume_bonus_per_unit=_resolve_matrix_volume_bonus_per_unit,
    to_non_negative_float=_to_non_negative_float,
    sale_commission_base_price=_sale_commission_base_price,
    coerce_utc_datetime=_coerce_utc_datetime,
    default_plant_share_pct=COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT,
    resolve_dashboard_scope_group_id_service=_resolve_dashboard_scope_group_id_service,
    find_brand_group_id=find_brand_group_id,
    find_agency_group_id=find_agency_group_id,
    find_monthly_close=find_monthly_close,
    enrich_vehicle=enrich_vehicle,
    build_vehicle_aging_suggestion_service=_build_vehicle_aging_suggestion_service,
    list_similar_sold_vehicles=_list_similar_sold_vehicles_dashboard_repo,
)
calculate_commission = _runtime_helper_bundle.calculate_commission
_extract_active_aging_incentive_plan = _runtime_helper_bundle.extract_active_aging_incentive_plan
_apply_aging_plan_to_effective_pricing = _runtime_helper_bundle.apply_aging_plan_to_effective_pricing
_resolve_dashboard_scope_group_id = _runtime_helper_bundle.resolve_dashboard_scope_group_id
_find_dashboard_monthly_close = _runtime_helper_bundle.find_dashboard_monthly_close
_build_vehicle_aging_suggestion = _runtime_helper_bundle.build_vehicle_aging_suggestion

# ============== IMPORT ROUTE HANDLERS ==============

_import_organization_roles = [
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
]
_import_vehicle_sales_roles = [
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_ADMIN,
]
_import_route_handlers = build_import_route_handlers(
    db=db,
    get_current_user=get_current_user,
    import_organization_roles=_import_organization_roles,
    import_vehicle_sales_roles=_import_vehicle_sales_roles,
    import_organization_from_excel=import_organization_from_excel,
    import_vehicles_from_file=import_vehicles_from_file,
    import_sales_from_file=import_sales_from_file,
    resolve_agency_location=_resolve_agency_location,
    hash_password=hash_password,
    get_catalog_model_year=get_catalog_model_year,
    calculate_commission=calculate_commission,
    resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
    apply_manual_sale_price_override=_apply_manual_sale_price_override,
    extract_active_aging_incentive_plan=_extract_active_aging_incentive_plan,
    apply_aging_plan_to_effective_pricing=_apply_aging_plan_to_effective_pricing,
    to_non_negative_float=_to_non_negative_float,
    log_audit_event=log_audit_event,
)
import_organization = _import_route_handlers.import_organization
import_vehicles = _import_route_handlers.import_vehicles
import_sales = _import_route_handlers.import_sales

# ============== VEHICLES ROUTE HANDLERS ==============

_vehicle_editor_roles = [
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_ADMIN,
]
_vehicle_aging_incentive_roles = [
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.GROUP_FINANCE_MANAGER,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_ADMIN,
    UserRole.AGENCY_GENERAL_MANAGER,
    UserRole.AGENCY_SALES_MANAGER,
]
_vehicles_route_handlers = build_vehicles_route_handlers(
    db=db,
    get_current_user=get_current_user,
    vehicle_editor_roles=_vehicle_editor_roles,
    vehicle_aging_incentive_roles=_vehicle_aging_incentive_roles,
    ensure_allowed_model_year=_ensure_allowed_model_year,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    log_audit_event=log_audit_event,
    enrich_vehicle=enrich_vehicle,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    object_id_cls=ObjectId,
    build_vehicle_aging_suggestion=_build_vehicle_aging_suggestion,
    to_non_negative_float=_to_non_negative_float,
)
create_vehicle = _vehicles_route_handlers.create_vehicle
get_vehicles = _vehicles_route_handlers.get_vehicles
get_vehicle = _vehicles_route_handlers.get_vehicle
apply_vehicle_aging_incentive = _vehicles_route_handlers.apply_vehicle_aging_incentive
update_vehicle = _vehicles_route_handlers.update_vehicle

# ============== FINANCIAL RATES ROUTE HANDLERS ==============

_financial_rates_route_handlers = build_financial_rates_route_handlers(
    db=db,
    get_current_user=get_current_user,
    financial_rate_manager_roles=FINANCIAL_RATE_MANAGER_ROLES,
    agency_scoped_roles=AGENCY_SCOPED_ROLES,
    resolve_financial_rate_scope=_resolve_financial_rate_scope,
    build_default_financial_rate_name=_build_default_financial_rate_name,
    build_financial_rate_record=_build_financial_rate_record_service,
    monthly_to_annual=_monthly_to_annual,
    insert_financial_rate_by_doc=_insert_financial_rate_repo,
    log_audit_event=log_audit_event,
    serialize_doc=serialize_doc,
    find_latest_financial_rate=_find_latest_financial_rate_repo,
    list_brands_for_group=_list_brands_for_group_repo,
    list_brand_financial_rates_for_group=_list_brand_financial_rates_for_group_repo,
    find_group_by_id=find_group_by_id,
    plan_group_default_rate_docs=_plan_group_default_rate_docs_service,
    extract_rate_components_from_doc=_extract_rate_components_from_doc,
    insert_many_financial_rates=_insert_many_financial_rates_repo,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    list_financial_rates=_list_financial_rates_repo,
    enrich_financial_rate=_enrich_financial_rate_service,
    resolve_effective_rate_components_for_scope=_resolve_effective_rate_components_for_scope,
    find_brand_by_id=find_brand_by_id,
    find_agency_by_id=find_agency_by_id,
    object_id_cls=ObjectId,
    find_financial_rate_by_id=_find_financial_rate_by_id_repo,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    build_financial_rate_update_fields=_build_financial_rate_update_fields_service,
    update_financial_rate_by_id=_update_financial_rate_by_id_repo,
    delete_financial_rate_by_id=_delete_financial_rate_by_id_repo,
)
create_financial_rate = _financial_rates_route_handlers.create_financial_rate
apply_group_default_financial_rate = _financial_rates_route_handlers.apply_group_default_financial_rate
get_financial_rates = _financial_rates_route_handlers.get_financial_rates
update_financial_rate = _financial_rates_route_handlers.update_financial_rate
delete_financial_rate = _financial_rates_route_handlers.delete_financial_rate

# ============== PRICE BULLETINS ROUTE HANDLERS ==============

_price_bulletins_route_handlers = build_price_bulletins_route_handlers(
    db=db,
    get_current_user=get_current_user,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    list_price_bulletins_with_enrichment=_list_price_bulletins_with_enrichment_service,
    serialize_doc=serialize_doc,
    is_price_bulletin_active=_is_price_bulletin_active_service,
    price_bulletin_editor_roles=PRICE_BULLETIN_EDITOR_ROLES,
    resolve_price_bulletin_scope=_resolve_price_bulletin_scope_service,
    normalize_iso_date_string=_normalize_iso_date_string,
    upsert_price_bulletins_items=_upsert_price_bulletins_items_service,
    reprice_sales_for_price_bulletin=_reprice_sales_for_price_bulletin_service,
    price_item_applies_to_sale=_price_item_applies_to_sale_service,
    resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
    apply_manual_sale_price_override=_apply_manual_sale_price_override,
    calculate_commission=calculate_commission,
    to_non_negative_float=_to_non_negative_float,
    coerce_utc_datetime=_coerce_utc_datetime,
    log_audit_event=log_audit_event,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    remove_price_bulletin=_remove_price_bulletin_service,
    object_id_cls=ObjectId,
)
get_price_bulletins = _price_bulletins_route_handlers.get_price_bulletins
upsert_price_bulletins_bulk = _price_bulletins_route_handlers.upsert_price_bulletins_bulk
delete_price_bulletin = _price_bulletins_route_handlers.delete_price_bulletin

# ============== SALES ROUTE HANDLERS ==============

_sale_creator_roles = [
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_ADMIN,
    UserRole.SELLER,
]
_sales_route_handlers = build_sales_route_handlers(
    db=db,
    get_current_user=get_current_user,
    sale_creator_roles=_sale_creator_roles,
    seller_role=UserRole.SELLER,
    find_sales_vehicle_by_id=find_sales_vehicle_by_id,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    find_user_by_id=find_user_by_id,
    create_sale_record=create_sale_record,
    calculate_commission=calculate_commission,
    resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
    apply_manual_sale_price_override=_apply_manual_sale_price_override,
    extract_active_aging_incentive_plan=_extract_active_aging_incentive_plan,
    apply_aging_plan_to_effective_pricing=_apply_aging_plan_to_effective_pricing,
    to_non_negative_float=_to_non_negative_float,
    log_audit_event=log_audit_event,
    serialize_doc=serialize_doc,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    list_sales_with_enrichment=list_sales_with_enrichment,
)
create_sale = _sales_route_handlers.create_sale
get_sales = _sales_route_handlers.get_sales

# ============== SALES OBJECTIVES ROUTE HANDLERS ==============

_sales_objectives_route_handlers = build_sales_objectives_route_handlers(
    db=db,
    get_current_user=get_current_user,
    objective_editor_roles=OBJECTIVE_EDITOR_ROLES,
    objective_approver_roles=OBJECTIVE_APPROVER_ROLES,
    objective_draft=OBJECTIVE_DRAFT,
    objective_approved=OBJECTIVE_APPROVED,
    objective_pending=OBJECTIVE_PENDING,
    objective_rejected=OBJECTIVE_REJECTED,
    seller_role=UserRole.SELLER,
    object_id_cls=ObjectId,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    log_audit_event=log_audit_event,
    serialize_doc=serialize_doc,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    list_sales_objectives_with_progress=_list_sales_objectives_with_progress_service,
    sale_effective_revenue=_sale_effective_revenue,
    list_sales_objectives=_list_sales_objectives_repo,
    find_user_by_id=_find_user_by_id_sales_objectives_repo,
    find_agency_by_id=_find_agency_by_id_sales_objectives_repo,
    find_brand_by_id=_find_brand_by_id_sales_objectives_repo,
    find_group_by_id=_find_group_by_id_sales_objectives_repo,
    list_sales=_list_sales_sales_objectives_repo,
    build_sales_objective_suggestion=_build_sales_objective_suggestion_service,
    add_months_ym=_add_months_ym,
    to_non_negative_float=_to_non_negative_float,
    is_price_bulletin_active=_is_price_bulletin_active_service,
    build_catalog_tree_from_source=_build_catalog_tree_from_source,
    find_catalog_make=_find_catalog_make,
    parse_catalog_price=_parse_catalog_price,
    list_price_bulletins=_list_price_bulletins_sales_objectives_repo,
)
create_sales_objective = _sales_objectives_route_handlers.create_sales_objective
get_sales_objectives = _sales_objectives_route_handlers.get_sales_objectives
get_sales_objective_suggestion = _sales_objectives_route_handlers.get_sales_objective_suggestion
update_sales_objective = _sales_objectives_route_handlers.update_sales_objective
approve_sales_objective = _sales_objectives_route_handlers.approve_sales_objective

# ============== COMMISSIONS ROUTE HANDLERS ==============

_commissions_route_handlers = build_commissions_route_handlers(
    db=db,
    get_current_user=get_current_user,
    object_id_cls=ObjectId,
    ensure_doc_scope_access=_ensure_doc_scope_access,
    log_audit_event=log_audit_event,
    serialize_doc=serialize_doc,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    validate_scope_filters=_validate_scope_filters,
    normalize_commission_matrix_general=_normalize_commission_matrix_general,
    normalize_commission_matrix_models=_normalize_commission_matrix_models,
    get_catalog_models_for_brand=_get_catalog_models_for_brand,
    build_matrix_models_response=_build_matrix_models_response,
    to_non_negative_float=_to_non_negative_float,
    normalize_commission_status=_normalize_commission_status_service,
    commission_matrix_editor_roles=COMMISSION_MATRIX_EDITOR_ROLES,
    commission_proposer_roles=COMMISSION_PROPOSER_ROLES,
    commission_approver_roles=COMMISSION_APPROVER_ROLES,
    commission_pending=COMMISSION_PENDING,
    commission_approved=COMMISSION_APPROVED,
    commission_rejected=COMMISSION_REJECTED,
    dealer_sales_effective_roles=DEALER_SALES_EFFECTIVE_ROLES,
    dealer_general_effective_roles=DEALER_GENERAL_EFFECTIVE_ROLES,
    dealer_seller_role=DEALER_SELLER_ROLE,
    seller_role=UserRole.SELLER,
    find_agency_by_id=_find_agency_by_id_commission_repo,
    find_brand_by_id=_find_brand_by_id_commission_repo,
    find_group_by_id=_find_group_by_id_commission_repo,
    find_user_by_id=_find_user_by_id_commission_repo,
    find_commission_matrix_by_agency=_find_commission_matrix_by_agency_repo,
    upsert_commission_matrix_by_agency=_upsert_commission_matrix_by_agency_repo,
    build_commission_matrix_upsert_fields=_build_commission_matrix_upsert_fields_service,
    build_commission_rule_doc=_build_commission_rule_doc_service,
    insert_commission_rule=_insert_commission_rule_repo,
    list_commission_rules=_list_commission_rules_repo,
    find_commission_rule_by_id=_find_commission_rule_by_id_repo,
    build_commission_rule_update_fields=_build_commission_rule_update_fields_service,
    update_commission_rule_by_id=_update_commission_rule_by_id_repo,
    build_commission_approval_update_fields=_build_commission_approval_update_fields_service,
    delete_commission_rule_by_id=_delete_commission_rule_by_id_repo,
    list_active_rules_by_agency=list_active_rules_by_agency,
    build_commission_simulator_projection=_build_commission_simulator_projection_service,
    calculate_commission_from_rules=_calculate_commission_from_rules,
    build_month_bounds=_build_month_bounds_service,
    list_sales_for_closure=_list_sales_for_closure_repo,
    build_commission_closure_snapshot=_build_commission_closure_snapshot_service,
    find_commission_closure_by_scope=_find_commission_closure_by_scope_repo,
    build_commission_closure_doc=_build_commission_closure_doc_service,
    update_commission_closure_by_id=_update_commission_closure_by_id_repo,
    find_commission_closure_by_id=_find_commission_closure_by_id_repo,
    insert_commission_closure=_insert_commission_closure_repo,
    list_commission_closures=_list_commission_closures_repo,
)
get_commission_matrix = _commissions_route_handlers.get_commission_matrix
upsert_commission_matrix = _commissions_route_handlers.upsert_commission_matrix
create_commission_rule = _commissions_route_handlers.create_commission_rule
get_commission_rules = _commissions_route_handlers.get_commission_rules
update_commission_rule = _commissions_route_handlers.update_commission_rule
approve_commission_rule = _commissions_route_handlers.approve_commission_rule
delete_commission_rule = _commissions_route_handlers.delete_commission_rule
commission_simulator = _commissions_route_handlers.commission_simulator
create_commission_closure = _commissions_route_handlers.create_commission_closure
get_commission_closures = _commissions_route_handlers.get_commission_closures
approve_commission_closure = _commissions_route_handlers.approve_commission_closure

# ============== DASHBOARD ROUTE HANDLERS ==============

_dashboard_route_handlers = build_dashboard_route_handlers(
    db=db,
    get_current_user=get_current_user,
    validate_scope_filters=_validate_scope_filters,
    build_scope_query=_build_scope_query,
    scope_query_has_access=_scope_query_has_access,
    resolve_dashboard_scope_group_id=_resolve_dashboard_scope_group_id,
    find_dashboard_monthly_close=_find_dashboard_monthly_close,
    build_dashboard_monthly_close_response=_build_dashboard_monthly_close_response_service,
    mexico_lft_holidays_by_month=_mexico_lft_holidays_by_month,
    list_global_monthly_closes_by_year=list_global_monthly_closes_by_year,
    build_dashboard_monthly_close_calendar=_build_dashboard_monthly_close_calendar_service,
    upsert_global_monthly_close=upsert_global_monthly_close,
    add_months_ym=_add_months_ym,
    log_audit_event=log_audit_event,
    app_admin_role=UserRole.APP_ADMIN,
    user_role_seller=UserRole.SELLER,
    empty_dashboard_kpis_response=_empty_dashboard_kpis_response_service,
    compute_dashboard_kpis=_compute_dashboard_kpis_service,
    list_vehicles=_list_vehicles_dashboard_repo,
    list_sales=_list_sales_dashboard_repo,
    list_vehicles_by_ids=_list_vehicles_by_ids_dashboard_repo,
    list_agencies_by_brand_id=_list_agencies_by_brand_id_dashboard_repo,
    list_agencies_by_group_id=_list_agencies_by_group_id_dashboard_repo,
    count_users=_count_users_dashboard_repo,
    count_sales=_count_sales_dashboard_repo,
    enrich_vehicle=enrich_vehicle,
    calculate_vehicle_financial_cost_in_period=calculate_vehicle_financial_cost_in_period,
    sale_effective_revenue=_sale_effective_revenue,
    compute_sales_trends=_compute_sales_trends_service,
    objective_approved=OBJECTIVE_APPROVED,
    objective_pending=OBJECTIVE_PENDING,
    list_sales_objectives=_list_sales_objectives_dashboard_repo,
    coerce_utc_datetime=_coerce_utc_datetime,
    decrement_month=_decrement_month,
    compute_operational_day_profile=_compute_operational_day_profile,
    resolve_effective_objective_units=_resolve_effective_objective_units,
    compute_seller_performance=_compute_seller_performance_service,
    find_user_by_id=_find_user_by_id_dashboard_repo,
    collect_vehicle_suggestions=_collect_vehicle_suggestions_service,
    build_vehicle_aging_suggestion=_build_vehicle_aging_suggestion,
)
get_dashboard_monthly_close = _dashboard_route_handlers["get_dashboard_monthly_close"]
get_dashboard_monthly_close_calendar = _dashboard_route_handlers["get_dashboard_monthly_close_calendar"]
upsert_dashboard_monthly_close = _dashboard_route_handlers["upsert_dashboard_monthly_close"]
get_dashboard_kpis = _dashboard_route_handlers["get_dashboard_kpis"]
get_sales_trends = _dashboard_route_handlers["get_sales_trends"]
get_seller_performance = _dashboard_route_handlers["get_seller_performance"]
get_vehicle_suggestions = _dashboard_route_handlers["get_vehicle_suggestions"]

# ============== ROOT ROUTE ==============

async def root():
    return {"message": "AutoConnect API - Vehicle Inventory Management System"}

async def health():
    return {"status": "healthy"}

register_route_modules(
    api_router,
    RouteModuleHandlers(
        auth_users=AuthUsersRouteHandlers(
            register=register,
            login=login,
            logout=logout,
            reset_password=reset_password,
            get_me=get_me,
            google_auth=google_auth,
            get_users=get_users,
            update_user=update_user,
            delete_user=delete_user,
            get_audit_logs=get_audit_logs,
            get_sellers=get_sellers,
        ),
        organization_catalog=OrganizationCatalogRouteHandlers(
            GroupCreate=GroupCreate,
            BrandCreate=BrandCreate,
            AgencyCreate=AgencyCreate,
            create_group=create_group,
            get_groups=get_groups,
            get_group=get_group,
            update_group=update_group,
            delete_group=delete_group,
            create_brand=create_brand,
            get_brands=get_brands,
            update_brand=update_brand,
            delete_brand=delete_brand,
            create_agency=create_agency,
            get_agencies=get_agencies,
            update_agency=update_agency,
            get_catalog_makes=get_catalog_makes,
            get_catalog_models=get_catalog_models,
            get_catalog_versions=get_catalog_versions,
        ),
        inventory=InventoryRouteHandlers(
            VehicleCreate=VehicleCreate,
            VehicleAgingIncentiveApply=VehicleAgingIncentiveApply,
            create_vehicle=create_vehicle,
            get_vehicles=get_vehicles,
            get_vehicle=get_vehicle,
            apply_vehicle_aging_incentive=apply_vehicle_aging_incentive,
            update_vehicle=update_vehicle,
        ),
        health=HealthRouteHandlers(
            root=root,
            health=health,
        ),
        imports=ImportRouteHandlers(
            import_organization=import_organization,
            import_vehicles=import_vehicles,
            import_sales=import_sales,
        ),
        sales=SalesRouteHandlers(
            SaleCreate=SaleCreate,
            create_sale=create_sale,
            get_sales=get_sales,
        ),
        price_bulletins=PriceBulletinsRouteHandlers(
            PriceBulletinBulkUpsert=PriceBulletinBulkUpsert,
            get_price_bulletins=get_price_bulletins,
            upsert_price_bulletins_bulk=upsert_price_bulletins_bulk,
            delete_price_bulletin=delete_price_bulletin,
        ),
        sales_objectives=SalesObjectivesRouteHandlers(
            SalesObjectiveCreate=SalesObjectiveCreate,
            SalesObjectiveApprovalAction=SalesObjectiveApprovalAction,
            create_sales_objective=create_sales_objective,
            get_sales_objectives=get_sales_objectives,
            get_sales_objective_suggestion=get_sales_objective_suggestion,
            update_sales_objective=update_sales_objective,
            approve_sales_objective=approve_sales_objective,
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
            create_financial_rate=create_financial_rate,
            apply_group_default_financial_rate=apply_group_default_financial_rate,
            get_financial_rates=get_financial_rates,
            update_financial_rate=update_financial_rate,
            delete_financial_rate=delete_financial_rate,
        ),
        commissions=CommissionsRouteHandlers(
            CommissionMatrixUpsert=CommissionMatrixUpsert,
            CommissionRuleCreate=CommissionRuleCreate,
            CommissionApprovalAction=CommissionApprovalAction,
            CommissionSimulatorInput=CommissionSimulatorInput,
            CommissionClosureCreate=CommissionClosureCreate,
            CommissionClosureApprovalAction=CommissionClosureApprovalAction,
            get_commission_matrix=get_commission_matrix,
            upsert_commission_matrix=upsert_commission_matrix,
            create_commission_rule=create_commission_rule,
            get_commission_rules=get_commission_rules,
            update_commission_rule=update_commission_rule,
            approve_commission_rule=approve_commission_rule,
            delete_commission_rule=delete_commission_rule,
            commission_simulator=commission_simulator,
            create_commission_closure=create_commission_closure,
            get_commission_closures=get_commission_closures,
            approve_commission_closure=approve_commission_closure,
        ),
    ),
)

mount_brand_logos(
    app=app,
    resolve_logo_directory=_resolve_logo_directory,
    logger=logger,
    logo_directory_env=LOGO_DIRECTORY_ENV,
)
include_api_router(app=app, api_router=api_router)
configure_cors(
    app=app,
    frontend_url=os.environ.get("FRONTEND_URL"),
    build_allowed_origins=_build_allowed_origins_service,
)

# ============== STARTUP ==============

@app.on_event("startup")
async def startup():
    await run_startup(
        db=db,
        logger=logger,
        create_core_indexes=_create_core_indexes_service,
        backfill_agency_locations=backfill_agency_locations,
        seed_admin_user=_seed_admin_user_service,
        default_admin_email="admin@autoconnect.com",
        default_admin_password="Admin123!",
        app_admin_role=UserRole.APP_ADMIN,
        hash_password=hash_password,
        verify_password=verify_password,
    )

@app.on_event("shutdown")
async def shutdown_db_client():
    await run_shutdown(client=client)
