from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, UploadFile, File, Depends, Query
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import os
import logging
import secrets
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple
from calendar import monthrange
from pathlib import Path
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
from handlers.catalog_handlers import build_catalog_route_handlers
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
def get_jwt_secret() -> str:
    return os.environ.get("JWT_SECRET", "default-secret-change-me")

def get_catalog_source_path() -> str:
    return _get_catalog_source_path_service(default_source_path=CATALOG_DEFAULT_SOURCE_PATH)

def get_catalog_model_year() -> int:
    return _get_catalog_model_year_service(default_model_year=CATALOG_DEFAULT_MODEL_YEAR)

def _resolve_logo_url_for_brand(brand_name: str, request: Optional[Request] = None) -> Optional[str]:
    return _resolve_logo_url_for_brand_service(
        brand_name,
        request=request,
        logo_directory_env=LOGO_DIRECTORY_ENV,
        cortex_root_default_path=CORTEX_ROOT_DEFAULT_PATH,
    )

def _resolve_logo_directory() -> Optional[Path]:
    return _resolve_logo_directory_service(
        logo_directory_env=LOGO_DIRECTORY_ENV,
        cortex_root_default_path=CORTEX_ROOT_DEFAULT_PATH,
    )

# Password functions
def hash_password(password: str) -> str:
    return _hash_password_service(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return _verify_password_service(plain_password, hashed_password)

# JWT functions
def create_access_token(user_id: str, email: str, role: str) -> str:
    return _create_access_token_service(
        user_id=user_id,
        email=email,
        role=role,
        jwt_secret=get_jwt_secret(),
        jwt_algorithm=JWT_ALGORITHM,
        expires_minutes=ACCESS_TOKEN_EXPIRE_MINUTES,
    )

def create_refresh_token(user_id: str) -> str:
    return _create_refresh_token_service(
        user_id=user_id,
        jwt_secret=get_jwt_secret(),
        jwt_algorithm=JWT_ALGORITHM,
        expires_days=REFRESH_TOKEN_EXPIRE_DAYS,
    )

# Create the main app
app = FastAPI(title="AutoConnect - Vehicle Inventory Management")

# Session middleware for OAuth
app.add_middleware(SessionMiddleware, secret_key=get_jwt_secret())

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# ============== AUTH HELPER ==============

async def get_current_user(request: Request) -> dict:
    return await _get_current_user_service(
        request=request,
        db=db,
        jwt_secret=get_jwt_secret(),
        jwt_algorithm=JWT_ALGORITHM,
    )

async def get_optional_user(request: Request) -> Optional[dict]:
    return await _get_optional_user_service(
        request=request,
        db=db,
        jwt_secret=get_jwt_secret(),
        jwt_algorithm=JWT_ALGORITHM,
    )

def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document to serializable dict"""
    if doc is None:
        return None
    result = {}
    for k, v in doc.items():
        if k == "_id":
            result["id"] = str(v)
        elif isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, datetime):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result

COMMISSION_PENDING = "pending"
COMMISSION_APPROVED = "approved"
COMMISSION_REJECTED = "rejected"
COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT = 100.0

async def _resolve_financial_rate_scope(
    current_user: dict,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
) -> Dict[str, Optional[str]]:
    normalized_group_id = str(group_id or "").strip()
    normalized_brand_id = str(brand_id or "").strip() or None
    normalized_agency_id = str(agency_id or "").strip() or None

    if not normalized_group_id:
        raise HTTPException(status_code=400, detail="group_id is required")
    if not ObjectId.is_valid(normalized_group_id):
        raise HTTPException(status_code=400, detail="Invalid group_id")

    group = await db.groups.find_one({"_id": ObjectId(normalized_group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    _validate_scope_filters(
        current_user,
        group_id=normalized_group_id,
        brand_id=normalized_brand_id,
        agency_id=normalized_agency_id,
    )

    brand = None
    if normalized_brand_id:
        if not ObjectId.is_valid(normalized_brand_id):
            raise HTTPException(status_code=400, detail="Invalid brand_id")
        brand = await db.brands.find_one({"_id": ObjectId(normalized_brand_id)})
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")
        if str(brand.get("group_id") or "") != normalized_group_id:
            raise HTTPException(status_code=400, detail="Brand does not belong to selected group")

    if normalized_agency_id:
        if not ObjectId.is_valid(normalized_agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")
        agency = await db.agencies.find_one({"_id": ObjectId(normalized_agency_id)})
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        if str(agency.get("group_id") or "") != normalized_group_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected group")

        agency_brand_id = str(agency.get("brand_id") or "")
        if normalized_brand_id:
            if agency_brand_id and agency_brand_id != normalized_brand_id:
                raise HTTPException(status_code=400, detail="Agency does not belong to selected brand")
        else:
            normalized_brand_id = agency_brand_id or None

    return {
        "group_id": normalized_group_id,
        "brand_id": normalized_brand_id,
        "agency_id": normalized_agency_id,
    }

OBJECTIVE_PENDING = "pending"
OBJECTIVE_DRAFT = "draft"
OBJECTIVE_APPROVED = "approved"
OBJECTIVE_REJECTED = "rejected"

def _to_jsonable(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    return value

async def log_audit_event(
    request: Request,
    current_user: Optional[dict],
    action: str,
    entity_type: str,
    entity_id: Optional[str] = None,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    if not current_user:
        return
    actor_role = current_user.get("role")
    if actor_role not in WRITE_AUDIT_ROLES:
        return

    client_ip = None
    if request and request.client:
        client_ip = request.client.host

    audit_doc = {
        "created_at": datetime.now(timezone.utc),
        "actor_id": current_user.get("id"),
        "actor_name": current_user.get("name"),
        "actor_email": current_user.get("email"),
        "actor_role": actor_role,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "group_id": group_id,
        "brand_id": brand_id,
        "agency_id": agency_id,
        "details": _to_jsonable(details or {}),
        "path": request.url.path if request and request.url else None,
        "method": request.method if request else None,
        "ip": client_ip,
    }
    await db.audit_logs.insert_one(audit_doc)

def _normalize_catalog_text(value: Any) -> Optional[str]:
    return _normalize_catalog_text_service(value)

def _parse_catalog_year(value: Any) -> Optional[int]:
    return _parse_catalog_year_service(value)

def _parse_catalog_price(value: Any) -> Optional[float]:
    return _parse_catalog_price_service(value)

def _resolve_agency_location(city: Optional[str], address: Optional[str]) -> Dict[str, Optional[str]]:
    return _resolve_agency_location_service(city, address)

def _compose_structured_agency_address(
    street: Optional[str],
    exterior_number: Optional[str],
    interior_number: Optional[str],
    neighborhood: Optional[str],
    city: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
    country: Optional[str],
) -> Optional[str]:
    return _compose_structured_agency_address_service(
        street=street,
        exterior_number=exterior_number,
        interior_number=interior_number,
        neighborhood=neighborhood,
        city=city,
        state=state,
        postal_code=postal_code,
        country=country,
    )

def _merge_optional_text(incoming: Optional[str], previous: Optional[str]) -> Optional[str]:
    return _merge_optional_text_service(incoming, previous)

def _merge_optional_float(incoming: Optional[float], previous: Any) -> Optional[float]:
    return _merge_optional_float_service(incoming, previous)

async def backfill_agency_locations() -> Dict[str, int]:
    return await _backfill_agency_locations_service(db=db)

def _build_catalog_tree_from_source(all_years: bool = False) -> Dict[str, Any]:
    return _build_catalog_tree_from_source_service(
        source_path=get_catalog_source_path(),
        model_year=get_catalog_model_year(),
        all_years=all_years,
    )

def _find_catalog_make(catalog: Dict[str, Any], make_name: str) -> Optional[Dict[str, Any]]:
    return _find_catalog_make_service(catalog, make_name)

def _find_catalog_model(make_entry: Dict[str, Any], model_name: str) -> Optional[Dict[str, Any]]:
    return _find_catalog_model_service(make_entry, model_name)

def _ensure_allowed_model_year(year: int) -> None:
    _ensure_allowed_model_year_service(year=year, allowed_year=get_catalog_model_year())

# ============== AUTH ROUTES ==============

async def register(user_data: UserCreate, request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    _require_action_role(ACTION_USERS_MANAGE, actor_role, detail="Not authorized")
    try:
        _apply_register_scope_constraints_service(
            current_user=current_user,
            user_data=user_data,
            app_admin_role=UserRole.APP_ADMIN,
            app_user_role=UserRole.APP_USER,
            group_admin_role=UserRole.GROUP_ADMIN,
            is_dealer_user_manager_role=_is_dealer_user_manager_role,
            get_dealer_assignable_roles=_get_dealer_assignable_roles,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    email = _normalize_user_email_service(user_data.email)
    existing = await find_user_by_email(db, email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    await _resolve_register_hierarchy_scope_service(
        db=db,
        user_data=user_data,
        find_brand_by_id=find_brand_by_id,
        find_agency_by_id=find_agency_by_id,
    )

    try:
        _validate_role_scope_requirements_service(
            role=user_data.role,
            brand_id=user_data.brand_id,
            agency_id=user_data.agency_id,
            brand_scoped_roles=[UserRole.BRAND_ADMIN, UserRole.BRAND_USER],
            agency_scoped_roles=[
                UserRole.AGENCY_ADMIN,
                UserRole.AGENCY_SALES_MANAGER,
                UserRole.AGENCY_GENERAL_MANAGER,
                UserRole.AGENCY_COMMERCIAL_MANAGER,
                UserRole.AGENCY_USER,
                UserRole.SELLER,
            ],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    normalized_position = _normalize_optional_position_service(user_data.position)

    user_doc = _build_user_document_service(
        email=email,
        password_hash=hash_password(user_data.password),
        name=user_data.name,
        position=normalized_position,
        role=user_data.role,
        group_id=user_data.group_id,
        brand_id=user_data.brand_id,
        agency_id=user_data.agency_id,
        created_at=datetime.now(timezone.utc),
    )
    user_id = await create_user(db, user_doc)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_user",
        entity_type="user",
        entity_id=user_id,
        group_id=user_data.group_id,
        brand_id=user_data.brand_id,
        agency_id=user_data.agency_id,
        details={
            "email": email,
            "name": user_data.name,
            "position": normalized_position,
            "role": user_data.role,
        },
    )

    return {
        "id": user_id,
        "email": email,
        "name": user_data.name,
        "position": normalized_position,
        "role": user_data.role,
        "group_id": user_data.group_id,
        "brand_id": user_data.brand_id,
        "agency_id": user_data.agency_id,
        "created_at": user_doc["created_at"].isoformat()
    }

async def login(user_data: UserLogin, response: Response):
    login_result = await _login_user_service(
        db,
        user_data=user_data,
        find_user_by_email=find_user_by_email,
        verify_password=verify_password,
        create_access_token=create_access_token,
        create_refresh_token=create_refresh_token,
    )
    access_token = login_result["access_token"]
    refresh_token = login_result["refresh_token"]
    
    response.set_cookie(key="access_token", value=access_token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, secure=False, samesite="lax", max_age=604800, path="/")
    
    return login_result["user_payload"]

async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    return {"message": "Logged out successfully"}

async def reset_password(payload: PasswordResetRequest):
    return await _reset_password_flow_service(
        db,
        payload=payload,
        find_user_by_email=find_user_by_email,
        update_user_password_hash=update_user_password_hash,
        hash_password=hash_password,
    )

async def get_me(request: Request):
    user = await get_current_user(request)
    return user

async def google_auth(request: Request, response: Response):
    """Handle Google OAuth callback"""
    data = await request.json()
    credential = data.get("credential")
    try:
        auth_result = await _google_auth_flow_service(
            db,
            credential=credential,
            find_user_by_email=find_user_by_email,
            create_user=create_user,
            create_access_token=create_access_token,
            create_refresh_token=create_refresh_token,
            app_user_role=UserRole.APP_USER,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Google auth error: {exc}")
        raise HTTPException(status_code=400, detail=f"Invalid Google credential: {str(exc)}") from exc

    response.set_cookie(key="access_token", value=auth_result["access_token"], httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
    response.set_cookie(key="refresh_token", value=auth_result["refresh_token"], httponly=True, secure=False, samesite="lax", max_age=604800, path="/")

    return auth_result["user_payload"]

# ============== USERS ROUTES ==============

async def get_users(request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    _require_action_role(ACTION_USERS_MANAGE, actor_role, detail="Not authorized")

    query, should_return_empty = _build_users_query_for_actor_service(
        actor_role=actor_role,
        current_user=current_user,
        group_admin_role=UserRole.GROUP_ADMIN,
        is_dealer_user_manager_role=_is_dealer_user_manager_role,
    )
    if should_return_empty:
        return []
    
    users = await list_users(db, query, include_password_hash=False, limit=1000)
    return [serialize_doc(u) for u in users]

async def update_user(user_id: str, request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    _require_action_role(ACTION_USERS_MANAGE, actor_role, detail="Not authorized")
    
    data = await request.json()
    try:
        new_password, payload_without_password = _extract_new_password_and_payload_service(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    update_data = _sanitize_user_update_data_service(payload_without_password)

    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id")
    existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        update_data = _enforce_update_scope_permissions_service(
            current_user=current_user,
            existing_user=existing_user,
            update_data=update_data,
            app_admin_role=UserRole.APP_ADMIN,
            app_user_role=UserRole.APP_USER,
            group_admin_role=UserRole.GROUP_ADMIN,
            is_dealer_user_manager_role=_is_dealer_user_manager_role,
            get_dealer_assignable_roles=_get_dealer_assignable_roles,
            same_scope_id=_same_scope_id,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    
    if new_password:
        update_data["password_hash"] = hash_password(new_password)

    if update_data:
        await update_user_by_id(db, user_id, update_data)
    user = await find_user_by_id(db, user_id, include_password_hash=False)

    audit_changes = _build_user_update_audit_changes_service(update_data)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_user",
        entity_type="user",
        entity_id=str(existing_user["_id"]),
        group_id=user.get("group_id"),
        brand_id=user.get("brand_id"),
        agency_id=user.get("agency_id"),
        details={
            "changes": audit_changes,
            "target_email": user.get("email"),
            "target_role": user.get("role"),
        },
    )
    return serialize_doc(user)

async def delete_user(user_id: str, request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    _require_action_role(ACTION_USERS_MANAGE, actor_role, detail="Not authorized")

    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id")

    existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        _enforce_delete_scope_permissions_service(
            current_user=current_user,
            existing_user=existing_user,
            app_admin_role=UserRole.APP_ADMIN,
            group_admin_role=UserRole.GROUP_ADMIN,
            is_dealer_user_manager_role=_is_dealer_user_manager_role,
            get_dealer_assignable_roles=_get_dealer_assignable_roles,
            same_scope_id=_same_scope_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    await delete_user_by_id(db, user_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_user",
        entity_type="user",
        entity_id=user_id,
        group_id=existing_user.get("group_id"),
        brand_id=existing_user.get("brand_id"),
        agency_id=existing_user.get("agency_id"),
        details={
            "target_email": existing_user.get("email"),
            "target_role": existing_user.get("role"),
            "target_name": existing_user.get("name"),
        },
    )
    return {"message": "User deleted"}

async def get_audit_logs(
    request: Request,
    agency_id: Optional[str] = None,
    group_id: Optional[str] = None,
    actor_id: Optional[str] = None,
    limit: int = 100
):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    _require_action_role(ACTION_AUDIT_LOGS_READ, actor_role, detail="Not authorized")

    safe_limit = max(1, min(limit, 500))
    query, should_return_empty = _build_audit_logs_query_for_actor_service(
        actor_role=actor_role,
        current_user=current_user,
        agency_id=agency_id,
        group_id=group_id,
        actor_id=actor_id,
        group_admin_role=UserRole.GROUP_ADMIN,
        group_finance_role=UserRole.GROUP_FINANCE_MANAGER,
        is_dealer_user_manager_role=_is_dealer_user_manager_role,
    )
    if should_return_empty:
        return []

    logs = await list_audit_logs(db, query, limit=safe_limit)
    return [serialize_doc(item) for item in logs]

async def get_sellers(request: Request, agency_id: Optional[str] = None, brand_id: Optional[str] = None, group_id: Optional[str] = None):
    """Get sellers (users with seller role) filtered by agency/brand/group"""
    current_user = await get_current_user(request)
    
    query = {"role": UserRole.SELLER}
    scope_query = _build_scope_query(current_user)
    if not _scope_query_has_access(scope_query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

    if group_id:
        query["group_id"] = group_id
    if brand_id:
        query["brand_id"] = brand_id
    if agency_id:
        query["agency_id"] = agency_id
    
    sellers = await list_users(db, query, include_password_hash=False, limit=1000)
    
    # Enrich with agency name
    result = []
    for seller in sellers:
        s = serialize_doc(seller)
        if seller.get("agency_id"):
            agency = await find_agency_by_id(db, seller["agency_id"])
            if agency:
                s["agency_name"] = agency["name"]
        result.append(s)
    
    return result

# ============== GROUPS ROUTES ==============

async def create_group(group_data: GroupCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    group_doc = {
        "name": group_data.name,
        "description": group_data.description,
        "created_at": datetime.now(timezone.utc)
    }
    group_id = await insert_group(db, group_doc)
    group_doc["id"] = group_id
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_group",
        entity_type="group",
        entity_id=group_id,
        group_id=group_id,
        details={"name": group_data.name, "description": group_data.description},
    )
    return serialize_doc(group_doc)

async def get_groups(request: Request):
    current_user = await get_current_user(request)
    
    query = {}
    # Super admin y super users pueden ver todos los grupos
    # Otros roles solo ven su grupo asignado
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if current_user.get("group_id"):
            query["_id"] = ObjectId(current_user["group_id"])
        else:
            # Si no tiene grupo asignado, no ve ninguno
            return []
    
    groups = await list_groups(db, query, limit=1000)
    return [serialize_doc(g) for g in groups]

async def get_group(group_id: str, request: Request):
    current_user = await get_current_user(request)
    
    # Verificar acceso al grupo
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if current_user.get("group_id") != group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    
    group = await find_group_by_id(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return serialize_doc(group)

async def update_group(group_id: str, group_data: GroupCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    previous = await find_group_by_id(db, group_id)
    await update_group_by_id(db, group_id, {"name": group_data.name, "description": group_data.description})
    group = await find_group_by_id(db, group_id)
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_group",
        entity_type="group",
        entity_id=group_id,
        group_id=group_id,
        details={
            "before": {"name": previous.get("name") if previous else None, "description": previous.get("description") if previous else None},
            "after": {"name": group.get("name") if group else None, "description": group.get("description") if group else None},
        },
    )
    return serialize_doc(group)

async def delete_group(group_id: str, request: Request, cascade: bool = Query(False)):
    current_user = await get_current_user(request)
    if current_user["role"] != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(group_id):
        raise HTTPException(status_code=400, detail="Invalid group_id")

    group = await find_group_by_id(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    context = await build_group_delete_context(db, group_id)
    dependency_counts = await summarize_group_dependencies(db, context)
    dependencies = format_dependency_messages(dependency_counts)
    if dependencies and not cascade:
        raise HTTPException(
            status_code=409,
            detail=f"No se puede borrar el grupo porque tiene registros relacionados: {', '.join(dependencies)}. Usa borrado en cascada para eliminar todo."
        )

    deleted_counts = {
        "sales": 0,
        "commission_rules": 0,
        "sales_objectives": 0,
        "financial_rates": 0,
        "vehicles": 0,
        "users": 0,
        "agencies": 0,
        "brands": 0,
        "groups": 0,
    }

    if cascade:
        deleted_counts.update(await execute_group_cascade_delete(db, context))

    deleted_counts["groups"] = await delete_group_by_id(db, group_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_group",
        entity_type="group",
        entity_id=group_id,
        group_id=group_id,
        details={
            "cascade": cascade,
            "deleted": deleted_counts,
            "name": group.get("name"),
        },
    )

    return {
        "message": "Group deleted",
        "cascade": cascade,
        "deleted": deleted_counts
    }

# ============== BRANDS ROUTES ==============

async def create_brand(brand_data: BrandCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(brand_data.group_id):
        raise HTTPException(status_code=400, detail="Invalid group_id")

    group = await find_group_by_id(db, brand_data.group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    if current_user["role"] == UserRole.GROUP_ADMIN:
        user_group_id = current_user.get("group_id")
        if not user_group_id or brand_data.group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso para crear marcas fuera de tu grupo")
    
    brand_doc = {
        "name": brand_data.name,
        "group_id": brand_data.group_id,
        "logo_url": brand_data.logo_url,
        "created_at": datetime.now(timezone.utc)
    }
    brand_id = await insert_brand(db, brand_doc)
    brand_doc["id"] = brand_id
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_brand",
        entity_type="brand",
        entity_id=brand_id,
        group_id=brand_data.group_id,
        details={"name": brand_data.name},
    )
    serialized = serialize_doc(brand_doc)
    if not serialized.get("logo_url"):
        fallback_logo = _resolve_logo_url_for_brand(serialized.get("name", ""), request)
        if fallback_logo:
            serialized["logo_url"] = fallback_logo
    return serialized

async def get_brands(request: Request, group_id: Optional[str] = None):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []
    # brands collection stores brand identifier in _id (not brand_id field).
    query.pop("agency_id", None)
    if _is_brand_scoped_role(current_user.get("role")) or _is_agency_scoped_role(current_user.get("role")):
        user_brand_id = current_user.get("brand_id")
        if not user_brand_id or not ObjectId.is_valid(user_brand_id):
            return []
        query["_id"] = ObjectId(user_brand_id)
        query.pop("brand_id", None)

    _validate_scope_filters(current_user, group_id=group_id)
    if group_id:
        query["group_id"] = group_id
    
    brands = await list_brands(db, query, limit=1000)
    output: List[Dict[str, Any]] = []
    for brand in brands:
        serialized = serialize_doc(brand)
        if not serialized.get("logo_url"):
            fallback_logo = _resolve_logo_url_for_brand(serialized.get("name", ""), request)
            if fallback_logo:
                serialized["logo_url"] = fallback_logo
        output.append(serialized)
    return output

async def update_brand(brand_id: str, brand_data: BrandCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(brand_id):
        raise HTTPException(status_code=400, detail="Invalid brand_id")
    if not ObjectId.is_valid(brand_data.group_id):
        raise HTTPException(status_code=400, detail="Invalid group_id")

    brand = await find_brand_by_id(db, brand_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    target_group = await find_group_by_id(db, brand_data.group_id)
    if not target_group:
        raise HTTPException(status_code=404, detail="Group not found")

    if current_user["role"] == UserRole.GROUP_ADMIN:
        user_group_id = current_user.get("group_id")
        current_group_id = str(brand.get("group_id") or "")
        if (
            not user_group_id or
            current_group_id != user_group_id or
            brand_data.group_id != user_group_id
        ):
            raise HTTPException(status_code=403, detail="No tienes acceso para modificar esta marca")
    
    previous = brand
    await update_brand_by_id(
        db,
        brand_id,
        {"name": brand_data.name, "group_id": brand_data.group_id, "logo_url": brand_data.logo_url},
    )
    brand = await find_brand_by_id(db, brand_id)
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_brand",
        entity_type="brand",
        entity_id=brand_id,
        group_id=brand_data.group_id,
        details={
            "before": {
                "name": previous.get("name"),
                "group_id": previous.get("group_id"),
                "logo_url": previous.get("logo_url"),
            },
            "after": {
                "name": brand.get("name") if brand else None,
                "group_id": brand.get("group_id") if brand else None,
                "logo_url": brand.get("logo_url") if brand else None,
            },
        },
    )
    serialized = serialize_doc(brand)
    if not serialized.get("logo_url"):
        fallback_logo = _resolve_logo_url_for_brand(serialized.get("name", ""), request)
        if fallback_logo:
            serialized["logo_url"] = fallback_logo
    return serialized

async def delete_brand(brand_id: str, request: Request, cascade: bool = Query(False)):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(brand_id):
        raise HTTPException(status_code=400, detail="Invalid brand_id")

    brand = await find_brand_by_id(db, brand_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    if current_user["role"] == UserRole.GROUP_ADMIN:
        user_group_id = current_user.get("group_id")
        if not user_group_id or not _same_scope_id(brand.get("group_id"), user_group_id):
            raise HTTPException(status_code=403, detail="No tienes acceso para borrar esta marca")

    brand_group_id = str(brand.get("group_id") or "")
    context = await build_brand_delete_context(db, brand_id)
    dependency_counts = await summarize_brand_dependencies(db, context)
    dependencies = format_dependency_messages(dependency_counts)
    if dependencies and not cascade:
        raise HTTPException(
            status_code=409,
            detail=f"No se puede borrar la marca porque tiene registros relacionados: {', '.join(dependencies)}. Usa borrado en cascada para eliminar todo."
        )

    deleted_counts = {
        "sales": 0,
        "commission_rules": 0,
        "sales_objectives": 0,
        "financial_rates": 0,
        "vehicles": 0,
        "users": 0,
        "agencies": 0,
        "brands": 0,
    }

    if cascade:
        deleted_counts.update(await execute_brand_cascade_delete(db, context))

    deleted_counts["brands"] = await delete_brand_by_id(db, brand_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_brand",
        entity_type="brand",
        entity_id=brand_id,
        group_id=brand_group_id,
        brand_id=brand_id,
        details={
            "name": brand.get("name"),
            "cascade": cascade,
            "deleted": deleted_counts,
        },
    )

    return {
        "message": "Brand deleted",
        "cascade": cascade,
        "deleted": deleted_counts
    }

# ============== AGENCIES ROUTES ==============

async def create_agency(agency_data: AgencyCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get brand to link group_id
    brand = await find_brand_by_id(db, agency_data.brand_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")

    street = _normalize_catalog_text(agency_data.street)
    exterior_number = _normalize_catalog_text(agency_data.exterior_number)
    interior_number = _normalize_catalog_text(agency_data.interior_number)
    neighborhood = _normalize_catalog_text(agency_data.neighborhood)
    municipality = _normalize_catalog_text(agency_data.municipality)
    state = _normalize_catalog_text(agency_data.state)
    country = _normalize_catalog_text(agency_data.country) or "Mexico"

    explicit_city = _normalize_catalog_text(agency_data.city) or municipality
    explicit_postal_code = _normalize_catalog_text(agency_data.postal_code)
    address = _normalize_catalog_text(agency_data.address) or _compose_structured_agency_address(
        street=street,
        exterior_number=exterior_number,
        interior_number=interior_number,
        neighborhood=neighborhood,
        city=explicit_city,
        state=state,
        postal_code=explicit_postal_code,
        country=country,
    )
    location = _resolve_agency_location(explicit_city, address)
    final_city = explicit_city or location["city"]
    final_postal_code = explicit_postal_code or location["postal_code"]
    
    agency_doc = {
        "name": agency_data.name,
        "brand_id": agency_data.brand_id,
        "group_id": brand["group_id"],
        "address": address,
        "city": final_city,
        "postal_code": final_postal_code,
        "street": street,
        "exterior_number": exterior_number,
        "interior_number": interior_number,
        "neighborhood": neighborhood,
        "municipality": municipality,
        "state": state,
        "country": country,
        "google_place_id": _normalize_catalog_text(agency_data.google_place_id),
        "latitude": agency_data.latitude,
        "longitude": agency_data.longitude,
        "created_at": datetime.now(timezone.utc)
    }
    agency_id = await insert_agency(db, agency_doc)
    agency_doc["id"] = agency_id
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_agency",
        entity_type="agency",
        entity_id=agency_id,
        group_id=brand.get("group_id"),
        brand_id=agency_data.brand_id,
        agency_id=agency_id,
        details={
            "name": agency_data.name,
            "city": final_city,
            "state": state,
            "postal_code": final_postal_code,
            "address": address,
            "google_place_id": agency_doc.get("google_place_id"),
            "latitude": agency_doc.get("latitude"),
            "longitude": agency_doc.get("longitude"),
        },
    )
    return serialize_doc(agency_doc)

async def get_agencies(request: Request, brand_id: Optional[str] = None, group_id: Optional[str] = None):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []
    # agencies collection stores agency identifier in _id (not agency_id field).
    if _is_agency_scoped_role(current_user.get("role")):
        user_agency_id = current_user.get("agency_id")
        if not user_agency_id or not ObjectId.is_valid(user_agency_id):
            return []
        query["_id"] = ObjectId(user_agency_id)
        query.pop("agency_id", None)

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id)
    if group_id:
        query["group_id"] = group_id
    if brand_id:
        query["brand_id"] = brand_id
    
    agencies = await list_agencies(db, query, limit=1000)
    
    # Enrich with brand names
    brand_ids = list(set(a.get("brand_id") for a in agencies if a.get("brand_id")))
    brands = await list_brands_by_ids(db, brand_ids, limit=1000)
    brand_map = {str(b["_id"]): b["name"] for b in brands}
    
    result = []
    for a in agencies:
        agency = serialize_doc(a)
        city_source = a.get("city") or a.get("municipality")
        location = _resolve_agency_location(city_source, a.get("address"))
        agency["city"] = city_source or location["city"]
        agency["postal_code"] = a.get("postal_code") or location["postal_code"]
        agency["brand_name"] = brand_map.get(a.get("brand_id"), "")
        result.append(agency)
    
    return result

async def update_agency(agency_id: str, agency_data: AgencyCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    previous = await find_agency_by_id(db, agency_id)
    if not previous:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta agencia")
    if current_user["role"] == UserRole.AGENCY_ADMIN and agency_data.brand_id != previous.get("brand_id"):
        raise HTTPException(status_code=403, detail="Agency admin cannot move agency to another brand")

    street = _merge_optional_text(agency_data.street, previous.get("street"))
    exterior_number = _merge_optional_text(agency_data.exterior_number, previous.get("exterior_number"))
    interior_number = _merge_optional_text(agency_data.interior_number, previous.get("interior_number"))
    neighborhood = _merge_optional_text(agency_data.neighborhood, previous.get("neighborhood"))
    municipality = _merge_optional_text(agency_data.municipality, previous.get("municipality"))
    state = _merge_optional_text(agency_data.state, previous.get("state"))
    country = _merge_optional_text(agency_data.country, previous.get("country")) or "Mexico"

    explicit_city = _merge_optional_text(agency_data.city, previous.get("city")) or municipality
    explicit_postal_code = _merge_optional_text(agency_data.postal_code, previous.get("postal_code"))
    address = _merge_optional_text(agency_data.address, previous.get("address")) or _compose_structured_agency_address(
        street=street,
        exterior_number=exterior_number,
        interior_number=interior_number,
        neighborhood=neighborhood,
        city=explicit_city,
        state=state,
        postal_code=explicit_postal_code,
        country=country,
    )
    location = _resolve_agency_location(explicit_city, address)
    final_city = explicit_city or location["city"]
    final_postal_code = explicit_postal_code or location["postal_code"]
    latitude = _merge_optional_float(agency_data.latitude, previous.get("latitude"))
    longitude = _merge_optional_float(agency_data.longitude, previous.get("longitude"))
    google_place_id = _merge_optional_text(agency_data.google_place_id, previous.get("google_place_id"))

    await update_agency_by_id(
        db,
        agency_id,
        {
            "name": agency_data.name,
            "address": address,
            "city": final_city,
            "postal_code": final_postal_code,
            "street": street,
            "exterior_number": exterior_number,
            "interior_number": interior_number,
            "neighborhood": neighborhood,
            "municipality": municipality,
            "state": state,
            "country": country,
            "google_place_id": google_place_id,
            "latitude": latitude,
            "longitude": longitude,
        },
    )
    agency = await find_agency_by_id(db, agency_id)
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_agency",
        entity_type="agency",
        entity_id=agency_id,
        group_id=agency.get("group_id") if agency else previous.get("group_id") if previous else None,
        brand_id=agency.get("brand_id") if agency else previous.get("brand_id") if previous else None,
        agency_id=agency_id,
        details={
            "before": {
                "name": previous.get("name") if previous else None,
                "city": previous.get("city") if previous else None,
                "postal_code": previous.get("postal_code") if previous else None,
                "address": previous.get("address") if previous else None,
                "state": previous.get("state") if previous else None,
                "municipality": previous.get("municipality") if previous else None,
                "google_place_id": previous.get("google_place_id") if previous else None,
                "latitude": previous.get("latitude") if previous else None,
                "longitude": previous.get("longitude") if previous else None,
            },
            "after": {
                "name": agency.get("name") if agency else None,
                "city": agency.get("city") if agency else None,
                "postal_code": agency.get("postal_code") if agency else None,
                "address": agency.get("address") if agency else None,
                "state": agency.get("state") if agency else None,
                "municipality": agency.get("municipality") if agency else None,
                "google_place_id": agency.get("google_place_id") if agency else None,
                "latitude": agency.get("latitude") if agency else None,
                "longitude": agency.get("longitude") if agency else None,
            },
        },
    )
    return serialize_doc(agency)

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

# ============== PRICE BULLETINS ROUTES ==============

def _normalize_iso_date_string(value: Optional[str], *, field_name: str, required: bool = False) -> Optional[str]:
    return _normalize_iso_date_string_service(value, field_name=field_name, required=required)

async def _resolve_effective_sale_pricing_for_model(
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    model: Optional[str],
    version: Optional[str] = None,
    reference_date_ymd: Optional[str] = None,
    fallback_msrp: Optional[float] = None,
) -> Dict[str, Any]:
    return await _resolve_effective_sale_pricing_for_model_service(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        model=model,
        version=version,
        reference_date_ymd=reference_date_ymd,
        fallback_msrp=fallback_msrp,
        to_non_negative_float=_to_non_negative_float,
    )

def _apply_manual_sale_price_override(
    pricing: Dict[str, Any],
    supplied_sale_price: Optional[float],
) -> Dict[str, Any]:
    return _apply_manual_sale_price_override_service(
        pricing=pricing,
        supplied_sale_price=supplied_sale_price,
        to_non_negative_float=_to_non_negative_float,
    )

async def _resolve_effective_transaction_price_for_model(
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    model: Optional[str],
    version: Optional[str] = None,
    reference_date_ymd: Optional[str] = None,
    fallback_msrp: Optional[float] = None,
) -> float:
    pricing = await _resolve_effective_sale_pricing_for_model_service(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        model=model,
        version=version,
        reference_date_ymd=reference_date_ymd,
        fallback_msrp=fallback_msrp,
        to_non_negative_float=_to_non_negative_float,
    )
    return _to_non_negative_float(pricing.get("transaction_price"), 0.0)

async def get_price_bulletins(
    request: Request,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    model: Optional[str] = None,
    active_only: bool = False,
    latest_per_model: bool = False,
    include_brand_defaults: bool = True,
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)

    if group_id:
        query["group_id"] = group_id
    if brand_id:
        query["brand_id"] = brand_id

    normalized_agency_id = str(agency_id or "").strip() or None
    if normalized_agency_id:
        if include_brand_defaults:
            query["$or"] = [{"agency_id": normalized_agency_id}, {"agency_id": None}]
        else:
            query["agency_id"] = normalized_agency_id
    else:
        query["agency_id"] = None

    normalized_model = str(model or "").strip()
    if normalized_model:
        query["model"] = normalized_model

    return await _list_price_bulletins_with_enrichment_service(
        db,
        query=query,
        normalized_agency_id=normalized_agency_id,
        active_only=active_only,
        latest_per_model=latest_per_model,
        serialize_doc=serialize_doc,
        is_price_bulletin_active=_is_price_bulletin_active_service,
    )

async def upsert_price_bulletins_bulk(payload: PriceBulletinBulkUpsert, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in PRICE_BULLETIN_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not payload.items:
        raise HTTPException(status_code=400, detail="At least one item is required")

    scope = await _resolve_price_bulletin_scope_service(
        db,
        current_user=current_user,
        group_id=payload.group_id,
        brand_id=payload.brand_id,
        agency_id=payload.agency_id,
        validate_scope_filters=_validate_scope_filters,
    )

    effective_from = _normalize_iso_date_string(
        payload.effective_from or datetime.now(timezone.utc).date().isoformat(),
        field_name="effective_from",
        required=True,
    )
    effective_to = _normalize_iso_date_string(payload.effective_to, field_name="effective_to", required=False)
    if effective_to and effective_to < effective_from:
        raise HTTPException(status_code=400, detail="effective_to must be on or after effective_from")

    bulletin_name = str(payload.bulletin_name or "").strip()
    if not bulletin_name:
        bulletin_name = f"Boletín {scope['brand_name']} {effective_from}"
    notes = str(payload.notes or "").strip() or None

    now = datetime.now(timezone.utc)
    updated_count, valid_items = await _upsert_price_bulletins_items_service(
        db,
        scope=scope,
        items=payload.items,
        effective_from=effective_from,
        effective_to=effective_to,
        bulletin_name=bulletin_name,
        notes=notes,
        current_user_id=current_user.get("id"),
        now=now,
        to_non_negative_float=_to_non_negative_float,
    )

    if updated_count == 0:
        raise HTTPException(status_code=400, detail="No valid items to save")

    repricing_summary = await _reprice_sales_for_price_bulletin_service(
        db,
        scope=scope,
        effective_from=effective_from,
        effective_to=effective_to,
        items=valid_items,
        price_item_applies_to_sale=_price_item_applies_to_sale_service,
        resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
        apply_manual_sale_price_override=_apply_manual_sale_price_override,
        calculate_commission=calculate_commission,
        to_non_negative_float=_to_non_negative_float,
        coerce_utc_datetime=_coerce_utc_datetime,
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="upsert_price_bulletins_bulk",
        entity_type="price_bulletin",
        entity_id=None,
        group_id=scope["group_id"],
        brand_id=scope["brand_id"],
        agency_id=scope["agency_id"],
        details={
            "bulletin_name": bulletin_name,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "items_count": updated_count,
            "repricing": repricing_summary,
        },
    )

    return {
        "message": "Price bulletins saved",
        "group_id": scope["group_id"],
        "brand_id": scope["brand_id"],
        "agency_id": scope["agency_id"],
        "bulletin_name": bulletin_name,
        "effective_from": effective_from,
        "effective_to": effective_to,
        "items_count": updated_count,
        "repricing": repricing_summary,
    }

async def delete_price_bulletin(bulletin_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in PRICE_BULLETIN_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(bulletin_id):
        raise HTTPException(status_code=400, detail="Invalid bulletin_id")

    previous = await db.price_bulletins.find_one({"_id": ObjectId(bulletin_id)})
    if not previous:
        raise HTTPException(status_code=404, detail="Price bulletin not found")
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este boletín")
    await _remove_price_bulletin_service(
        db,
        bulletin_id=bulletin_id,
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_price_bulletin",
        entity_type="price_bulletin",
        entity_id=bulletin_id,
        group_id=previous.get("group_id"),
        brand_id=previous.get("brand_id"),
        agency_id=previous.get("agency_id"),
        details={
            "model": previous.get("model"),
            "version": previous.get("version"),
            "effective_from": previous.get("effective_from"),
        },
    )
    return {"message": "Price bulletin deleted"}

# ============== FINANCIAL RATES ROUTES ==============

DAYS_PER_MONTH_FOR_RATE = 30


def _monthly_to_annual(rate_monthly_pct: float) -> float:
    return _monthly_to_annual_service(rate_monthly_pct)


def _extract_rate_components_from_doc(rate_doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    return _extract_rate_components_from_doc_service(rate_doc)


async def _resolve_effective_rate_components_for_scope(
    *,
    group_id: Any,
    brand_id: Any,
    agency_id: Any,
) -> Dict[str, float]:
    return await _resolve_effective_rate_components_service(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        find_latest_financial_rate=_find_latest_financial_rate_repo,
        extract_rate_components_from_doc=_extract_rate_components_from_doc,
    )


async def _resolve_effective_rate_components_for_vehicle(vehicle: Dict[str, Any]) -> Dict[str, float]:
    return await _resolve_effective_rate_components_for_scope(
        group_id=vehicle.get("group_id"),
        brand_id=vehicle.get("brand_id"),
        agency_id=vehicle.get("agency_id"),
    )


async def _build_default_financial_rate_name(
    group_id: Optional[str],
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
) -> str:
    return await _build_default_financial_rate_name_service(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        find_group_by_id=find_group_by_id,
        find_brand_by_id=find_brand_by_id,
        find_agency_by_id=find_agency_by_id,
    )

async def create_financial_rate(rate_data: FinancialRateCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in FINANCIAL_RATE_MANAGER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    scope = await _resolve_financial_rate_scope(
        current_user=current_user,
        group_id=rate_data.group_id,
        brand_id=rate_data.brand_id,
        agency_id=rate_data.agency_id,
    )

    rate_name = str(rate_data.name or "").strip()
    if not rate_name:
        rate_name = await _build_default_financial_rate_name(
            group_id=scope["group_id"],
            brand_id=scope["brand_id"],
            agency_id=scope["agency_id"],
        )

    financial_rate_payload = _build_financial_rate_record_service(
        scope=scope,
        tiie_rate=rate_data.tiie_rate,
        spread=rate_data.spread,
        grace_days=rate_data.grace_days,
        rate_name=rate_name,
        now=datetime.now(timezone.utc),
        monthly_to_annual=_monthly_to_annual,
    )
    rate_doc = financial_rate_payload["rate_doc"]
    tiie_monthly = financial_rate_payload["tiie_monthly"]
    spread_monthly = financial_rate_payload["spread_monthly"]

    created_rate_id = await _insert_financial_rate_repo(db, rate_doc)
    rate_doc["id"] = created_rate_id
    rate_doc["total_rate"] = financial_rate_payload["total_rate"]

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_financial_rate",
        entity_type="financial_rate",
        entity_id=created_rate_id,
        group_id=scope["group_id"],
        brand_id=scope["brand_id"],
        agency_id=scope["agency_id"],
        details={
            "name": rate_name,
            "rate_period": "monthly",
            "tiie_rate": tiie_monthly,
            "spread": spread_monthly,
            "grace_days": rate_data.grace_days,
            "total_rate": rate_doc["total_rate"],
        },
    )
    return serialize_doc(rate_doc)


async def apply_group_default_financial_rate(
    payload: FinancialRateBulkApplyRequest,
    request: Request,
):
    current_user = await get_current_user(request)
    if current_user["role"] not in FINANCIAL_RATE_MANAGER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    scope = await _resolve_financial_rate_scope(
        current_user=current_user,
        group_id=payload.group_id,
        brand_id=None,
        agency_id=None,
    )

    group_id = scope["group_id"]
    group_base_rate = await _find_latest_financial_rate_repo(
        db,
        group_id=group_id,
        brand_id=None,
        agency_id=None,
    )
    if not group_base_rate:
        raise HTTPException(
            status_code=400,
            detail="Primero crea una tasa general de grupo para poder aplicarla a marcas.",
        )

    brands = await _list_brands_for_group_repo(db, group_id=group_id, limit=1000)
    if not brands:
        return {
            "group_id": group_id,
            "created_count": 0,
            "skipped_count": 0,
            "message": "El grupo no tiene marcas para aplicar la tasa.",
        }

    existing_brand_rates = await _list_brand_financial_rates_for_group_repo(
        db,
        group_id=group_id,
        limit=5000,
    )
    existing_brand_ids = {
        str(rate.get("brand_id"))
        for rate in existing_brand_rates
        if rate.get("brand_id")
    }

    group_doc = await find_group_by_id(db, group_id)
    group_name = str(group_doc.get("name") or "Grupo") if group_doc else "Grupo"

    planned_defaults = _plan_group_default_rate_docs_service(
        group_id=group_id,
        group_name=group_name,
        group_base_rate=group_base_rate,
        brands=brands,
        existing_brand_ids=existing_brand_ids,
        now=datetime.now(timezone.utc),
        extract_rate_components_from_doc=_extract_rate_components_from_doc,
        monthly_to_annual=_monthly_to_annual,
    )
    docs_to_insert = planned_defaults["docs_to_insert"]
    skipped_count = planned_defaults["skipped_count"]
    base_tiie = planned_defaults["base_tiie"]
    base_spread = planned_defaults["base_spread"]
    base_grace_days = planned_defaults["base_grace_days"]

    created_count = 0
    if docs_to_insert:
        created_count = await _insert_many_financial_rates_repo(db, docs_to_insert)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="apply_group_default_financial_rate",
        entity_type="financial_rate",
        entity_id=str(group_base_rate.get("_id")),
        group_id=group_id,
        details={
            "group_base_rate_id": str(group_base_rate.get("_id")),
            "group_base_rate_name": group_base_rate.get("name"),
            "base_tiie_rate": base_tiie,
            "base_spread": base_spread,
            "base_grace_days": base_grace_days,
            "created_count": created_count,
            "skipped_count": skipped_count,
        },
    )

    return {
        "group_id": group_id,
        "created_count": created_count,
        "skipped_count": skipped_count,
        "message": "Tasa general aplicada a marcas sin tasa propia.",
    }

async def get_financial_rates(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    if current_user.get("role") in AGENCY_SCOPED_ROLES:
        raise HTTPException(status_code=403, detail="No autorizado para ver tasas financieras")
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        query["brand_id"] = brand_id
    elif group_id:
        query["group_id"] = group_id

    rates = await _list_financial_rates_repo(db, query=query, limit=1000)
    result = []
    for rate_doc in rates:
        enriched_rate = await _enrich_financial_rate_service(
            db,
            rate_doc=rate_doc,
            serialize_doc=serialize_doc,
            extract_rate_components_from_doc=_extract_rate_components_from_doc,
            monthly_to_annual=_monthly_to_annual,
            resolve_effective_rate_components_for_scope=_resolve_effective_rate_components_for_scope,
            find_group_by_id=find_group_by_id,
            find_brand_by_id=find_brand_by_id,
            find_agency_by_id=find_agency_by_id,
        )
        result.append(enriched_rate)

    return result

async def update_financial_rate(rate_id: str, rate_data: FinancialRateCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in FINANCIAL_RATE_MANAGER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rate_id):
        raise HTTPException(status_code=400, detail="Invalid rate_id")

    previous = await _find_financial_rate_by_id_repo(db, rate_id)
    if not previous:
        raise HTTPException(status_code=404, detail="Rate not found")
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta tasa")

    scope = await _resolve_financial_rate_scope(
        current_user=current_user,
        group_id=rate_data.group_id,
        brand_id=rate_data.brand_id,
        agency_id=rate_data.agency_id,
    )
    rate_name = str(rate_data.name or "").strip()
    if not rate_name:
        rate_name = await _build_default_financial_rate_name(
            group_id=scope["group_id"],
            brand_id=scope["brand_id"],
            agency_id=scope["agency_id"],
        )

    update_payload = _build_financial_rate_update_fields_service(
        scope=scope,
        tiie_rate=rate_data.tiie_rate,
        spread=rate_data.spread,
        grace_days=rate_data.grace_days,
        rate_name=rate_name,
        monthly_to_annual=_monthly_to_annual,
    )

    await _update_financial_rate_by_id_repo(
        db,
        rate_id=rate_id,
        set_fields=update_payload["update_fields"],
    )

    rate = await _find_financial_rate_by_id_repo(db, rate_id)
    if not rate:
        raise HTTPException(status_code=404, detail="Rate not found")
    result = await _enrich_financial_rate_service(
        db,
        rate_doc=rate,
        serialize_doc=serialize_doc,
        extract_rate_components_from_doc=_extract_rate_components_from_doc,
        monthly_to_annual=_monthly_to_annual,
        resolve_effective_rate_components_for_scope=_resolve_effective_rate_components_for_scope,
        find_group_by_id=find_group_by_id,
        find_brand_by_id=find_brand_by_id,
        find_agency_by_id=find_agency_by_id,
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_financial_rate",
        entity_type="financial_rate",
        entity_id=rate_id,
        group_id=rate.get("group_id") if rate else previous.get("group_id") if previous else None,
        brand_id=rate.get("brand_id") if rate else previous.get("brand_id") if previous else None,
        agency_id=rate.get("agency_id") if rate else previous.get("agency_id") if previous else None,
        details={
            "before": {
                "name": previous.get("name") if previous else None,
                "tiie_rate": previous.get("tiie_rate") if previous else None,
                "spread": previous.get("spread") if previous else None,
                "grace_days": previous.get("grace_days") if previous else None,
                "group_id": previous.get("group_id") if previous else None,
                "brand_id": previous.get("brand_id") if previous else None,
                "agency_id": previous.get("agency_id") if previous else None,
            },
            "after": {
                "name": rate.get("name") if rate else None,
                "tiie_rate": rate.get("tiie_rate") if rate else None,
                "spread": rate.get("spread") if rate else None,
                "grace_days": rate.get("grace_days") if rate else None,
                "group_id": rate.get("group_id") if rate else None,
                "brand_id": rate.get("brand_id") if rate else None,
                "agency_id": rate.get("agency_id") if rate else None,
            },
        },
    )
    return result

async def delete_financial_rate(rate_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in FINANCIAL_RATE_MANAGER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rate_id):
        raise HTTPException(status_code=400, detail="Invalid rate_id")

    previous = await _find_financial_rate_by_id_repo(db, rate_id)
    if not previous:
        raise HTTPException(status_code=404, detail="Rate not found")
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta tasa")
    await _delete_financial_rate_by_id_repo(db, rate_id=rate_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_financial_rate",
        entity_type="financial_rate",
        entity_id=rate_id,
        group_id=previous.get("group_id") if previous else None,
        brand_id=previous.get("brand_id") if previous else None,
        agency_id=previous.get("agency_id") if previous else None,
        details={
            "name": previous.get("name") if previous else None,
            "tiie_rate": previous.get("tiie_rate") if previous else None,
            "spread": previous.get("spread") if previous else None,
            "grace_days": previous.get("grace_days") if previous else None,
        },
    )
    return {"message": "Rate deleted"}

# ============== VEHICLES ROUTES ==============

def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    return _coerce_utc_datetime_service(value)


async def calculate_vehicle_financial_cost_in_period(
    vehicle: dict,
    period_start: datetime,
    period_end: datetime,
) -> float:
    return await _calculate_vehicle_financial_cost_in_period_service(
        vehicle,
        period_start,
        period_end,
        resolve_effective_rate_components_for_vehicle=_resolve_effective_rate_components_for_vehicle,
        days_per_month_for_rate=DAYS_PER_MONTH_FOR_RATE,
    )


async def calculate_vehicle_financial_cost(vehicle: dict) -> float:
    """Calculate financial cost using monthly rate (TIIE mensual + spread mensual)."""
    return await _calculate_vehicle_financial_cost_service(
        vehicle,
        resolve_effective_rate_components_for_vehicle=_resolve_effective_rate_components_for_vehicle,
        days_per_month_for_rate=DAYS_PER_MONTH_FOR_RATE,
    )

async def enrich_vehicle(vehicle: dict) -> dict:
    return await _enrich_vehicle_service(
        vehicle,
        serialize_doc=serialize_doc,
        find_agency_by_id=lambda agency_id: db.agencies.find_one({"_id": ObjectId(agency_id)}),
        find_brand_by_id=lambda brand_id: db.brands.find_one({"_id": ObjectId(brand_id)}),
        calculate_vehicle_financial_cost=calculate_vehicle_financial_cost,
        find_latest_sale_for_vehicle=lambda vehicle_id: db.sales.find_one({"vehicle_id": vehicle_id}, sort=[("sale_date", -1)]),
        sale_effective_revenue=_sale_effective_revenue,
        is_valid_object_id=ObjectId.is_valid,
        find_user_by_id=lambda user_id: db.users.find_one({"_id": ObjectId(user_id)}),
    )

async def create_vehicle(vehicle_data: VehicleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    _ensure_allowed_model_year(vehicle_data.year)
    
    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(vehicle_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")
    
    entry_date = vehicle_data.entry_date
    if entry_date:
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    else:
        entry_date = datetime.now(timezone.utc)
    
    vehicle_doc = {
        "vin": vehicle_data.vin,
        "model": vehicle_data.model,
        "year": vehicle_data.year,
        "trim": vehicle_data.trim,
        "color": vehicle_data.color,
        "vehicle_type": vehicle_data.vehicle_type,
        "purchase_price": vehicle_data.purchase_price,
        "agency_id": vehicle_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "entry_date": entry_date,
        "exit_date": None,
        "status": "in_stock",
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.vehicles.insert_one(vehicle_doc)
    vehicle_doc["_id"] = result.inserted_id

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_vehicle",
        entity_type="vehicle",
        entity_id=str(result.inserted_id),
        group_id=vehicle_doc.get("group_id"),
        brand_id=vehicle_doc.get("brand_id"),
        agency_id=vehicle_doc.get("agency_id"),
        details={
            "vin": vehicle_data.vin,
            "model": vehicle_data.model,
            "year": vehicle_data.year,
            "trim": vehicle_data.trim,
            "color": vehicle_data.color,
            "vehicle_type": vehicle_data.vehicle_type,
            "purchase_price": vehicle_data.purchase_price,
        },
    )
    return await enrich_vehicle(vehicle_doc)

async def get_vehicles(
    request: Request,
    agency_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    group_id: Optional[str] = None,
    status: Optional[str] = None,
    vehicle_type: Optional[str] = None,
    sold_current_month_only: bool = False,
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    if brand_id:
        query["brand_id"] = brand_id
    if group_id:
        query["group_id"] = group_id
    
    if status:
        query["status"] = status
    if vehicle_type:
        query["vehicle_type"] = vehicle_type

    if sold_current_month_only:
        now = datetime.now(timezone.utc)
        start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        if now.month == 12:
            end_of_month = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_of_month = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)

        sold_month_clause = {
            "status": "sold",
            "exit_date": {"$gte": start_of_month, "$lt": end_of_month},
        }

        if status == "sold":
            query["exit_date"] = {"$gte": start_of_month, "$lt": end_of_month}
        elif not status:
            query = {
                "$and": [
                    query,
                    {
                        "$or": [
                            {"status": {"$ne": "sold"}},
                            sold_month_clause,
                        ]
                    },
                ]
            }
    
    vehicles = await db.vehicles.find(query).to_list(1000)
    return [await enrich_vehicle(v) for v in vehicles]

async def get_vehicle(vehicle_id: str, request: Request):
    current_user = await get_current_user(request)
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    _ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")
    return await enrich_vehicle(vehicle)

async def apply_vehicle_aging_incentive(
    vehicle_id: str,
    payload: VehicleAgingIncentiveApply,
    request: Request,
):
    current_user = await get_current_user(request)
    allowed_roles = {
        UserRole.APP_ADMIN,
        UserRole.GROUP_ADMIN,
        UserRole.GROUP_FINANCE_MANAGER,
        UserRole.BRAND_ADMIN,
        UserRole.AGENCY_ADMIN,
        UserRole.AGENCY_GENERAL_MANAGER,
        UserRole.AGENCY_SALES_MANAGER,
    }
    if current_user.get("role") not in allowed_roles:
        raise HTTPException(status_code=403, detail="Not authorized")

    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    _ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")
    if vehicle.get("status") != "in_stock":
        raise HTTPException(status_code=400, detail="Solo se puede configurar incentivo aging para vehículos en stock")

    enriched_vehicle = await enrich_vehicle(vehicle)
    suggestion = await _build_vehicle_aging_suggestion(vehicle, enriched_vehicle=enriched_vehicle)
    if not suggestion:
        raise HTTPException(status_code=400, detail="No hay incentivo sugerido para este vehículo en este momento")

    sale_discount_amount = round(_to_non_negative_float(payload.sale_discount_amount, 0.0), 2)
    seller_bonus_amount = round(_to_non_negative_float(payload.seller_bonus_amount, 0.0), 2)
    total_amount = round(sale_discount_amount + seller_bonus_amount, 2)
    if total_amount <= 0:
        raise HTTPException(status_code=400, detail="Debes capturar un monto mayor a cero para venta o vendedor")

    suggested_amount = round(_to_non_negative_float(suggestion.get("suggested_bonus"), 0.0), 2)
    if total_amount - suggested_amount > 0.01:
        raise HTTPException(
            status_code=400,
            detail=f"El total aplicado ({total_amount}) no puede ser mayor al sugerido ({suggested_amount})",
        )

    plan_doc = {
        "active": True,
        "suggested_amount": suggested_amount,
        "sale_discount_amount": sale_discount_amount,
        "seller_bonus_amount": seller_bonus_amount,
        "total_amount": total_amount,
        "avg_days_to_sell": int(suggestion.get("avg_days_to_sell") or 0),
        "current_aging": int(suggestion.get("current_aging") or 0),
        "reason": suggestion.get("reason"),
        "notes": (payload.notes or "").strip() or None,
        "configured_by": current_user.get("id"),
        "configured_by_name": current_user.get("name"),
        "configured_at": datetime.now(timezone.utc),
        "applied_sale_id": None,
        "applied_at": None,
        "applied_sale_discount_amount": 0.0,
        "applied_seller_bonus_amount": 0.0,
    }

    await db.vehicles.update_one({"_id": ObjectId(vehicle_id)}, {"$set": {"aging_incentive_plan": plan_doc}})
    updated_vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="apply_vehicle_aging_incentive",
        entity_type="vehicle",
        entity_id=vehicle_id,
        group_id=vehicle.get("group_id"),
        brand_id=vehicle.get("brand_id"),
        agency_id=vehicle.get("agency_id"),
        details={
            "vin": vehicle.get("vin"),
            "model": vehicle.get("model"),
            "trim": vehicle.get("trim"),
            "suggested_amount": suggested_amount,
            "sale_discount_amount": sale_discount_amount,
            "seller_bonus_amount": seller_bonus_amount,
            "total_amount": total_amount,
            "notes": plan_doc.get("notes"),
        },
    )

    return await enrich_vehicle(updated_vehicle)

async def update_vehicle(vehicle_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    data = await request.json()
    update_data = {k: v for k, v in data.items() if k not in ["id", "_id"]}

    previous = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este vehículo")

    if update_data.get("agency_id"):
        target_agency = await db.agencies.find_one({"_id": ObjectId(update_data["agency_id"])})
        if not target_agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        _ensure_doc_scope_access(current_user, target_agency, agency_field="_id", detail="No puedes mover este vehículo a otra agencia")
        update_data["group_id"] = target_agency.get("group_id")
        update_data["brand_id"] = target_agency.get("brand_id")

    await db.vehicles.update_one({"_id": ObjectId(vehicle_id)}, {"$set": update_data})
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_vehicle",
        entity_type="vehicle",
        entity_id=vehicle_id,
        group_id=vehicle.get("group_id") if vehicle else previous.get("group_id") if previous else None,
        brand_id=vehicle.get("brand_id") if vehicle else previous.get("brand_id") if previous else None,
        agency_id=vehicle.get("agency_id") if vehicle else previous.get("agency_id") if previous else None,
        details={
            "changes": update_data,
            "vin": vehicle.get("vin") if vehicle else previous.get("vin") if previous else None,
            "status": vehicle.get("status") if vehicle else previous.get("status") if previous else None,
        },
    )
    return await enrich_vehicle(vehicle)

# ============== SALES OBJECTIVES ROUTES ==============

async def create_sales_objective(objective_data: SalesObjectiveCreate, request: Request):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in OBJECTIVE_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(objective_data.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")

    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(objective_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    seller_id = None
    if objective_data.seller_id:
        if not ObjectId.is_valid(objective_data.seller_id):
            raise HTTPException(status_code=400, detail="Invalid seller_id")
        seller = await db.users.find_one({"_id": ObjectId(objective_data.seller_id)})
        if not seller:
            raise HTTPException(status_code=404, detail="Seller not found")
        _ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
        if seller.get("role") != UserRole.SELLER:
            raise HTTPException(status_code=400, detail="Selected user is not a seller")
        if str(seller.get("agency_id") or "") != objective_data.agency_id:
            raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")
        seller_id = objective_data.seller_id

    normalized_vehicle_line = str(objective_data.vehicle_line or "").strip() or None

    now = datetime.now(timezone.utc)
    current_user_id = current_user.get("id")
    is_draft = bool(objective_data.save_as_draft)
    approval_status = OBJECTIVE_DRAFT if is_draft else OBJECTIVE_APPROVED
    objective_doc = {
        "seller_id": seller_id,
        "agency_id": objective_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "month": objective_data.month,
        "year": objective_data.year,
        "units_target": objective_data.units_target,
        "revenue_target": objective_data.revenue_target,
        "vehicle_line": normalized_vehicle_line,
        "approval_status": approval_status,
        "approval_comment": None,
        "created_by": current_user_id,
        "approved_by": None if is_draft else current_user_id,
        "approved_at": None if is_draft else now,
        "rejected_by": None,
        "rejected_at": None,
        "created_at": now,
        "updated_at": now,
        "updated_by": current_user_id,
    }
    result = await db.sales_objectives.insert_one(objective_doc)
    objective_doc["id"] = str(result.inserted_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_sales_objective",
        entity_type="sales_objective",
        entity_id=str(result.inserted_id),
        group_id=objective_doc.get("group_id"),
        brand_id=objective_doc.get("brand_id"),
        agency_id=objective_doc.get("agency_id"),
        details={
            "seller_id": seller_id,
            "month": objective_data.month,
            "year": objective_data.year,
            "units_target": objective_data.units_target,
            "revenue_target": objective_data.revenue_target,
            "vehicle_line": normalized_vehicle_line,
            "approval_status": approval_status,
            "save_as_draft": bool(objective_data.save_as_draft),
        },
    )
    return serialize_doc(objective_doc)

async def get_sales_objectives(
    request: Request,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    include_seller_objectives: bool = False,
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if current_user["role"] == UserRole.SELLER:
        current_seller_id = current_user.get("id")
        if not current_seller_id:
            return []
        if seller_id and seller_id != current_seller_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
        query["seller_id"] = current_seller_id
    elif seller_id:
        query["seller_id"] = seller_id
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        query["brand_id"] = brand_id
    elif group_id:
        query["group_id"] = group_id
    
    if month:
        query["month"] = month
    if year:
        query["year"] = year

    # Modo actual por defecto: solo objetivos de agencia-marca (sin seller_id).
    # seller_id explícito conserva lectura legado para auditoría/histórico.
    if not seller_id and not include_seller_objectives and "seller_id" not in query:
        query["$or"] = [
            {"seller_id": None},
            {"seller_id": {"$exists": False}},
        ]

    return await _list_sales_objectives_with_progress_service(
        db,
        objectives_query=query,
        objective_approved=OBJECTIVE_APPROVED,
        objective_pending=OBJECTIVE_PENDING,
        serialize_doc=serialize_doc,
        sale_effective_revenue=_sale_effective_revenue,
        list_sales_objectives=_list_sales_objectives_repo,
        find_user_by_id=_find_user_by_id_sales_objectives_repo,
        find_agency_by_id=_find_agency_by_id_sales_objectives_repo,
        find_brand_by_id=_find_brand_by_id_sales_objectives_repo,
        find_group_by_id=_find_group_by_id_sales_objectives_repo,
        list_sales=_list_sales_sales_objectives_repo,
    )

async def get_sales_objective_suggestion(
    request: Request,
    agency_id: str,
    seller_id: str,
    month: Optional[int] = None,
    year: Optional[int] = None,
    lookback_months: int = 6,
):
    current_user = await get_current_user(request)
    now = datetime.now(timezone.utc)
    target_month = int(month or now.month)
    target_year = int(year or now.year)
    safe_lookback = max(3, min(int(lookback_months), 24))

    if target_month < 1 or target_month > 12:
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")
    if target_year < 2000 or target_year > 2100:
        raise HTTPException(status_code=400, detail="Year must be between 2000 and 2100")
    if not ObjectId.is_valid(agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")
    if not ObjectId.is_valid(seller_id):
        raise HTTPException(status_code=400, detail="Invalid seller_id")

    agency = await _find_agency_by_id_sales_objectives_repo(db, agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    seller = await _find_user_by_id_sales_objectives_repo(db, seller_id)
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    _ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
    if seller.get("role") != UserRole.SELLER:
        raise HTTPException(status_code=400, detail="Selected user is not a seller")
    if str(seller.get("agency_id") or "") != agency_id:
        raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

    return await _build_sales_objective_suggestion_service(
        db,
        agency_id=agency_id,
        seller_id=seller_id,
        target_month=target_month,
        target_year=target_year,
        safe_lookback=safe_lookback,
        agency=agency,
        seller=seller,
        add_months_ym=_add_months_ym,
        sale_effective_revenue=_sale_effective_revenue,
        to_non_negative_float=_to_non_negative_float,
        is_price_bulletin_active=_is_price_bulletin_active_service,
        build_catalog_tree_from_source=_build_catalog_tree_from_source,
        find_catalog_make=_find_catalog_make,
        parse_catalog_price=_parse_catalog_price,
        list_sales=_list_sales_sales_objectives_repo,
        list_price_bulletins=_list_price_bulletins_sales_objectives_repo,
        find_brand_by_id=_find_brand_by_id_sales_objectives_repo,
    )

async def update_sales_objective(objective_id: str, objective_data: SalesObjectiveCreate, request: Request):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in OBJECTIVE_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(objective_id):
        raise HTTPException(status_code=400, detail="Invalid objective_id")

    previous = await db.sales_objectives.find_one({"_id": ObjectId(objective_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este objetivo")
    normalized_vehicle_line = str(objective_data.vehicle_line or "").strip() or None
    now = datetime.now(timezone.utc)
    is_draft = bool(objective_data.save_as_draft)
    update_fields: Dict[str, Any] = {
        "units_target": objective_data.units_target,
        "revenue_target": objective_data.revenue_target,
        "vehicle_line": normalized_vehicle_line,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }

    update_fields.update({
        "approval_status": OBJECTIVE_DRAFT if is_draft else OBJECTIVE_APPROVED,
        "approved_by": None if is_draft else current_user.get("id"),
        "approved_at": None if is_draft else now,
        "rejected_by": None,
        "rejected_at": None,
        "approval_comment": None,
    })

    await db.sales_objectives.update_one({"_id": ObjectId(objective_id)}, {"$set": update_fields})
    objective = await db.sales_objectives.find_one({"_id": ObjectId(objective_id)})

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_sales_objective",
        entity_type="sales_objective",
        entity_id=objective_id,
        group_id=objective.get("group_id") if objective else previous.get("group_id") if previous else None,
        brand_id=objective.get("brand_id") if objective else previous.get("brand_id") if previous else None,
        agency_id=objective.get("agency_id") if objective else previous.get("agency_id") if previous else None,
        details={
            "before": {
                "units_target": previous.get("units_target") if previous else None,
                "revenue_target": previous.get("revenue_target") if previous else None,
                "vehicle_line": previous.get("vehicle_line") if previous else None,
                "approval_status": previous.get("approval_status") if previous else None,
            },
            "after": {
                "units_target": objective.get("units_target") if objective else None,
                "revenue_target": objective.get("revenue_target") if objective else None,
                "vehicle_line": objective.get("vehicle_line") if objective else None,
                "approval_status": objective.get("approval_status") if objective else None,
            },
        },
    )
    return serialize_doc(objective)

async def approve_sales_objective(
    objective_id: str,
    approval: SalesObjectiveApprovalAction,
    request: Request
):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in OBJECTIVE_APPROVER_ROLES:
        raise HTTPException(status_code=403, detail="Solo Gerente General operativo del dealer puede aprobar")

    if not ObjectId.is_valid(objective_id):
        raise HTTPException(status_code=400, detail="Invalid objective_id")

    previous = await db.sales_objectives.find_one({"_id": ObjectId(objective_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este objetivo")

    decision = str(approval.decision or "").strip().lower()
    if decision not in {OBJECTIVE_APPROVED, OBJECTIVE_REJECTED}:
        raise HTTPException(status_code=400, detail="Decision must be approved or rejected")

    now = datetime.now(timezone.utc)
    update_fields: Dict[str, Any] = {
        "approval_status": decision,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }

    if decision == OBJECTIVE_APPROVED:
        update_fields.update({
            "approved_by": current_user.get("id"),
            "approved_at": now,
            "rejected_by": None,
            "rejected_at": None,
            "approval_comment": None,
        })
    else:
        comment = str(approval.comment or "").strip()
        if not comment:
            raise HTTPException(status_code=400, detail="Rejection requires a comment")
        update_fields.update({
            "approved_by": None,
            "approved_at": None,
            "rejected_by": current_user.get("id"),
            "rejected_at": now,
            "approval_comment": comment,
        })

    await db.sales_objectives.update_one({"_id": ObjectId(objective_id)}, {"$set": update_fields})
    objective = await db.sales_objectives.find_one({"_id": ObjectId(objective_id)})

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="approve_sales_objective",
        entity_type="sales_objective",
        entity_id=objective_id,
        group_id=objective.get("group_id") if objective else previous.get("group_id") if previous else None,
        brand_id=objective.get("brand_id") if objective else previous.get("brand_id") if previous else None,
        agency_id=objective.get("agency_id") if objective else previous.get("agency_id") if previous else None,
        details={
            "before_status": previous.get("approval_status") if previous else None,
            "after_status": objective.get("approval_status") if objective else None,
            "comment": update_fields.get("approval_comment"),
        },
    )
    return serialize_doc(objective)

# ============== COMMISSION RULES ROUTES ==============

def _to_non_negative_float(value: Any, default: float = 0.0) -> float:
    return _to_non_negative_float_service(value, default)

def _sale_effective_revenue(sale: Dict[str, Any]) -> float:
    return _sale_effective_revenue_service(sale)

def _sale_commission_base_price(sale: Dict[str, Any]) -> float:
    return _sale_commission_base_price_service(sale)

def _normalize_commission_matrix_volume_tiers(tiers: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return _normalize_commission_matrix_volume_tiers_service(tiers)

def _normalize_commission_matrix_general(general: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return _normalize_commission_matrix_general_service(general)

def _normalize_commission_matrix_models(models: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return _normalize_commission_matrix_models_service(
        models,
        default_plant_share_pct=COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT,
    )

def _get_catalog_models_for_brand(brand_name: Optional[str]) -> List[Dict[str, Any]]:
    return _get_catalog_models_for_brand_service(
        brand_name,
        build_catalog_tree_from_source=_build_catalog_tree_from_source,
        find_catalog_make=_find_catalog_make,
        parse_catalog_price=_parse_catalog_price,
    )

def _build_matrix_models_response(
    catalog_models: List[Dict[str, Any]],
    overrides: List[Dict[str, Any]],
    default_percentage: float,
) -> List[Dict[str, Any]]:
    return _build_matrix_models_response_service(
        catalog_models=catalog_models,
        overrides=overrides,
        default_percentage=default_percentage,
        default_plant_share_pct=COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT,
    )

def _resolve_matrix_volume_bonus_per_unit(volume_tiers: Optional[List[Dict[str, Any]]], seller_month_units: int) -> float:
    return _resolve_matrix_volume_bonus_per_unit_service(volume_tiers, seller_month_units)

async def _serialize_commission_matrix(agency: Dict[str, Any], matrix_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    brand_name = ""
    group_name = ""
    if agency.get("brand_id"):
        brand = await _find_brand_by_id_commission_repo(db, str(agency["brand_id"]))
        if brand:
            brand_name = str(brand.get("name") or "")
    if agency.get("group_id"):
        group = await _find_group_by_id_commission_repo(db, str(agency["group_id"]))
        if group:
            group_name = str(group.get("name") or "")

    general = _normalize_commission_matrix_general((matrix_doc or {}).get("general"))
    overrides = _normalize_commission_matrix_models((matrix_doc or {}).get("models"))
    catalog_models = _get_catalog_models_for_brand(brand_name)
    models_response = _build_matrix_models_response(catalog_models, overrides, general.get("global_percentage", 0.0))

    return {
        "agency_id": str(agency.get("_id")),
        "agency_name": agency.get("name"),
        "brand_id": str(agency.get("brand_id")) if agency.get("brand_id") else None,
        "brand_name": brand_name,
        "group_id": str(agency.get("group_id")) if agency.get("group_id") else None,
        "group_name": group_name,
        "general": general,
        "models": models_response,
        "updated_at": matrix_doc.get("updated_at").isoformat() if matrix_doc and isinstance(matrix_doc.get("updated_at"), datetime) else None,
        "updated_by": matrix_doc.get("updated_by") if matrix_doc else None,
    }

async def get_commission_matrix(request: Request, agency_id: str):
    current_user = await get_current_user(request)
    if not ObjectId.is_valid(agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")

    agency = await _find_agency_by_id_commission_repo(db, agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    matrix_doc = await _find_commission_matrix_by_agency_repo(db, agency_id=agency_id)
    return await _serialize_commission_matrix(agency, matrix_doc)

async def upsert_commission_matrix(payload: CommissionMatrixUpsert, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_MATRIX_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not ObjectId.is_valid(payload.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")

    agency = await _find_agency_by_id_commission_repo(db, payload.agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    now = datetime.now(timezone.utc)
    normalized_general = _normalize_commission_matrix_general(payload.general.model_dump())
    normalized_models = _normalize_commission_matrix_models([model.model_dump() for model in payload.models])

    matrix_upsert_payload = _build_commission_matrix_upsert_fields_service(
        agency_id=payload.agency_id,
        brand_id=agency.get("brand_id"),
        group_id=agency.get("group_id"),
        normalized_general=normalized_general,
        normalized_models=normalized_models,
        current_user_id=current_user.get("id"),
        now=now,
    )
    await _upsert_commission_matrix_by_agency_repo(
        db,
        agency_id=payload.agency_id,
        set_fields=matrix_upsert_payload["set_fields"],
        set_on_insert=matrix_upsert_payload["set_on_insert"],
    )

    matrix_doc = await _find_commission_matrix_by_agency_repo(db, agency_id=payload.agency_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="upsert_commission_matrix",
        entity_type="commission_matrix",
        entity_id=str(matrix_doc.get("_id")) if matrix_doc else None,
        group_id=str(agency.get("group_id")) if agency.get("group_id") else None,
        brand_id=str(agency.get("brand_id")) if agency.get("brand_id") else None,
        agency_id=payload.agency_id,
        details={
            "general": normalized_general,
            "models_count": len(normalized_models),
        },
    )

    return await _serialize_commission_matrix(agency, matrix_doc)

async def create_commission_rule(rule_data: CommissionRuleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_PROPOSER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rule_data.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")
    
    agency = await _find_agency_by_id_commission_repo(db, rule_data.agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    now = datetime.now(timezone.utc)
    rule_doc = _build_commission_rule_doc_service(
        agency_id=rule_data.agency_id,
        brand_id=agency.get("brand_id"),
        group_id=agency.get("group_id"),
        name=rule_data.name,
        rule_type=rule_data.rule_type,
        value=rule_data.value,
        min_units=rule_data.min_units,
        max_units=rule_data.max_units,
        current_user_id=current_user.get("id"),
        now=now,
        pending_status=COMMISSION_PENDING,
    )
    inserted_rule_id = await _insert_commission_rule_repo(db, rule_doc)
    rule_doc["id"] = inserted_rule_id

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_commission_rule",
        entity_type="commission_rule",
        entity_id=inserted_rule_id,
        group_id=rule_doc.get("group_id"),
        brand_id=rule_doc.get("brand_id"),
        agency_id=rule_doc.get("agency_id"),
        details={
            "name": rule_data.name,
            "rule_type": rule_data.rule_type,
            "value": rule_data.value,
            "min_units": rule_data.min_units,
            "max_units": rule_data.max_units,
            "approval_status": COMMISSION_PENDING,
        },
    )
    return serialize_doc(rule_doc)

async def _serialize_commission_rule(rule_doc: Dict[str, Any]) -> Dict[str, Any]:
    serialized = serialize_doc(rule_doc)
    serialized["approval_status"] = _normalize_commission_status_service(
        serialized.get("approval_status"),
        pending_status=COMMISSION_PENDING,
        approved_status=COMMISSION_APPROVED,
        rejected_status=COMMISSION_REJECTED,
    )

    if rule_doc.get("agency_id"):
        agency = await _find_agency_by_id_commission_repo(db, rule_doc["agency_id"])
        if agency:
            serialized["agency_name"] = agency["name"]
    if rule_doc.get("brand_id"):
        brand = await _find_brand_by_id_commission_repo(db, rule_doc["brand_id"])
        if brand:
            serialized["brand_name"] = brand["name"]
    if rule_doc.get("group_id"):
        group = await _find_group_by_id_commission_repo(db, rule_doc["group_id"])
        if group:
            serialized["group_name"] = group["name"]

    if rule_doc.get("submitted_by") and ObjectId.is_valid(rule_doc["submitted_by"]):
        submitter = await _find_user_by_id_commission_repo(db, rule_doc["submitted_by"])
        if submitter:
            serialized["submitted_by_name"] = submitter.get("name")
    if rule_doc.get("approved_by") and ObjectId.is_valid(rule_doc["approved_by"]):
        approver = await _find_user_by_id_commission_repo(db, rule_doc["approved_by"])
        if approver:
            serialized["approved_by_name"] = approver.get("name")
    if rule_doc.get("rejected_by") and ObjectId.is_valid(rule_doc["rejected_by"]):
        rejector = await _find_user_by_id_commission_repo(db, rule_doc["rejected_by"])
        if rejector:
            serialized["rejected_by_name"] = rejector.get("name")
    return serialized

async def get_commission_rules(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        query["brand_id"] = brand_id
    elif group_id:
        query["group_id"] = group_id
    
    rules = await _list_commission_rules_repo(db, query=query, limit=1000)
    result = [await _serialize_commission_rule(rule) for rule in rules]
    return result

async def update_commission_rule(rule_id: str, rule_data: CommissionRuleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_PROPOSER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rule_id):
        raise HTTPException(status_code=400, detail="Invalid rule_id")
    
    previous = await _find_commission_rule_by_id_repo(db, rule_id)
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
    now = datetime.now(timezone.utc)
    update_fields = _build_commission_rule_update_fields_service(
        name=rule_data.name,
        rule_type=rule_data.rule_type,
        value=rule_data.value,
        min_units=rule_data.min_units,
        max_units=rule_data.max_units,
        current_user_id=current_user.get("id"),
        now=now,
        pending_status=COMMISSION_PENDING,
    )
    await _update_commission_rule_by_id_repo(
        db,
        rule_id=rule_id,
        set_fields=update_fields,
    )
    rule = await _find_commission_rule_by_id_repo(db, rule_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="update_commission_rule",
        entity_type="commission_rule",
        entity_id=rule_id,
        group_id=rule.get("group_id") if rule else previous.get("group_id") if previous else None,
        brand_id=rule.get("brand_id") if rule else previous.get("brand_id") if previous else None,
        agency_id=rule.get("agency_id") if rule else previous.get("agency_id") if previous else None,
        details={
            "before": {
                "name": previous.get("name") if previous else None,
                "rule_type": previous.get("rule_type") if previous else None,
                "value": previous.get("value") if previous else None,
                "min_units": previous.get("min_units") if previous else None,
                "max_units": previous.get("max_units") if previous else None,
                "approval_status": previous.get("approval_status") if previous else None,
            },
            "after": {
                "name": rule.get("name") if rule else None,
                "rule_type": rule.get("rule_type") if rule else None,
                "value": rule.get("value") if rule else None,
                "min_units": rule.get("min_units") if rule else None,
                "max_units": rule.get("max_units") if rule else None,
                "approval_status": rule.get("approval_status") if rule else None,
            },
        },
    )
    return await _serialize_commission_rule(rule)

async def approve_commission_rule(
    rule_id: str,
    approval: CommissionApprovalAction,
    request: Request,
):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_APPROVER_ROLES:
        raise HTTPException(status_code=403, detail="Solo gerente general operativo puede aprobar reglas")

    if not ObjectId.is_valid(rule_id):
        raise HTTPException(status_code=400, detail="Invalid rule_id")

    previous = await _find_commission_rule_by_id_repo(db, rule_id)
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")

    now = datetime.now(timezone.utc)
    try:
        update_fields = _build_commission_approval_update_fields_service(
            decision=approval.decision,
            comment=approval.comment,
            current_user_id=current_user.get("id"),
            now=now,
            approved_status=COMMISSION_APPROVED,
            rejected_status=COMMISSION_REJECTED,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await _update_commission_rule_by_id_repo(
        db,
        rule_id=rule_id,
        set_fields=update_fields,
    )
    rule = await _find_commission_rule_by_id_repo(db, rule_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="approve_commission_rule",
        entity_type="commission_rule",
        entity_id=rule_id,
        group_id=rule.get("group_id") if rule else previous.get("group_id") if previous else None,
        brand_id=rule.get("brand_id") if rule else previous.get("brand_id") if previous else None,
        agency_id=rule.get("agency_id") if rule else previous.get("agency_id") if previous else None,
        details={
            "before_status": previous.get("approval_status") if previous else None,
            "after_status": rule.get("approval_status") if rule else None,
            "comment": update_fields.get("approval_comment"),
        },
    )
    return await _serialize_commission_rule(rule)

async def delete_commission_rule(rule_id: str, request: Request):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in COMMISSION_PROPOSER_ROLES and role not in COMMISSION_APPROVER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rule_id):
        raise HTTPException(status_code=400, detail="Invalid rule_id")
    
    previous = await _find_commission_rule_by_id_repo(db, rule_id)
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
    previous_status = str(previous.get("approval_status") or "").strip().lower()
    if role in COMMISSION_PROPOSER_ROLES and previous_status == COMMISSION_APPROVED:
        raise HTTPException(status_code=403, detail="Solo gerencia general puede borrar reglas aprobadas")
    await _delete_commission_rule_by_id_repo(db, rule_id=rule_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="delete_commission_rule",
        entity_type="commission_rule",
        entity_id=rule_id,
        group_id=previous.get("group_id") if previous else None,
        brand_id=previous.get("brand_id") if previous else None,
        agency_id=previous.get("agency_id") if previous else None,
        details={
            "name": previous.get("name") if previous else None,
            "rule_type": previous.get("rule_type") if previous else None,
            "value": previous.get("value") if previous else None,
        },
    )
    return {"message": "Rule deleted"}

async def commission_simulator(payload: CommissionSimulatorInput, request: Request):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in DEALER_SALES_EFFECTIVE_ROLES and role not in DEALER_GENERAL_EFFECTIVE_ROLES and role != DEALER_SELLER_ROLE:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(payload.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")
    agency = await _find_agency_by_id_commission_repo(db, payload.agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    if role == DEALER_SELLER_ROLE:
        seller_id = current_user.get("id")
        if not seller_id:
            raise HTTPException(status_code=400, detail="Seller identity not found")
    else:
        seller_id = payload.seller_id
        if seller_id and not ObjectId.is_valid(seller_id):
            raise HTTPException(status_code=400, detail="Invalid seller_id")
        if seller_id:
            seller = await _find_user_by_id_commission_repo(db, seller_id)
            if not seller:
                raise HTTPException(status_code=404, detail="Seller not found")
            _ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
            if seller.get("role") != UserRole.SELLER:
                raise HTTPException(status_code=400, detail="Selected user is not a seller")
            if seller.get("agency_id") != payload.agency_id:
                raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

    rules = await list_active_rules_by_agency(
        db,
        agency_id=payload.agency_id,
        approved_status=COMMISSION_APPROVED,
        limit=1000,
    )

    projection = _build_commission_simulator_projection_service(
        rules=rules,
        units=payload.units,
        average_ticket=payload.average_ticket,
        average_fi_revenue=payload.average_fi_revenue,
        target_commission=payload.target_commission,
        calculate_commission_from_rules=_calculate_commission_from_rules,
    )

    return {
        "agency_id": payload.agency_id,
        "seller_id": seller_id,
        "target_commission": payload.target_commission,
        "units": payload.units,
        "average_ticket": payload.average_ticket,
        "average_fi_revenue": payload.average_fi_revenue,
        "estimated_commission": projection["estimated_commission"],
        "difference_vs_target": projection["difference_vs_target"],
        "suggested_units_to_target": projection["suggested_units_to_target"],
    }

async def create_commission_closure(payload: CommissionClosureCreate, request: Request):
    current_user = await get_current_user(request)
    role = current_user.get("role")
    if role not in COMMISSION_PROPOSER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(payload.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")
    if not ObjectId.is_valid(payload.seller_id):
        raise HTTPException(status_code=400, detail="Invalid seller_id")

    agency = await _find_agency_by_id_commission_repo(db, payload.agency_id)
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    seller = await _find_user_by_id_commission_repo(db, payload.seller_id)
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    _ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
    if seller.get("role") != UserRole.SELLER:
        raise HTTPException(status_code=400, detail="Selected user is not a seller")
    if seller.get("agency_id") != payload.agency_id:
        raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

    start_date, end_date = _build_month_bounds_service(payload.year, payload.month)
    sales = await _list_sales_for_closure_repo(
        db,
        agency_id=payload.agency_id,
        seller_id=payload.seller_id,
        start_date=start_date,
        end_date=end_date,
        limit=10000,
    )
    now = datetime.now(timezone.utc)
    snapshot = _build_commission_closure_snapshot_service(
        sales=sales,
        now=now,
    )

    existing = await _find_commission_closure_by_scope_repo(
        db,
        seller_id=payload.seller_id,
        agency_id=payload.agency_id,
        month=payload.month,
        year=payload.year,
    )
    if existing and str(existing.get("approval_status") or "").strip().lower() == COMMISSION_APPROVED:
        raise HTTPException(status_code=409, detail="Approved closure cannot be modified")

    closure_doc = _build_commission_closure_doc_service(
        seller_id=payload.seller_id,
        agency_id=payload.agency_id,
        brand_id=agency.get("brand_id"),
        group_id=agency.get("group_id"),
        month=payload.month,
        year=payload.year,
        snapshot=snapshot,
        current_user_id=current_user.get("id"),
        now=now,
        pending_status=COMMISSION_PENDING,
    )

    if existing:
        await _update_commission_closure_by_id_repo(
            db,
            closure_id=str(existing["_id"]),
            set_fields=closure_doc,
        )
        closure = await _find_commission_closure_by_id_repo(db, str(existing["_id"]))
        entity_id = str(existing["_id"])
        action = "update_commission_closure"
    else:
        entity_id = await _insert_commission_closure_repo(db, closure_doc)
        closure = await _find_commission_closure_by_id_repo(db, entity_id)
        action = "create_commission_closure"

    await log_audit_event(
        request=request,
        current_user=current_user,
        action=action,
        entity_type="commission_closure",
        entity_id=entity_id,
        group_id=closure.get("group_id"),
        brand_id=closure.get("brand_id"),
        agency_id=closure.get("agency_id"),
        details={
            "seller_id": payload.seller_id,
            "month": payload.month,
            "year": payload.year,
            "snapshot": snapshot,
            "approval_status": COMMISSION_PENDING,
        },
    )
    return serialize_doc(closure)

async def get_commission_closures(
    request: Request,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if group_id:
        query["group_id"] = group_id
    if brand_id:
        query["brand_id"] = brand_id
    if agency_id:
        query["agency_id"] = agency_id
    if seller_id:
        query["seller_id"] = seller_id
    if month:
        query["month"] = month
    if year:
        query["year"] = year

    closures = await _list_commission_closures_repo(db, query=query, limit=1000)
    enriched: List[Dict[str, Any]] = []
    for closure in closures:
        closure["approval_status"] = _normalize_commission_status_service(
            closure.get("approval_status"),
            pending_status=COMMISSION_PENDING,
            approved_status=COMMISSION_APPROVED,
            rejected_status=COMMISSION_REJECTED,
        )

        if closure.get("seller_id") and ObjectId.is_valid(closure["seller_id"]):
            seller = await _find_user_by_id_commission_repo(db, closure["seller_id"])
            if seller:
                closure["seller_name"] = seller.get("name")
        if closure.get("agency_id") and ObjectId.is_valid(closure["agency_id"]):
            agency = await _find_agency_by_id_commission_repo(db, closure["agency_id"])
            if agency:
                closure["agency_name"] = agency.get("name")
        if closure.get("brand_id") and ObjectId.is_valid(closure["brand_id"]):
            brand = await _find_brand_by_id_commission_repo(db, closure["brand_id"])
            if brand:
                closure["brand_name"] = brand.get("name")
        if closure.get("group_id") and ObjectId.is_valid(closure["group_id"]):
            group = await _find_group_by_id_commission_repo(db, closure["group_id"])
            if group:
                closure["group_name"] = group.get("name")

        enriched.append(closure)
    return [serialize_doc(c) for c in enriched]

async def approve_commission_closure(
    closure_id: str,
    approval: CommissionClosureApprovalAction,
    request: Request,
):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_APPROVER_ROLES:
        raise HTTPException(status_code=403, detail="Solo gerente general operativo puede aprobar cierres")

    if not ObjectId.is_valid(closure_id):
        raise HTTPException(status_code=400, detail="Invalid closure_id")

    previous = await _find_commission_closure_by_id_repo(db, closure_id)
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este cierre")

    now = datetime.now(timezone.utc)
    try:
        update_fields = _build_commission_approval_update_fields_service(
            decision=approval.decision,
            comment=approval.comment,
            current_user_id=current_user.get("id"),
            now=now,
            approved_status=COMMISSION_APPROVED,
            rejected_status=COMMISSION_REJECTED,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await _update_commission_closure_by_id_repo(
        db,
        closure_id=closure_id,
        set_fields=update_fields,
    )
    closure = await _find_commission_closure_by_id_repo(db, closure_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="approve_commission_closure",
        entity_type="commission_closure",
        entity_id=closure_id,
        group_id=closure.get("group_id") if closure else previous.get("group_id") if previous else None,
        brand_id=closure.get("brand_id") if closure else previous.get("brand_id") if previous else None,
        agency_id=closure.get("agency_id") if closure else previous.get("agency_id") if previous else None,
        details={
            "before_status": previous.get("approval_status") if previous else None,
            "after_status": closure.get("approval_status") if closure else None,
            "comment": update_fields.get("approval_comment"),
        },
    )
    return serialize_doc(closure)

# ============== SALES ROUTES ==============

async def calculate_commission(
    sale: dict,
    agency_id: str,
    seller_id: str,
    *,
    vehicle: Optional[Dict[str, Any]] = None,
    sale_date: Optional[datetime] = None,
) -> float:
    return await _calculate_commission_service(
        db,
        sale=sale,
        agency_id=agency_id,
        seller_id=seller_id,
        vehicle=vehicle,
        sale_date=sale_date,
        approved_status=COMMISSION_APPROVED,
        normalize_general=_normalize_commission_matrix_general,
        normalize_models=_normalize_commission_matrix_models,
        resolve_volume_bonus_per_unit=_resolve_matrix_volume_bonus_per_unit,
        to_non_negative_float=_to_non_negative_float,
        sale_commission_base_price=_sale_commission_base_price,
        coerce_utc_datetime=_coerce_utc_datetime,
        default_plant_share_pct=COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT,
    )

def _extract_active_aging_incentive_plan(vehicle: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    plan = (vehicle or {}).get("aging_incentive_plan")
    if not isinstance(plan, dict):
        return None
    if not bool(plan.get("active")):
        return None

    sale_discount_amount = round(_to_non_negative_float(plan.get("sale_discount_amount"), 0.0), 2)
    seller_bonus_amount = round(_to_non_negative_float(plan.get("seller_bonus_amount"), 0.0), 2)
    total_amount = round(sale_discount_amount + seller_bonus_amount, 2)
    if total_amount <= 0:
        return None

    return {
        "sale_discount_amount": sale_discount_amount,
        "seller_bonus_amount": seller_bonus_amount,
        "total_amount": total_amount,
        "suggested_amount": round(_to_non_negative_float(plan.get("suggested_amount"), 0.0), 2),
        "configured_by": plan.get("configured_by"),
        "configured_by_name": plan.get("configured_by_name"),
        "configured_at": plan.get("configured_at"),
    }


def _apply_aging_plan_to_effective_pricing(
    effective_pricing: Dict[str, Any],
    aging_plan: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    pricing = dict(effective_pricing or {})
    applied_sale_discount = 0.0
    applied_seller_bonus = 0.0

    if aging_plan:
        transaction_price = _to_non_negative_float(pricing.get("transaction_price"), 0.0)
        planned_sale_discount = _to_non_negative_float(aging_plan.get("sale_discount_amount"), 0.0)
        applied_sale_discount = round(min(transaction_price, planned_sale_discount), 2)
        transaction_price = round(max(0.0, transaction_price - applied_sale_discount), 2)
        pricing["transaction_price"] = transaction_price

        brand_incentive_amount = _to_non_negative_float(pricing.get("brand_incentive_amount"), 0.0)
        pricing["commission_base_price"] = round(transaction_price + brand_incentive_amount, 2)
        pricing["effective_revenue"] = round(transaction_price + brand_incentive_amount, 2)

        applied_seller_bonus = round(_to_non_negative_float(aging_plan.get("seller_bonus_amount"), 0.0), 2)

    return pricing, {
        "sale_discount_amount": applied_sale_discount,
        "seller_bonus_amount": applied_seller_bonus,
        "total_amount": round(applied_sale_discount + applied_seller_bonus, 2),
    }

async def create_sale(sale_data: SaleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN, UserRole.SELLER]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    vehicle = await find_sales_vehicle_by_id(db, sale_data.vehicle_id)
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    _ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")

    if current_user["role"] == UserRole.SELLER and sale_data.seller_id != current_user.get("id"):
        raise HTTPException(status_code=403, detail="Seller can only register own sales")
    if sale_data.seller_id:
        target_seller = await find_user_by_id(db, sale_data.seller_id)
        if not target_seller:
            raise HTTPException(status_code=404, detail="Seller not found")
        _ensure_doc_scope_access(
            current_user,
            target_seller,
            detail="No tienes acceso a este vendedor",
        )
    creation_result = await create_sale_record(
        db,
        sale_data=sale_data.model_dump(),
        vehicle=vehicle,
        calculate_commission=calculate_commission,
        resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
        apply_manual_sale_price_override=_apply_manual_sale_price_override,
        extract_active_aging_incentive_plan=_extract_active_aging_incentive_plan,
        apply_aging_plan_to_effective_pricing=_apply_aging_plan_to_effective_pricing,
        to_non_negative_float=_to_non_negative_float,
    )
    sale_doc = creation_result["sale_doc"]
    sale_id = creation_result["sale_id"]
    effective_pricing = creation_result["effective_pricing"]
    applied_aging = creation_result["applied_aging"]
    resolved_sale_price = creation_result["resolved_sale_price"]
    base_commission = creation_result["base_commission"]
    commission = sale_doc.get("commission", 0)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_sale",
        entity_type="sale",
        entity_id=str(sale_id),
        group_id=sale_doc.get("group_id"),
        brand_id=sale_doc.get("brand_id"),
        agency_id=sale_doc.get("agency_id"),
        details={
            "vehicle_id": sale_data.vehicle_id,
            "seller_id": sale_data.seller_id,
            "sale_price": resolved_sale_price,
            "commission_base_price": round(_to_non_negative_float(effective_pricing.get("commission_base_price"), resolved_sale_price), 2),
            "effective_revenue": round(_to_non_negative_float(effective_pricing.get("effective_revenue"), resolved_sale_price), 2),
            "brand_incentive_amount": round(_to_non_negative_float(effective_pricing.get("brand_incentive_amount"), 0.0), 2),
            "dealer_incentive_amount": round(_to_non_negative_float(effective_pricing.get("dealer_incentive_amount"), 0.0), 2),
            "undocumented_dealer_incentive_amount": round(_to_non_negative_float(effective_pricing.get("undocumented_dealer_incentive_amount"), 0.0), 2),
            "aging_incentive_sale_discount_amount": round(_to_non_negative_float(applied_aging.get("sale_discount_amount"), 0.0), 2),
            "aging_incentive_seller_bonus_amount": round(_to_non_negative_float(applied_aging.get("seller_bonus_amount"), 0.0), 2),
            "aging_incentive_total_amount": round(_to_non_negative_float(applied_aging.get("total_amount"), 0.0), 2),
            "fi_revenue": sale_data.fi_revenue,
            "plant_incentive": sale_data.plant_incentive,
            "base_commission": base_commission,
            "commission": commission,
        },
    )

    sale_doc["id"] = str(sale_id)
    return serialize_doc(sale_doc)

async def get_sales(
    request: Request,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    if current_user["role"] == UserRole.SELLER and current_user.get("id"):
        query["seller_id"] = current_user["id"]

    if current_user["role"] == UserRole.SELLER:
        current_seller_id = current_user.get("id")
        if not current_seller_id:
            return []
        if seller_id and seller_id != current_seller_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
        query["seller_id"] = current_seller_id
    elif seller_id:
        query["seller_id"] = seller_id
    if month and year:
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        query["sale_date"] = {"$gte": start_date, "$lt": end_date}

    return await list_sales_with_enrichment(
        db,
        query=query,
        serialize_doc=serialize_doc,
        limit=1000,
    )

# ============== DASHBOARD / ANALYTICS ROUTES ==============

async def _resolve_dashboard_scope_group_id(scope_query: Dict[str, Any]) -> Optional[str]:
    return await _resolve_dashboard_scope_group_id_service(
        db,
        scope_query=scope_query,
        find_brand_group_id=find_brand_group_id,
        find_agency_group_id=find_agency_group_id,
    )


async def _find_dashboard_monthly_close(
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


async def get_dashboard_monthly_close(
    request: Request,
    month: Optional[int] = None,
    year: Optional[int] = None,
    group_id: Optional[str] = None,
):
    current_user = await get_current_user(request)
    now = datetime.now(timezone.utc)
    target_month = int(month or now.month)
    target_year = int(year or now.year)

    if target_month < 1 or target_month > 12:
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")

    _validate_scope_filters(current_user, group_id=group_id)
    effective_group_id = group_id
    if not effective_group_id:
        scope_query = _build_scope_query(current_user)
        effective_group_id = await _resolve_dashboard_scope_group_id(scope_query)

    close_doc, close_scope = await _find_dashboard_monthly_close(
        year=target_year,
        month=target_month,
        group_id=effective_group_id,
    )

    return _build_dashboard_monthly_close_response_service(
        target_year=target_year,
        target_month=target_month,
        effective_group_id=effective_group_id,
        close_doc=close_doc,
        close_scope=close_scope,
    )


async def get_dashboard_monthly_close_calendar(
    request: Request,
    year: Optional[int] = None,
    from_current_month: bool = True,
):
    await get_current_user(request)

    now = datetime.now(timezone.utc)
    target_year = int(year or now.year)
    if target_year < 2020 or target_year > 2100:
        raise HTTPException(status_code=400, detail="Year must be between 2020 and 2100")

    start_month = now.month if (from_current_month and target_year == now.year) else 1
    holidays_by_month = _mexico_lft_holidays_by_month(target_year)

    docs = await list_global_monthly_closes_by_year(db, year=target_year, limit=1000)
    return _build_dashboard_monthly_close_calendar_service(
        target_year=target_year,
        start_month=start_month,
        docs=docs,
        holidays_by_month=holidays_by_month,
    )


async def upsert_dashboard_monthly_close(payload: DashboardMonthlyCloseUpsert, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Only app_admin can update monthly close values")

    target_group_id = None  # calendario operativo global para todas las marcas y grupos
    days_in_month = monthrange(int(payload.year), int(payload.month))[1]
    if payload.fiscal_close_day is not None and payload.fiscal_close_day > days_in_month:
        raise HTTPException(status_code=400, detail=f"fiscal_close_day must be <= {days_in_month}")
    industry_target_year, industry_target_month = _add_months_ym(
        int(payload.year),
        int(payload.month),
        int(payload.industry_close_month_offset or 0),
    )
    industry_days_in_target_month = monthrange(industry_target_year, industry_target_month)[1]
    if payload.industry_close_day is not None and payload.industry_close_day > industry_days_in_target_month:
        raise HTTPException(
            status_code=400,
            detail=(
                "industry_close_day must be <= "
                f"{industry_days_in_target_month} for target month {industry_target_year}-{industry_target_month:02d}"
            ),
        )

    now = datetime.now(timezone.utc)
    updated = await upsert_global_monthly_close(
        db,
        year=int(payload.year),
        month=int(payload.month),
        fiscal_close_day=payload.fiscal_close_day,
        industry_close_day=payload.industry_close_day,
        industry_close_month_offset=int(payload.industry_close_month_offset or 0),
        updated_by=current_user.get("id"),
        now=now,
    )
    await log_audit_event(
        request=request,
        current_user=current_user,
        action="upsert_dashboard_monthly_close",
        entity_type="dashboard_monthly_close",
        entity_id=f"{payload.year}-{payload.month:02d}:{target_group_id or 'global'}",
        group_id=target_group_id,
        details={
            "year": payload.year,
            "month": payload.month,
            "group_id": None,
            "fiscal_close_day": payload.fiscal_close_day,
            "industry_close_day": payload.industry_close_day,
            "industry_close_month_offset": int(payload.industry_close_month_offset or 0),
        },
    )

    return {
        "year": payload.year,
        "month": payload.month,
        "group_id": None,
        "fiscal_close_day": updated.get("fiscal_close_day") if updated else payload.fiscal_close_day,
        "industry_close_day": updated.get("industry_close_day") if updated else payload.industry_close_day,
        "industry_close_month_offset": int(
            updated.get("industry_close_month_offset") or (payload.industry_close_month_offset or 0)
        ) if updated else int(payload.industry_close_month_offset or 0),
        "updated_at": updated.get("updated_at") if updated else now,
    }

async def get_dashboard_kpis(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    if current_user["role"] == UserRole.SELLER:
        current_seller_id = current_user.get("id")
        if seller_id and seller_id != current_seller_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
        seller_id = current_seller_id
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return _empty_dashboard_kpis_response_service()

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        query["brand_id"] = brand_id
    elif group_id:
        query["group_id"] = group_id

    return await _compute_dashboard_kpis_service(
        db,
        query=query,
        seller_id=seller_id,
        now=datetime.now(timezone.utc),
        user_role_seller=UserRole.SELLER,
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
        resolve_dashboard_scope_group_id=_resolve_dashboard_scope_group_id,
        find_dashboard_monthly_close=_find_dashboard_monthly_close,
    )

async def get_sales_trends(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    months: int = 6,
    granularity: str = "month"
):
    current_user = await get_current_user(request)
    query = _build_scope_query(current_user)
    if not _scope_query_has_access(query):
        return []

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        query["brand_id"] = brand_id
    elif group_id:
        query["group_id"] = group_id

    if current_user["role"] == UserRole.SELLER:
        current_seller_id = current_user.get("id")
        if not current_seller_id:
            return []
        if seller_id and seller_id != current_seller_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
        query["seller_id"] = current_seller_id
    elif seller_id:
        query["seller_id"] = seller_id

    return await _compute_sales_trends_service(
        db,
        query=query,
        now=datetime.now(timezone.utc),
        months=months,
        granularity=granularity,
        objective_approved=OBJECTIVE_APPROVED,
        objective_pending=OBJECTIVE_PENDING,
        list_sales=_list_sales_dashboard_repo,
        list_sales_objectives=_list_sales_objectives_dashboard_repo,
        coerce_utc_datetime=_coerce_utc_datetime,
        sale_effective_revenue=_sale_effective_revenue,
        decrement_month=_decrement_month,
        compute_operational_day_profile=_compute_operational_day_profile,
        resolve_effective_objective_units=_resolve_effective_objective_units,
    )

async def get_seller_performance(request: Request, agency_id: Optional[str] = None, month: Optional[int] = None, year: Optional[int] = None):
    current_user = await get_current_user(request)
    
    now = datetime.now(timezone.utc)
    if not month:
        month = now.month
    if not year:
        year = now.year
    
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    
    query = {"sale_date": {"$gte": start_date, "$lt": end_date}}
    scope_query = _build_scope_query(current_user)
    if not _scope_query_has_access(scope_query):
        return []
    query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

    _validate_scope_filters(current_user, agency_id=agency_id)
    if agency_id:
        query["agency_id"] = agency_id

    return await _compute_seller_performance_service(
        db,
        query=query,
        list_sales=_list_sales_dashboard_repo,
        find_user_by_id=_find_user_by_id_dashboard_repo,
        sale_effective_revenue=_sale_effective_revenue,
    )


async def _build_vehicle_aging_suggestion(
    vehicle: Dict[str, Any],
    *,
    enriched_vehicle: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if enriched_vehicle is None:
        enriched_vehicle = await enrich_vehicle(vehicle)
    return await _build_vehicle_aging_suggestion_service(
        db,
        vehicle=vehicle,
        enriched_vehicle=enriched_vehicle,
        list_similar_sold_vehicles=_list_similar_sold_vehicles_dashboard_repo,
        to_non_negative_float=_to_non_negative_float,
        now=datetime.now(timezone.utc),
    )

async def get_vehicle_suggestions(
    request: Request,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    limit: int = 20,
):
    """Get smart suggestions for vehicles that should be promoted/discounted"""
    current_user = await get_current_user(request)
    
    query = {"status": "in_stock"}
    scope_query = _build_scope_query(current_user)
    if not _scope_query_has_access(scope_query):
        return []
    query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

    _validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
    if group_id:
        query["group_id"] = group_id
    if brand_id:
        query["brand_id"] = brand_id
    if agency_id:
        query["agency_id"] = agency_id
    
    return await _collect_vehicle_suggestions_service(
        db,
        query=query,
        limit=limit,
        list_vehicles=_list_vehicles_dashboard_repo,
        enrich_vehicle=enrich_vehicle,
        build_vehicle_aging_suggestion=_build_vehicle_aging_suggestion,
    )

# ============== IMPORT ROUTES ==============

async def import_organization(request: Request, file: UploadFile = File(...)):
    """
    Import organizational structure from an Excel file with optional sheets:
    - groups:  name, description
    - brands:  name, group_id|group_name, logo_url
    - agencies: name, brand_id|brand_name, group_id|group_name, city, address
    - sellers: email, name, password, agency_id|agency_name, brand_id|brand_name, group_id|group_name, role
    """
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    content = await file.read()
    response = await import_organization_from_excel(
        db,
        current_user=current_user,
        filename=file.filename,
        content=content,
        resolve_agency_location=_resolve_agency_location,
        hash_password=hash_password,
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="import_organization",
        entity_type="organization_import",
        group_id=current_user.get("group_id"),
        details={
            "filename": file.filename,
            "summary": response.get("summary", {}),
            "errors_count": len(response.get("errors", [])),
        },
    )
    return response

async def import_vehicles(request: Request, file: UploadFile = File(...)):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    content = await file.read()
    response = await import_vehicles_from_file(
        db,
        filename=file.filename,
        content=content,
        allowed_model_year=get_catalog_model_year(),
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="import_vehicles",
        entity_type="vehicle_import",
        group_id=current_user.get("group_id"),
        details={
            "filename": file.filename,
            "imported": response.get("imported", 0),
            "total_rows": response.get("total_rows", 0),
            "errors_count": len(response.get("errors", [])),
        },
    )
    return response

async def import_sales(request: Request, file: UploadFile = File(...)):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    content = await file.read()
    response = await import_sales_from_file(
        db,
        filename=file.filename,
        content=content,
        calculate_commission=calculate_commission,
        resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
        apply_manual_sale_price_override=_apply_manual_sale_price_override,
        extract_active_aging_incentive_plan=_extract_active_aging_incentive_plan,
        apply_aging_plan_to_effective_pricing=_apply_aging_plan_to_effective_pricing,
        to_non_negative_float=_to_non_negative_float,
    )

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="import_sales",
        entity_type="sales_import",
        group_id=current_user.get("group_id"),
        details={
            "filename": file.filename,
            "imported": response.get("imported", 0),
            "total_rows": response.get("total_rows", 0),
            "errors_count": len(response.get("errors", [])),
        },
    )
    return response

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

# Mount logos directory as static assets so frontend can render brand logos
_resolved_logo_dir = _resolve_logo_directory()
if _resolved_logo_dir:
    app.mount("/logos", StaticFiles(directory=str(_resolved_logo_dir)), name="brand-logos")
    logger.info("Brand logos directory mounted: %s", _resolved_logo_dir)
else:
    logger.warning(
        "Brand logos directory not found. Configure %s or place logos under cortex_frontend/public/logos.",
        LOGO_DIRECTORY_ENV,
    )

# Include the router in the main app
app.include_router(api_router)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=_build_allowed_origins_service(os.environ.get("FRONTEND_URL")),
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== STARTUP ==============

@app.on_event("startup")
async def startup():
    await _create_core_indexes_service(db=db)
    backfill_summary = await backfill_agency_locations()
    logger.info(
        "Agency location backfill: checked=%s updated=%s city=%s postal_code=%s",
        backfill_summary["checked"],
        backfill_summary["updated"],
        backfill_summary["filled_city"],
        backfill_summary["filled_postal_code"],
    )
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@autoconnect.com")
    admin_password = os.environ.get("ADMIN_PASSWORD", "Admin123!")
    seed_status = await _seed_admin_user_service(
        db=db,
        admin_email=admin_email,
        admin_password=admin_password,
        app_admin_role=UserRole.APP_ADMIN,
        hash_password=hash_password,
        verify_password=verify_password,
    )
    if seed_status == "created":
        logger.info("Admin user created: %s", admin_email)
    elif seed_status == "password_updated":
        logger.info("Admin password updated: %s", admin_email)
    logger.info("AutoConnect API started")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
