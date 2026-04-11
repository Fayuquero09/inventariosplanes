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
import bcrypt
import jwt
import secrets
import re
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple
from calendar import monthrange
from pydantic import BaseModel, Field, EmailStr
import json
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
from repositories.commission_repository import list_active_rules_by_agency
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
    resolve_price_bulletin_for_model as _resolve_price_bulletin_for_model_service,
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
catalog_cache: Dict[str, Any] = {
    "source_path": None,
    "model_year": None,
    "all_years": False,
    "mtime": None,
    "payload": None,
}
logo_assets_cache: Dict[str, Any] = {
    "directory": None,
    "files": None,
}

LOGO_FILE_EXTENSIONS = {".png", ".svg", ".webp", ".jpg", ".jpeg"}
LOGO_SLUG_ALIASES: Dict[str, List[str]] = {
    "changan": ["changang"],
    "changang": ["changan"],
    "gac-motor": ["gac"],
    "gac": ["gac-motor"],
}

def get_jwt_secret() -> str:
    return os.environ.get("JWT_SECRET", "default-secret-change-me")

def get_catalog_source_path() -> str:
    return os.environ.get("STRAPI_JATO_CATALOG_PATH", CATALOG_DEFAULT_SOURCE_PATH)

def get_catalog_model_year() -> int:
    raw_value = os.environ.get("CATALOG_MODEL_YEAR", str(CATALOG_DEFAULT_MODEL_YEAR))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return CATALOG_DEFAULT_MODEL_YEAR

def _slugify_asset_name(value: str) -> str:
    import unicodedata

    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized

def _build_logo_directory_candidates() -> List[Path]:
    candidates: List[Path] = []
    direct_logo_dir = os.environ.get(LOGO_DIRECTORY_ENV)
    if direct_logo_dir:
        candidates.append(Path(direct_logo_dir))

    roots = [
        os.environ.get("CORTEX_AUTOMOTRIZ_ROOT"),
        CORTEX_ROOT_DEFAULT_PATH,
    ]
    for root in roots:
        if not root:
            continue
        base = Path(root)
        candidates.extend(
            [
                base / "cortex_frontend" / "public" / "logos",
                base / "dataframe_base_backup_20251006" / "cortex_frontend" / "public" / "logos",
                base / "dataframe_base_backup_only_db_20251006" / "cortex_frontend" / "public" / "logos",
                base / "strapi" / "cortex_frontend" / "public" / "logos",
                base / "Strapi" / "cortex_frontend" / "public" / "logos",
            ]
        )

    deduped: List[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped

def _count_logo_files(directory: Path) -> int:
    count = 0
    try:
        for child in directory.iterdir():
            if child.is_file() and child.suffix.lower() in LOGO_FILE_EXTENSIONS:
                count += 1
    except Exception:
        return 0
    return count

def _resolve_logo_directory() -> Optional[Path]:
    best_dir: Optional[Path] = None
    best_count = 0

    for candidate in _build_logo_directory_candidates():
        try:
            if not candidate.exists() or not candidate.is_dir():
                continue
        except Exception:
            continue
        candidate_count = _count_logo_files(candidate)
        if candidate_count > best_count:
            best_count = candidate_count
            best_dir = candidate

    return best_dir

def _load_logo_filename_index() -> Dict[str, str]:
    directory = _resolve_logo_directory()
    cached_directory = logo_assets_cache.get("directory")
    cached_files = logo_assets_cache.get("files")
    directory_key = str(directory) if directory else None

    if cached_directory == directory_key and isinstance(cached_files, dict):
        return cached_files

    files: Dict[str, str] = {}
    if directory:
        try:
            for child in directory.iterdir():
                if not child.is_file():
                    continue
                if child.suffix.lower() not in LOGO_FILE_EXTENSIONS:
                    continue
                files[child.name.lower()] = child.name
        except Exception:
            files = {}

    logo_assets_cache["directory"] = directory_key
    logo_assets_cache["files"] = files
    return files

def _resolve_logo_filename_for_brand(brand_name: str) -> Optional[str]:
    files = _load_logo_filename_index()
    if not files:
        return None

    slug = _slugify_asset_name(brand_name)
    if not slug:
        return None

    candidate_slugs = [slug]
    candidate_slugs.extend(LOGO_SLUG_ALIASES.get(slug, []))

    for slug_candidate in candidate_slugs:
        filename_candidates = [
            f"{slug_candidate}-logo.png",
            f"{slug_candidate}.png",
            f"{slug_candidate}-logo.svg",
            f"{slug_candidate}.svg",
            f"{slug_candidate}-logo.webp",
            f"{slug_candidate}.webp",
            f"{slug_candidate}-logo.jpg",
            f"{slug_candidate}.jpg",
            f"{slug_candidate}-logo.jpeg",
            f"{slug_candidate}.jpeg",
        ]
        for filename in filename_candidates:
            resolved = files.get(filename.lower())
            if resolved:
                return resolved
    return None

def _resolve_logo_url_for_brand(brand_name: str, request: Optional[Request] = None) -> Optional[str]:
    filename = _resolve_logo_filename_for_brand(brand_name)
    if not filename:
        return None
    relative_path = f"/logos/{filename}"
    if not request:
        return relative_path
    return f"{str(request.base_url).rstrip('/')}{relative_path}"

# Password functions
def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

# JWT functions
def create_access_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "type": "access"
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=JWT_ALGORITHM)

def create_refresh_token(user_id: str) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        "type": "refresh"
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=JWT_ALGORITHM)

# Create the main app
app = FastAPI(title="AutoConnect - Vehicle Inventory Management")

# Session middleware for OAuth
app.add_middleware(SessionMiddleware, secret_key=get_jwt_secret())

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# ============== PYDANTIC MODELS ==============

class UserRole:
    APP_ADMIN = "app_admin"
    APP_USER = "app_user"
    GROUP_ADMIN = "group_admin"
    GROUP_FINANCE_MANAGER = "group_finance_manager"
    BRAND_ADMIN = "brand_admin"
    AGENCY_ADMIN = "agency_admin"
    AGENCY_SALES_MANAGER = "agency_sales_manager"
    AGENCY_GENERAL_MANAGER = "agency_general_manager"
    AGENCY_COMMERCIAL_MANAGER = "agency_commercial_manager"
    GROUP_USER = "group_user"
    BRAND_USER = "brand_user"
    AGENCY_USER = "agency_user"
    SELLER = "seller"

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: str
    position: Optional[str] = None
    role: str = UserRole.APP_USER
    group_id: Optional[str] = None
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class PasswordResetRequest(BaseModel):
    email: EmailStr
    new_password: str

class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    position: Optional[str] = None
    role: str
    group_id: Optional[str] = None
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    created_at: str

class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

class GroupResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: str

class BrandCreate(BaseModel):
    name: str
    group_id: str
    logo_url: Optional[str] = None

class BrandResponse(BaseModel):
    id: str
    name: str
    group_id: str
    logo_url: Optional[str] = None
    created_at: str

class AgencyCreate(BaseModel):
    name: str
    brand_id: str
    address: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None
    exterior_number: Optional[str] = None
    interior_number: Optional[str] = None
    neighborhood: Optional[str] = None
    municipality: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    google_place_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class AgencyResponse(BaseModel):
    id: str
    name: str
    brand_id: str
    address: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None
    exterior_number: Optional[str] = None
    interior_number: Optional[str] = None
    neighborhood: Optional[str] = None
    municipality: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    google_place_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    created_at: str

class FinancialRateCreate(BaseModel):
    group_id: str
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    tiie_rate: Optional[float] = None  # Tasa TIIE mensual (%), opcional para heredar
    spread: Optional[float] = None  # Spread mensual adicional (%), opcional para heredar
    grace_days: int = 0  # Días de gracia
    name: str

class FinancialRateBulkApplyRequest(BaseModel):
    group_id: str

class FinancialRateResponse(BaseModel):
    id: str
    group_id: str
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    tiie_rate: Optional[float] = None  # mensual configurada (nulo = hereda)
    spread: Optional[float] = None  # mensual configurado (nulo = hereda)
    total_rate: Optional[float] = None  # mensual configurada (nulo = hereda)
    effective_tiie_rate: Optional[float] = None  # mensual efectiva tras herencia
    effective_spread: Optional[float] = None  # mensual efectivo tras herencia
    effective_total_rate: Optional[float] = None  # mensual efectiva tras herencia
    effective_grace_days: Optional[int] = None
    rate_period: Optional[str] = "monthly"
    tiie_rate_annual: Optional[float] = None
    spread_annual: Optional[float] = None
    total_rate_annual: Optional[float] = None
    grace_days: int
    name: str
    created_at: str

class VehicleCreate(BaseModel):
    vin: str
    model: str
    year: int
    trim: str
    color: str
    vehicle_type: str  # new, used
    purchase_price: float
    agency_id: str
    entry_date: Optional[str] = None

class VehicleResponse(BaseModel):
    id: str
    vin: str
    model: str
    year: int
    trim: str
    color: str
    vehicle_type: str
    purchase_price: float
    agency_id: str
    agency_name: Optional[str] = None
    brand_id: Optional[str] = None
    brand_name: Optional[str] = None
    group_id: Optional[str] = None
    entry_date: str
    exit_date: Optional[str] = None
    status: str  # in_stock, sold, transferred
    aging_days: int
    financial_cost: float
    sale_commission: Optional[float] = None
    sale_price: Optional[float] = None
    sale_date: Optional[str] = None
    sold_by_name: Optional[str] = None
    created_at: str

class SalesObjectiveCreate(BaseModel):
    seller_id: Optional[str] = None
    agency_id: str
    month: int
    year: int
    units_target: int
    revenue_target: float
    vehicle_line: Optional[str] = None
    save_as_draft: bool = False

class SalesObjectiveApprovalAction(BaseModel):
    decision: str  # approved | rejected
    comment: Optional[str] = None

class SalesObjectiveResponse(BaseModel):
    id: str
    seller_id: Optional[str] = None
    seller_name: Optional[str] = None
    agency_id: str
    agency_name: Optional[str] = None
    brand_id: Optional[str] = None
    brand_name: Optional[str] = None
    group_id: Optional[str] = None
    month: int
    year: int
    units_target: int
    revenue_target: float
    vehicle_line: Optional[str] = None
    approval_status: str = "approved"
    approval_comment: Optional[str] = None
    created_by: Optional[str] = None
    created_by_name: Optional[str] = None
    approved_by: Optional[str] = None
    approved_by_name: Optional[str] = None
    approved_at: Optional[str] = None
    rejected_by: Optional[str] = None
    rejected_by_name: Optional[str] = None
    rejected_at: Optional[str] = None
    units_sold: int = 0
    revenue_achieved: float = 0
    progress_units: float = 0
    progress_revenue: float = 0
    created_at: str

class CommissionRuleCreate(BaseModel):
    agency_id: str
    name: str
    rule_type: str  # per_unit, percentage, volume_bonus, fi_bonus
    value: float
    min_units: Optional[int] = None
    max_units: Optional[int] = None

class CommissionMatrixVolumeTierConfig(BaseModel):
    min_units: int = Field(..., ge=1)
    max_units: Optional[int] = Field(default=None, ge=1)
    bonus_per_unit: float = Field(0, ge=0)

class CommissionMatrixGeneralConfig(BaseModel):
    global_percentage: float = 0
    global_per_unit_bonus: float = 0
    global_aged_61_90_bonus: float = 0
    global_aged_90_plus_bonus: float = 0
    volume_tiers: List[CommissionMatrixVolumeTierConfig] = Field(default_factory=list)

class CommissionMatrixModelConfig(BaseModel):
    model: str
    model_percentage: Optional[float] = None
    model_bonus: float = 0
    aged_61_90_bonus: float = 0
    aged_90_plus_bonus: float = 0
    plant_incentive_share_pct: float = 100

class CommissionMatrixUpsert(BaseModel):
    agency_id: str
    general: CommissionMatrixGeneralConfig = Field(default_factory=CommissionMatrixGeneralConfig)
    models: List[CommissionMatrixModelConfig] = Field(default_factory=list)

class PriceBulletinItem(BaseModel):
    model: str
    version: Optional[str] = None
    msrp: float = Field(0, ge=0)
    transaction_price: Optional[float] = Field(default=None, ge=0)
    brand_bonus_amount: float = Field(0, ge=0)
    brand_bonus_percentage: float = Field(0, ge=0)
    dealer_bonus_amount: float = Field(0, ge=0)
    dealer_share_percentage: float = Field(0, ge=0, le=100)

class PriceBulletinBulkUpsert(BaseModel):
    group_id: str
    brand_id: str
    agency_id: Optional[str] = None
    bulletin_name: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    notes: Optional[str] = None
    items: List[PriceBulletinItem] = Field(default_factory=list)

class CommissionRuleResponse(BaseModel):
    id: str
    agency_id: str
    name: str
    rule_type: str
    value: float
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    approval_status: str = "pending"
    approval_comment: Optional[str] = None
    created_by: Optional[str] = None
    submitted_by: Optional[str] = None
    submitted_at: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None
    rejected_by: Optional[str] = None
    rejected_at: Optional[str] = None
    created_at: str

class CommissionApprovalAction(BaseModel):
    decision: str  # approved | rejected
    comment: Optional[str] = None

class CommissionClosureCreate(BaseModel):
    seller_id: str
    agency_id: str
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=2000, le=2100)

class CommissionClosureApprovalAction(BaseModel):
    decision: str  # approved | rejected
    comment: Optional[str] = None

class CommissionSimulatorInput(BaseModel):
    agency_id: str
    seller_id: Optional[str] = None
    target_commission: float = Field(..., ge=0)
    units: int = Field(..., ge=0)
    average_ticket: float = Field(..., ge=0)
    average_fi_revenue: float = Field(0, ge=0)

class SaleCreate(BaseModel):
    vehicle_id: str
    seller_id: str
    sale_price: float
    sale_date: Optional[str] = None
    fi_revenue: float = 0
    plant_incentive: float = 0

class SaleResponse(BaseModel):
    id: str
    vehicle_id: str
    vehicle_info: Optional[Dict] = None
    seller_id: str
    seller_name: Optional[str] = None
    agency_id: str
    sale_price: float
    commission_base_price: Optional[float] = None
    effective_revenue: Optional[float] = None
    brand_incentive_amount: Optional[float] = None
    dealer_incentive_amount: Optional[float] = None
    aging_incentive_sale_discount_amount: Optional[float] = None
    aging_incentive_seller_bonus_amount: Optional[float] = None
    aging_incentive_total_amount: Optional[float] = None
    sale_date: str
    fi_revenue: float
    plant_incentive: float = 0
    commission: float
    created_at: str

class AuditLogResponse(BaseModel):
    id: str
    created_at: str
    actor_id: Optional[str] = None
    actor_name: Optional[str] = None
    actor_email: Optional[str] = None
    actor_role: Optional[str] = None
    action: str
    entity_type: str
    entity_id: Optional[str] = None
    group_id: Optional[str] = None
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

class VehicleSuggestion(BaseModel):
    vehicle_id: str
    vehicle_info: Dict
    avg_days_to_sell: int
    current_aging: int
    financial_cost: float
    suggested_bonus: float
    reason: str

class VehicleAgingIncentiveApply(BaseModel):
    sale_discount_amount: float = Field(0, ge=0)
    seller_bonus_amount: float = Field(0, ge=0)
    notes: Optional[str] = None

class DashboardMonthlyCloseUpsert(BaseModel):
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=2020, le=2100)
    group_id: Optional[str] = None  # deprecated: calendario operativo ahora es global
    fiscal_close_day: Optional[int] = Field(default=None, ge=1, le=31)
    industry_close_day: Optional[int] = Field(default=None, ge=1, le=31)
    industry_close_month_offset: int = Field(default=0, ge=0, le=1)  # 0=mismo mes, 1=mes siguiente

# ============== AUTH HELPER ==============

async def get_current_user(request: Request) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[JWT_ALGORITHM])
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user = await db.users.find_one({"_id": ObjectId(payload["sub"])})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        user["id"] = str(user["_id"])
        del user["_id"]
        user.pop("password_hash", None)
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_optional_user(request: Request) -> Optional[dict]:
    try:
        return await get_current_user(request)
    except:
        return None

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

APP_LEVEL_ROLES = {UserRole.APP_ADMIN, UserRole.APP_USER}
BRAND_SCOPED_ROLES = {UserRole.BRAND_ADMIN, UserRole.BRAND_USER}
AGENCY_SCOPED_ROLES = {
    UserRole.AGENCY_ADMIN,
    UserRole.AGENCY_SALES_MANAGER,
    UserRole.AGENCY_GENERAL_MANAGER,
    UserRole.AGENCY_COMMERCIAL_MANAGER,
    UserRole.AGENCY_USER,
    UserRole.SELLER,
}
CORP_STRUCTURE_ROLES = {
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
}
CORP_FINANCE_ROLES = {
    UserRole.APP_ADMIN,
    UserRole.GROUP_FINANCE_MANAGER,
}
DEALER_GENERAL_EFFECTIVE_ROLES = {
    UserRole.AGENCY_GENERAL_MANAGER,
    UserRole.AGENCY_ADMIN,  # legacy write role
    UserRole.AGENCY_COMMERCIAL_MANAGER,  # legacy approver equivalent
}
DEALER_SALES_EFFECTIVE_ROLES = {
    UserRole.AGENCY_SALES_MANAGER,
}
DEALER_SELLER_ROLE = UserRole.SELLER
DEALER_LEGACY_READONLY_ROLE = UserRole.AGENCY_USER
DEALER_USER_MANAGER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES | DEALER_SALES_EFFECTIVE_ROLES

DEALER_GENERAL_ASSIGNABLE_ROLES = {
    UserRole.AGENCY_SALES_MANAGER,
    UserRole.SELLER,
    UserRole.AGENCY_USER,  # legacy read-only compatibility
}
DEALER_SALES_ASSIGNABLE_ROLES = {
    UserRole.SELLER,
}

COMMISSION_PENDING = "pending"
COMMISSION_APPROVED = "approved"
COMMISSION_REJECTED = "rejected"
COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT = 100.0

def _same_scope_id(left: Optional[str], right: Optional[str]) -> bool:
    if left is None or right is None:
        return False
    return str(left) == str(right)

def _empty_scope_query() -> Dict[str, Any]:
    # Mongo ObjectId field never equals this string, so query returns no rows.
    return {"_id": "__none__"}

def _is_app_level_role(role: Optional[str]) -> bool:
    return role in APP_LEVEL_ROLES

def _is_brand_scoped_role(role: Optional[str]) -> bool:
    return role in BRAND_SCOPED_ROLES

def _is_agency_scoped_role(role: Optional[str]) -> bool:
    return role in AGENCY_SCOPED_ROLES

def _is_corp_structure_role(role: Optional[str]) -> bool:
    return role in CORP_STRUCTURE_ROLES

def _is_corp_finance_role(role: Optional[str]) -> bool:
    return role in CORP_FINANCE_ROLES

def _is_dealer_general_effective_role(role: Optional[str]) -> bool:
    return role in DEALER_GENERAL_EFFECTIVE_ROLES

def _is_dealer_sales_effective_role(role: Optional[str]) -> bool:
    return role in DEALER_SALES_EFFECTIVE_ROLES

def _is_dealer_user_manager_role(role: Optional[str]) -> bool:
    return role in DEALER_USER_MANAGER_ROLES

def _get_dealer_assignable_roles(role: Optional[str]) -> set[str]:
    if _is_dealer_general_effective_role(role):
        return DEALER_GENERAL_ASSIGNABLE_ROLES
    if _is_dealer_sales_effective_role(role):
        return DEALER_SALES_ASSIGNABLE_ROLES
    return set()

def _build_scope_query(current_user: dict) -> Dict[str, Any]:
    role = current_user.get("role")
    if _is_app_level_role(role):
        return {}

    user_group_id = current_user.get("group_id")
    if not user_group_id:
        return _empty_scope_query()

    query: Dict[str, Any] = {"group_id": user_group_id}

    if _is_brand_scoped_role(role):
        user_brand_id = current_user.get("brand_id")
        if not user_brand_id:
            return _empty_scope_query()
        query["brand_id"] = user_brand_id

    if _is_agency_scoped_role(role):
        user_agency_id = current_user.get("agency_id")
        if not user_agency_id:
            return _empty_scope_query()
        query["agency_id"] = user_agency_id
        # Agency roles usually carry brand_id as well; if present, keep strict scope.
        if current_user.get("brand_id"):
            query["brand_id"] = current_user["brand_id"]

    return query

def _scope_query_has_access(query: Dict[str, Any]) -> bool:
    return query.get("_id") != "__none__"

def _validate_scope_filters(
    current_user: dict,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
) -> None:
    role = current_user.get("role")
    if _is_app_level_role(role):
        return

    user_group_id = current_user.get("group_id")
    user_brand_id = current_user.get("brand_id")
    user_agency_id = current_user.get("agency_id")

    if group_id and user_group_id and not _same_scope_id(group_id, user_group_id):
        raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")

    if _is_brand_scoped_role(role):
        if brand_id and user_brand_id and not _same_scope_id(brand_id, user_brand_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta marca")

    if _is_agency_scoped_role(role):
        if brand_id and user_brand_id and not _same_scope_id(brand_id, user_brand_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta marca")
        if agency_id and user_agency_id and not _same_scope_id(agency_id, user_agency_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta agencia")

def _ensure_doc_scope_access(
    current_user: dict,
    doc: Optional[Dict[str, Any]],
    *,
    group_field: str = "group_id",
    brand_field: str = "brand_id",
    agency_field: str = "agency_id",
    detail: str = "No tienes acceso a este recurso",
) -> None:
    if not doc:
        raise HTTPException(status_code=404, detail="Resource not found")

    role = current_user.get("role")
    if _is_app_level_role(role):
        return

    user_group_id = current_user.get("group_id")
    user_brand_id = current_user.get("brand_id")
    user_agency_id = current_user.get("agency_id")

    if user_group_id and not _same_scope_id(doc.get(group_field), user_group_id):
        raise HTTPException(status_code=403, detail=detail)

    if _is_brand_scoped_role(role):
        if user_brand_id and not _same_scope_id(doc.get(brand_field), user_brand_id):
            raise HTTPException(status_code=403, detail=detail)

    if _is_agency_scoped_role(role):
        if user_agency_id and not _same_scope_id(doc.get(agency_field), user_agency_id):
            raise HTTPException(status_code=403, detail=detail)

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

WRITE_AUDIT_ROLES = {
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.GROUP_FINANCE_MANAGER,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_ADMIN,
    UserRole.AGENCY_SALES_MANAGER,
    UserRole.AGENCY_GENERAL_MANAGER,
    UserRole.AGENCY_COMMERCIAL_MANAGER,
    UserRole.SELLER,
}

FINANCIAL_RATE_MANAGER_ROLES = CORP_FINANCE_ROLES
PRICE_BULLETIN_EDITOR_ROLES = {
    UserRole.APP_ADMIN,
    UserRole.GROUP_ADMIN,
    UserRole.GROUP_FINANCE_MANAGER,
    UserRole.BRAND_ADMIN,
    UserRole.AGENCY_GENERAL_MANAGER,
    UserRole.AGENCY_SALES_MANAGER,
    UserRole.AGENCY_ADMIN,
    UserRole.AGENCY_COMMERCIAL_MANAGER,
}

OBJECTIVE_PENDING = "pending"
OBJECTIVE_DRAFT = "draft"
OBJECTIVE_APPROVED = "approved"
OBJECTIVE_REJECTED = "rejected"

OBJECTIVE_EDITOR_ROLES = DEALER_SALES_EFFECTIVE_ROLES
OBJECTIVE_APPROVER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES

COMMISSION_PROPOSER_ROLES = DEALER_SALES_EFFECTIVE_ROLES
COMMISSION_APPROVER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES
COMMISSION_MATRIX_EDITOR_ROLES = (
    DEALER_SALES_EFFECTIVE_ROLES
    | DEALER_GENERAL_EFFECTIVE_ROLES
    | {
        UserRole.APP_ADMIN,
        UserRole.GROUP_ADMIN,
        UserRole.GROUP_FINANCE_MANAGER,
        UserRole.BRAND_ADMIN,
    }
)

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
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

def _parse_catalog_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 4:
        try:
            return int(digits[:4])
        except ValueError:
            return None
    try:
        return int(text)
    except ValueError:
        return None

def _parse_catalog_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed

MEX_STATE_SUFFIX_PATTERN = (
    r"(?:Aguascalientes|Ags\.?|Baja California(?: Sur)?|B\.?C\.?S?\.?|Campeche|Camp\.?|Chiapas|Chis\.?|"
    r"Chihuahua|Chih\.?|Coahuila|Coah\.?|Colima|Col\.?|Durango|Dgo\.?|Guanajuato|Gto\.?|Guerrero|Gro\.?|"
    r"Hidalgo|Hgo\.?|Jalisco|Jal\.?|México|Estado de México|Edo\.? Mex\.?|Edomex|Michoacán|Michoacan|Mich\.?|"
    r"Morelos|Mor\.?|Nayarit|Nay\.?|Nuevo León|Nuevo Leon|N\.?L\.?|Oaxaca|Oax\.?|Puebla|Pue\.?|Querétaro|Queretaro|Qro\.?|"
    r"Quintana Roo|Q\.? Roo|San Luis Potosí|San Luis Potosi|S\.?L\.?P\.?|Sinaloa|Sin\.?|Sonora|Son\.?|"
    r"Tabasco|Tab\.?|Tamaulipas|Tamps\.?|Tlaxcala|Tlax\.?|Veracruz|Ver\.?|Yucatán|Yucatan|Yuc\.?|Zacatecas|Zac\.?|"
    r"CDMX|Ciudad de México|Ciudad de Mexico)"
)

CITY_CONNECTORS = {"de", "del", "la", "las", "los", "y", "el"}
CITY_STOPWORDS = {
    "av", "av.", "avenida", "blvd", "blvd.", "boulevard", "calz", "calz.", "calzada",
    "carretera", "km", "no", "no.", "num", "numero", "número", "col", "col.", "colonia",
    "fracc", "fracc.", "fraccionamiento", "cp", "c.p.", "pte", "pte.", "oriente",
    "poniente", "sur", "norte", "sn", "s/n"
}
CITY_AREA_MARKERS = {"col", "col.", "colonia", "fracc", "fracc.", "fraccionamiento", "cp", "c.p.", "c", "p"}

def _extract_postal_code(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    text = " ".join(str(address).split())
    if not text:
        return None
    match = re.search(r"\b(?:C\.?\s*P\.?\s*:?\s*)?(\d{5})\b", text, flags=re.IGNORECASE)
    return match.group(1) if match else None

def _extract_city_from_fragment(fragment: Optional[str]) -> Optional[str]:
    if not fragment:
        return None

    cleaned = " ".join(str(fragment).split())
    if not cleaned:
        return None

    raw_tokens = re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ.'-]+|\d+", cleaned)
    if not raw_tokens:
        return None

    city_tokens: List[str] = []
    reversed_tokens = list(reversed(raw_tokens))
    for index, raw in enumerate(reversed_tokens):
        token = raw.strip(" .,#;:-")
        if not token:
            continue
        lower = token.lower()
        next_token = (
            reversed_tokens[index + 1].strip(" .,#;:-").lower()
            if index + 1 < len(reversed_tokens)
            else None
        )

        if token.isdigit():
            if city_tokens:
                break
            continue

        if len(token) == 1:
            if city_tokens:
                break
            continue

        if lower in CITY_CONNECTORS and city_tokens:
            city_tokens.append(token)
            continue

        if next_token in CITY_AREA_MARKERS and city_tokens:
            break

        if lower in CITY_STOPWORDS:
            if city_tokens:
                break
            continue

        city_tokens.append(token)
        if len(city_tokens) >= 5:
            break

    if not city_tokens:
        return None

    city = " ".join(reversed(city_tokens)).strip()
    city = re.sub(r"\s{2,}", " ", city)
    return city if len(city) >= 2 else None

def _infer_city_from_address(address: Optional[str]) -> Optional[str]:
    if not address:
        return None

    text = " ".join(str(address).split())
    if not text or re.search(r"\bPROXIMAMENTE\b", text, flags=re.IGNORECASE):
        return None

    if re.search(r"\bCDMX\b", text, flags=re.IGNORECASE):
        return "CDMX"

    without_cp = re.sub(r"\bC\.?\s*P\.?\s*:?\s*\d{5}\b", " ", text, flags=re.IGNORECASE)
    without_cp = re.sub(r"\bCP\s*:?\s*\d{5}\b", " ", without_cp, flags=re.IGNORECASE)
    without_cp = re.sub(r"\b\d{5}\b", " ", without_cp)
    without_cp = " ".join(without_cp.split())
    if not without_cp:
        return None

    by_comma_and_state = re.search(
        rf"([^,]+?),\s*{MEX_STATE_SUFFIX_PATTERN}\.?\s*$",
        without_cp,
        flags=re.IGNORECASE
    )
    if by_comma_and_state:
        city = _extract_city_from_fragment(by_comma_and_state.group(1))
        if city:
            return city

    by_space_and_state = re.search(
        rf"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9\s\.,#'/-]+?)\s+{MEX_STATE_SUFFIX_PATTERN}\.?\s*$",
        without_cp,
        flags=re.IGNORECASE
    )
    if by_space_and_state:
        city = _extract_city_from_fragment(by_space_and_state.group(1))
        if city:
            return city

    segments = [segment.strip(" .") for segment in without_cp.split(",") if segment.strip(" .")]
    if len(segments) >= 2:
        city = _extract_city_from_fragment(segments[-2])
        if city:
            return city
    if segments:
        city = _extract_city_from_fragment(segments[-1])
        if city:
            return city

    return _extract_city_from_fragment(without_cp)

def _resolve_agency_location(city: Optional[str], address: Optional[str]) -> Dict[str, Optional[str]]:
    resolved_city = _normalize_catalog_text(city) or _infer_city_from_address(address)
    resolved_postal_code = _extract_postal_code(address)
    return {
        "city": resolved_city,
        "postal_code": resolved_postal_code,
    }

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
    line1_tokens = [token for token in [street, exterior_number] if token]
    line1 = " ".join(line1_tokens).strip()
    if interior_number:
        line1 = f"{line1}, Int {interior_number}" if line1 else f"Int {interior_number}"

    line2 = neighborhood or None

    city_line_tokens = [token for token in [city, state] if token]
    city_line = ", ".join(city_line_tokens)
    if postal_code:
        city_line = f"{city_line} {postal_code}".strip() if city_line else postal_code

    line4 = country or None
    parts = [part.strip() for part in [line1, line2, city_line, line4] if part and part.strip()]
    return ", ".join(parts) if parts else None

def _merge_optional_text(incoming: Optional[str], previous: Optional[str]) -> Optional[str]:
    if incoming is None:
        return _normalize_catalog_text(previous)
    return _normalize_catalog_text(incoming)

def _merge_optional_float(incoming: Optional[float], previous: Any) -> Optional[float]:
    if incoming is None:
        try:
            return float(previous) if previous is not None else None
        except (TypeError, ValueError):
            return None
    return incoming

async def backfill_agency_locations() -> Dict[str, int]:
    agencies = await db.agencies.find({}, {"city": 1, "address": 1, "postal_code": 1}).to_list(20000)
    updated = 0
    filled_city = 0
    filled_postal_code = 0

    for agency in agencies:
        current_city = agency.get("city")
        current_postal_code = agency.get("postal_code")
        location = _resolve_agency_location(current_city, agency.get("address"))

        update_data: Dict[str, Any] = {}
        if not current_city and location.get("city"):
            update_data["city"] = location["city"]
            filled_city += 1

        if not current_postal_code and location.get("postal_code"):
            update_data["postal_code"] = location["postal_code"]
            filled_postal_code += 1

        if update_data:
            await db.agencies.update_one({"_id": agency["_id"]}, {"$set": update_data})
            updated += 1

    return {
        "checked": len(agencies),
        "updated": updated,
        "filled_city": filled_city,
        "filled_postal_code": filled_postal_code,
    }

def _build_catalog_tree_from_source(all_years: bool = False) -> Dict[str, Any]:
    source_path = get_catalog_source_path()
    model_year = get_catalog_model_year()
    target_model_year: Optional[int] = None if all_years else model_year
    source_file = Path(source_path)
    if not source_file.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Catalog source file not found: {source_path}"
        )

    source_mtime = source_file.stat().st_mtime
    if (
        catalog_cache.get("payload") is not None and
        catalog_cache.get("source_path") == source_path and
        catalog_cache.get("model_year") == target_model_year and
        catalog_cache.get("all_years") == bool(all_years) and
        catalog_cache.get("mtime") == source_mtime
    ):
        return catalog_cache["payload"]

    try:
        with source_file.open("r", encoding="utf-8") as f:
            source_data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading catalog source file: {str(e)}")

    if isinstance(source_data, dict):
        vehicles = source_data.get("vehicles", [])
    elif isinstance(source_data, list):
        vehicles = source_data
    else:
        vehicles = []

    if not isinstance(vehicles, list):
        raise HTTPException(status_code=500, detail="Invalid catalog format: vehicles list is required")

    make_map: Dict[str, Dict[str, Any]] = {}
    available_years: set[int] = set()
    matched_rows = 0

    for item in vehicles:
        if not isinstance(item, dict):
            continue

        version_data = item.get("version")
        version_data = version_data if isinstance(version_data, dict) else {}
        row_year = _parse_catalog_year(version_data.get("year"))
        if row_year is not None:
            available_years.add(row_year)
        if target_model_year is not None and row_year != target_model_year:
            continue

        make_data = item.get("make")
        model_data = item.get("model")
        make_name = _normalize_catalog_text(make_data.get("name") if isinstance(make_data, dict) else make_data)
        model_name = _normalize_catalog_text(model_data.get("name") if isinstance(model_data, dict) else model_data)
        version_name = _normalize_catalog_text(version_data.get("name"))
        pricing_data = item.get("pricing")
        pricing_data = pricing_data if isinstance(pricing_data, dict) else {}
        msrp_value = _parse_catalog_price(pricing_data.get("msrp"))

        if not make_name or not model_name or not version_name:
            continue

        matched_rows += 1
        make_key = make_name.casefold()
        model_key = model_name.casefold()
        version_key = version_name.casefold()

        make_entry = make_map.setdefault(make_key, {"name": make_name, "models": {}})
        model_entry = make_entry["models"].setdefault(
            model_key,
            {"name": model_name, "versions": {}, "min_msrp": None},
        )

        existing_version = model_entry["versions"].get(version_key)
        if not existing_version:
            model_entry["versions"][version_key] = {"name": version_name, "msrp": msrp_value}
        elif msrp_value is not None:
            current_msrp = _parse_catalog_price(existing_version.get("msrp"))
            if current_msrp is None or msrp_value < current_msrp:
                existing_version["msrp"] = msrp_value

        if msrp_value is not None:
            current_min_msrp = _parse_catalog_price(model_entry.get("min_msrp"))
            if current_min_msrp is None or msrp_value < current_min_msrp:
                model_entry["min_msrp"] = msrp_value

    makes: List[Dict[str, Any]] = []
    total_models = 0
    total_versions = 0

    for make_entry in sorted(make_map.values(), key=lambda x: x["name"].casefold()):
        models: List[Dict[str, Any]] = []
        for model_entry in sorted(make_entry["models"].values(), key=lambda x: x["name"].casefold()):
            versions = [
                {
                    "name": version_entry["name"],
                    "msrp": _parse_catalog_price(version_entry.get("msrp")),
                }
                for version_entry in sorted(model_entry["versions"].values(), key=lambda x: x["name"].casefold())
            ]
            total_models += 1
            total_versions += len(versions)
            models.append({
                "name": model_entry["name"],
                "min_msrp": _parse_catalog_price(model_entry.get("min_msrp")),
                "versions": versions
            })
        makes.append({
            "name": make_entry["name"],
            "models": models
        })

    payload = {
        "model_year": target_model_year,
        "all_years": bool(all_years),
        "available_years": sorted(list(available_years)),
        "source_path": source_path,
        "source_last_modified": datetime.fromtimestamp(source_mtime, tz=timezone.utc).isoformat(),
        "rows_matched": matched_rows,
        "counts": {
            "makes": len(makes),
            "models": total_models,
            "versions": total_versions
        },
        "makes": makes
    }

    catalog_cache["source_path"] = source_path
    catalog_cache["model_year"] = target_model_year
    catalog_cache["all_years"] = bool(all_years)
    catalog_cache["mtime"] = source_mtime
    catalog_cache["payload"] = payload
    return payload

def _find_catalog_make(catalog: Dict[str, Any], make_name: str) -> Optional[Dict[str, Any]]:
    key = _normalize_catalog_text(make_name)
    if not key:
        return None
    lookup = key.casefold()
    for make_entry in catalog.get("makes", []):
        if str(make_entry.get("name", "")).casefold() == lookup:
            return make_entry
    return None

def _find_catalog_model(make_entry: Dict[str, Any], model_name: str) -> Optional[Dict[str, Any]]:
    key = _normalize_catalog_text(model_name)
    if not key:
        return None
    lookup = key.casefold()
    for model_entry in make_entry.get("models", []):
        if str(model_entry.get("name", "")).casefold() == lookup:
            return model_entry
    return None

def _ensure_allowed_model_year(year: int) -> None:
    allowed_year = get_catalog_model_year()
    if year != allowed_year:
        raise HTTPException(status_code=400, detail=f"Solo se permite año modelo {allowed_year}")

# ============== AUTH ROUTES ==============

async def register(user_data: UserCreate, request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    if actor_role not in {UserRole.APP_ADMIN, UserRole.GROUP_ADMIN} and not _is_dealer_user_manager_role(actor_role):
        raise HTTPException(status_code=403, detail="Not authorized")

    if actor_role == UserRole.GROUP_ADMIN:
        if user_data.role in [UserRole.APP_ADMIN, UserRole.APP_USER]:
            raise HTTPException(status_code=403, detail="Group admin cannot create app-level users")

        if not current_user.get("group_id"):
            raise HTTPException(status_code=403, detail="Group admin has no assigned group")

        # Group admins can only create users inside their own group.
        if user_data.group_id and user_data.group_id != current_user["group_id"]:
            raise HTTPException(status_code=403, detail="Cannot create users outside your group")
        user_data.group_id = current_user["group_id"]
    elif _is_dealer_user_manager_role(actor_role):
        assignable_roles = _get_dealer_assignable_roles(actor_role)
        if user_data.role not in assignable_roles:
            raise HTTPException(status_code=403, detail="Role is not assignable for this dealer manager")
        if not current_user.get("agency_id"):
            raise HTTPException(status_code=403, detail="Dealer manager has no assigned agency")

        if user_data.group_id and user_data.group_id != current_user.get("group_id"):
            raise HTTPException(status_code=403, detail="Cannot create users outside your group")
        if user_data.brand_id and user_data.brand_id != current_user.get("brand_id"):
            raise HTTPException(status_code=403, detail="Cannot create users outside your brand")
        if user_data.agency_id and user_data.agency_id != current_user.get("agency_id"):
            raise HTTPException(status_code=403, detail="Cannot create users outside your agency")

        # Dealer managers can only create users inside their own agency scope.
        user_data.group_id = current_user.get("group_id")
        user_data.brand_id = current_user.get("brand_id")
        user_data.agency_id = current_user.get("agency_id")

    email = str(user_data.email).strip().lower()
    existing = await find_user_by_email(db, email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    if user_data.brand_id:
        brand = await find_brand_by_id(db, user_data.brand_id)
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")
        if user_data.group_id and brand.get("group_id") != user_data.group_id:
            raise HTTPException(status_code=400, detail="Brand does not belong to selected group")
        if not user_data.group_id:
            user_data.group_id = brand.get("group_id")

    if user_data.agency_id:
        agency = await find_agency_by_id(db, user_data.agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        if user_data.group_id and agency.get("group_id") != user_data.group_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected group")
        if user_data.brand_id and agency.get("brand_id") != user_data.brand_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected brand")
        if not user_data.group_id:
            user_data.group_id = agency.get("group_id")
        if not user_data.brand_id:
            user_data.brand_id = agency.get("brand_id")

    if user_data.role in [UserRole.BRAND_ADMIN, UserRole.BRAND_USER] and not user_data.brand_id:
        raise HTTPException(status_code=400, detail="Brand role requires brand_id")
    if user_data.role in [
        UserRole.AGENCY_ADMIN,
        UserRole.AGENCY_SALES_MANAGER,
        UserRole.AGENCY_GENERAL_MANAGER,
        UserRole.AGENCY_COMMERCIAL_MANAGER,
        UserRole.AGENCY_USER,
        UserRole.SELLER,
    ] and not user_data.agency_id:
        raise HTTPException(status_code=400, detail="Agency role requires agency_id")

    normalized_position = str(user_data.position or "").strip() or None

    user_doc = {
        "email": email,
        "password_hash": hash_password(user_data.password),
        "name": user_data.name,
        "position": normalized_position,
        "role": user_data.role,
        "group_id": user_data.group_id,
        "brand_id": user_data.brand_id,
        "agency_id": user_data.agency_id,
        "created_at": datetime.now(timezone.utc)
    }
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
    email = str(user_data.email).strip().lower()
    user = await find_user_by_email(db, email)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    password_raw = user_data.password or ""
    password_trimmed = password_raw.strip()
    if not (
        verify_password(password_raw, user["password_hash"]) or
        (password_trimmed != password_raw and verify_password(password_trimmed, user["password_hash"]))
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    user_id = str(user["_id"])
    access_token = create_access_token(user_id, email, user["role"])
    refresh_token = create_refresh_token(user_id)
    
    response.set_cookie(key="access_token", value=access_token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, secure=False, samesite="lax", max_age=604800, path="/")
    
    return {
        "id": user_id,
        "email": user["email"],
        "name": user["name"],
        "position": user.get("position"),
        "role": user["role"],
        "group_id": user.get("group_id"),
        "brand_id": user.get("brand_id"),
        "agency_id": user.get("agency_id"),
        "created_at": user["created_at"].isoformat() if isinstance(user["created_at"], datetime) else user["created_at"],
        "token": access_token
    }

async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    return {"message": "Logged out successfully"}

async def reset_password(payload: PasswordResetRequest):
    email = str(payload.email).strip().lower()

    if len(payload.new_password or "") < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    user = await find_user_by_email(db, email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await update_user_password_hash(db, str(user["_id"]), hash_password(payload.new_password))

    return {"message": "Password updated successfully"}

async def get_me(request: Request):
    user = await get_current_user(request)
    return user

async def google_auth(request: Request, response: Response):
    """Handle Google OAuth callback"""
    data = await request.json()
    credential = data.get("credential")
    
    if not credential:
        raise HTTPException(status_code=400, detail="No credential provided")
    
    try:
        # Decode the JWT token from Google (without verification for simplicity)
        # In production, verify with Google's public keys
        import base64
        parts = credential.split(".")
        payload = parts[1]
        # Add padding if necessary
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        google_user = json.loads(decoded)
        
        email = str(google_user.get("email", "")).strip().lower()
        name = google_user.get("name", "")
        
        # Check if user exists
        user = await find_user_by_email(db, email)
        
        if not user:
            # Create new user
            user_doc = {
                "email": email,
                "password_hash": "",  # No password for Google users
                "name": name,
                "position": None,
                "role": UserRole.APP_USER,
                "group_id": None,
                "brand_id": None,
                "agency_id": None,
                "google_id": google_user.get("sub"),
                "created_at": datetime.now(timezone.utc)
            }
            user_id = await create_user(db, user_doc)
            user = user_doc
            user["_id"] = ObjectId(user_id)
        else:
            user_id = str(user["_id"])
        
        access_token = create_access_token(user_id, email, user.get("role", UserRole.APP_USER))
        refresh_token = create_refresh_token(user_id)
        
        response.set_cookie(key="access_token", value=access_token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
        response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, secure=False, samesite="lax", max_age=604800, path="/")
        
        return {
            "id": user_id,
            "email": email,
            "name": user.get("name", name),
            "position": user.get("position"),
            "role": user.get("role", UserRole.APP_USER),
            "group_id": user.get("group_id"),
            "brand_id": user.get("brand_id"),
            "agency_id": user.get("agency_id"),
            "created_at": user["created_at"].isoformat() if isinstance(user.get("created_at"), datetime) else str(user.get("created_at", "")),
            "token": access_token
        }
    except Exception as e:
        logger.error(f"Google auth error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid Google credential: {str(e)}")

# ============== USERS ROUTES ==============

async def get_users(request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    if actor_role not in {UserRole.APP_ADMIN, UserRole.GROUP_ADMIN} and not _is_dealer_user_manager_role(actor_role):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    query = {}
    if actor_role == UserRole.GROUP_ADMIN and current_user.get("group_id"):
        query["group_id"] = current_user["group_id"]
    elif _is_dealer_user_manager_role(actor_role):
        if not current_user.get("agency_id"):
            return []
        query["agency_id"] = current_user["agency_id"]
    
    users = await list_users(db, query, include_password_hash=False, limit=1000)
    return [serialize_doc(u) for u in users]

async def update_user(user_id: str, request: Request):
    current_user = await get_current_user(request)
    actor_role = current_user.get("role")
    if actor_role not in {UserRole.APP_ADMIN, UserRole.GROUP_ADMIN} and not _is_dealer_user_manager_role(actor_role):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    data = await request.json()
    raw_new_password = data.pop("new_password", None)
    if raw_new_password is not None:
        new_password = str(raw_new_password)
        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    else:
        new_password = None

    update_data = {
        k: v for k, v in data.items()
        if k not in ["id", "_id", "password_hash", "password", "email", "confirm_new_password"]
    }

    if "position" in update_data:
        update_data["position"] = str(update_data.get("position") or "").strip() or None

    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id")
    existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    if str(existing_user.get("_id")) == str(current_user.get("id")) and actor_role != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Self-edit is only allowed for app admin")
    if existing_user.get("role") == UserRole.APP_ADMIN and actor_role != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Cannot edit app admin users")

    if actor_role == UserRole.GROUP_ADMIN:
        current_group_id = current_user.get("group_id")
        if not current_group_id:
            raise HTTPException(status_code=403, detail="Group admin has no assigned group")
        if not _same_scope_id(existing_user.get("group_id"), current_group_id):
            raise HTTPException(status_code=403, detail="Cannot edit users outside your group")
        if update_data.get("group_id") and not _same_scope_id(update_data["group_id"], current_group_id):
            raise HTTPException(status_code=403, detail="Cannot move users outside your group")
        requested_role = update_data.get("role")
        if requested_role == UserRole.APP_ADMIN:
            raise HTTPException(status_code=403, detail="Group admin cannot assign app admin role")
        if requested_role == UserRole.APP_USER and existing_user.get("role") != UserRole.APP_USER:
            raise HTTPException(status_code=403, detail="Group admin cannot assign app user role")

    if _is_dealer_user_manager_role(actor_role):
        current_agency_id = current_user.get("agency_id")
        if not current_agency_id:
            raise HTTPException(status_code=403, detail="Dealer manager has no assigned agency")
        if not _same_scope_id(existing_user.get("agency_id"), current_agency_id):
            raise HTTPException(status_code=403, detail="Cannot edit users outside your agency")

        assignable_roles = _get_dealer_assignable_roles(actor_role)
        existing_role = existing_user.get("role")
        if existing_role not in assignable_roles:
            raise HTTPException(status_code=403, detail="Cannot edit this user role from your dealer scope")

        if update_data.get("role") and update_data["role"] not in assignable_roles:
            raise HTTPException(status_code=403, detail="Role is not assignable for this dealer manager")
        if update_data.get("agency_id") and not _same_scope_id(update_data["agency_id"], current_agency_id):
            raise HTTPException(status_code=403, detail="Cannot move users outside your agency")

        # Keep updated user locked to manager agency scope.
        update_data["group_id"] = current_user.get("group_id")
        update_data["brand_id"] = current_user.get("brand_id")
        update_data["agency_id"] = current_agency_id
    
    if new_password:
        update_data["password_hash"] = hash_password(new_password)

    if update_data:
        await update_user_by_id(db, user_id, update_data)
    user = await find_user_by_id(db, user_id, include_password_hash=False)

    audit_changes = {k: v for k, v in update_data.items() if k != "password_hash"}
    if "password_hash" in update_data:
        audit_changes["password_updated"] = True

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
    if actor_role not in {UserRole.APP_ADMIN, UserRole.GROUP_ADMIN} and not _is_dealer_user_manager_role(actor_role):
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id")

    existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")
    if existing_user.get("role") == UserRole.APP_ADMIN and actor_role != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Cannot delete app admin users")

    if str(existing_user["_id"]) == current_user.get("id"):
        raise HTTPException(status_code=400, detail="Cannot delete your own user")

    if actor_role == UserRole.GROUP_ADMIN:
        current_group_id = current_user.get("group_id")
        if not current_group_id:
            raise HTTPException(status_code=403, detail="Group admin has no assigned group")
        if not _same_scope_id(existing_user.get("group_id"), current_group_id):
            raise HTTPException(status_code=403, detail="Cannot delete users outside your group")

    if _is_dealer_user_manager_role(actor_role):
        current_agency_id = current_user.get("agency_id")
        if not current_agency_id:
            raise HTTPException(status_code=403, detail="Dealer manager has no assigned agency")
        if not _same_scope_id(existing_user.get("agency_id"), current_agency_id):
            raise HTTPException(status_code=403, detail="Cannot delete users outside your agency")
        assignable_roles = _get_dealer_assignable_roles(actor_role)
        if existing_user.get("role") not in assignable_roles:
            raise HTTPException(status_code=403, detail="Cannot delete this user role from your dealer scope")

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
    if actor_role not in {UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.GROUP_FINANCE_MANAGER} and not _is_dealer_user_manager_role(actor_role):
        raise HTTPException(status_code=403, detail="Not authorized")

    safe_limit = max(1, min(limit, 500))
    query: Dict[str, Any] = {}

    if actor_role in [UserRole.GROUP_ADMIN, UserRole.GROUP_FINANCE_MANAGER]:
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        query["group_id"] = user_group_id
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    elif _is_dealer_user_manager_role(actor_role):
        user_agency_id = current_user.get("agency_id")
        if not user_agency_id:
            return []
        query["agency_id"] = user_agency_id
        if agency_id and agency_id != user_agency_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a esta agencia")
        user_group_id = current_user.get("group_id")
        if group_id and user_group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    elif group_id:
        query["group_id"] = group_id

    if agency_id and not _is_dealer_user_manager_role(actor_role):
        query["agency_id"] = agency_id
    if actor_id:
        query["actor_id"] = actor_id

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

async def get_catalog_makes(request: Request):
    await get_current_user(request)
    catalog = _build_catalog_tree_from_source()
    return {
        "model_year": catalog["model_year"],
        "source_last_modified": catalog["source_last_modified"],
        "items": [
            {
                "name": make_entry["name"],
                "models_count": len(make_entry.get("models", [])),
                "logo_url": _resolve_logo_url_for_brand(make_entry["name"], request),
            }
            for make_entry in catalog.get("makes", [])
        ]
    }

async def get_catalog_models(
    request: Request,
    make: str = Query(..., min_length=1),
    all_years: bool = Query(False),
):
    await get_current_user(request)
    catalog = _build_catalog_tree_from_source(all_years=all_years)
    make_entry = _find_catalog_make(catalog, make)
    if not make_entry:
        if catalog.get("all_years"):
            raise HTTPException(status_code=404, detail="Make not found in catalog")
        raise HTTPException(status_code=404, detail=f"Make not found in catalog for model year {catalog['model_year']}")

    return {
        "model_year": catalog["model_year"],
        "all_years": bool(catalog.get("all_years")),
        "available_years": catalog.get("available_years", []),
        "make": make_entry["name"],
        "items": [
            {
                "name": model_entry["name"],
                "versions_count": len(model_entry.get("versions", [])),
                "min_msrp": _parse_catalog_price(model_entry.get("min_msrp")),
            }
            for model_entry in make_entry.get("models", [])
        ]
    }

async def get_catalog_versions(
    request: Request,
    make: str = Query(..., min_length=1),
    model: str = Query(..., min_length=1),
    all_years: bool = Query(False),
):
    await get_current_user(request)
    catalog = _build_catalog_tree_from_source(all_years=all_years)
    make_entry = _find_catalog_make(catalog, make)
    if not make_entry:
        if catalog.get("all_years"):
            raise HTTPException(status_code=404, detail="Make not found in catalog")
        raise HTTPException(status_code=404, detail=f"Make not found in catalog for model year {catalog['model_year']}")

    model_entry = _find_catalog_model(make_entry, model)
    if not model_entry:
        raise HTTPException(status_code=404, detail=f"Model not found for make {make_entry['name']}")

    return {
        "model_year": catalog["model_year"],
        "all_years": bool(catalog.get("all_years")),
        "available_years": catalog.get("available_years", []),
        "make": make_entry["name"],
        "model": model_entry["name"],
        "items": model_entry.get("versions", [])
    }

# ============== PRICE BULLETINS ROUTES ==============

def _normalize_iso_date_string(value: Optional[str], *, field_name: str, required: bool = False) -> Optional[str]:
    return _normalize_iso_date_string_service(value, field_name=field_name, required=required)

async def _resolve_price_bulletin_scope(
    current_user: dict,
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
) -> Dict[str, Optional[str]]:
    return await _resolve_price_bulletin_scope_service(
        db,
        current_user=current_user,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        validate_scope_filters=_validate_scope_filters,
    )

def _is_price_bulletin_active(doc: Dict[str, Any], current_date_ymd: str) -> bool:
    return _is_price_bulletin_active_service(doc, current_date_ymd)

async def _resolve_price_bulletin_for_model(
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    model: Optional[str],
    version: Optional[str] = None,
    reference_date_ymd: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    return await _resolve_price_bulletin_for_model_service(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        model=model,
        version=version,
        reference_date_ymd=reference_date_ymd,
    )

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
    pricing = await _resolve_effective_sale_pricing_for_model(
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        model=model,
        version=version,
        reference_date_ymd=reference_date_ymd,
        fallback_msrp=fallback_msrp,
    )
    return _to_non_negative_float(pricing.get("transaction_price"), 0.0)

def _price_item_applies_to_sale(
    *,
    sale_model: Optional[str],
    sale_version: Optional[str],
    affected_exact_keys: set[str],
    affected_model_keys: set[str],
) -> bool:
    return _price_item_applies_to_sale_service(
        sale_model=sale_model,
        sale_version=sale_version,
        affected_exact_keys=affected_exact_keys,
        affected_model_keys=affected_model_keys,
    )

async def _reprice_sales_for_price_bulletin(
    *,
    scope: Dict[str, Optional[str]],
    effective_from: Optional[str],
    effective_to: Optional[str],
    items: List[PriceBulletinItem],
) -> Dict[str, int]:
    return await _reprice_sales_for_price_bulletin_service(
        db,
        scope=scope,
        effective_from=effective_from,
        effective_to=effective_to,
        items=items,
        price_item_applies_to_sale=_price_item_applies_to_sale,
        resolve_effective_sale_pricing_for_model=_resolve_effective_sale_pricing_for_model,
        apply_manual_sale_price_override=_apply_manual_sale_price_override,
        calculate_commission=calculate_commission,
        to_non_negative_float=_to_non_negative_float,
        coerce_utc_datetime=_coerce_utc_datetime,
    )

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
        is_price_bulletin_active=_is_price_bulletin_active,
    )

async def upsert_price_bulletins_bulk(payload: PriceBulletinBulkUpsert, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in PRICE_BULLETIN_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not payload.items:
        raise HTTPException(status_code=400, detail="At least one item is required")

    scope = await _resolve_price_bulletin_scope(
        current_user=current_user,
        group_id=payload.group_id,
        brand_id=payload.brand_id,
        agency_id=payload.agency_id,
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

    repricing_summary = await _reprice_sales_for_price_bulletin(
        scope=scope,
        effective_from=effective_from,
        effective_to=effective_to,
        items=valid_items,
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
    if value is None:
        return None
    parsed: Optional[datetime] = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def calculate_vehicle_financial_cost_in_period(
    vehicle: dict,
    period_start: datetime,
    period_end: datetime,
) -> float:
    entry_date = _coerce_utc_datetime(vehicle.get("entry_date"))
    if not entry_date:
        return 0.0

    start_at = _coerce_utc_datetime(period_start) or period_start
    end_at = _coerce_utc_datetime(period_end) or period_end
    if start_at.tzinfo is None:
        start_at = start_at.replace(tzinfo=timezone.utc)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=timezone.utc)
    if end_at <= start_at:
        return 0.0

    effective_rate = await _resolve_effective_rate_components_for_vehicle(vehicle)
    total_rate_monthly = effective_rate["total_rate_monthly"]
    if total_rate_monthly is None:
        return 0.0

    grace_days = int(effective_rate.get("grace_days", 0) or 0)
    grace_end = entry_date + timedelta(days=grace_days)
    vehicle_end = _coerce_utc_datetime(vehicle.get("exit_date")) or datetime.now(timezone.utc)

    charge_start = max(start_at, grace_end)
    charge_end = min(end_at, vehicle_end)
    if charge_end <= charge_start:
        return 0.0

    charge_days = (charge_end - charge_start).days
    if charge_days <= 0:
        return 0.0

    daily_rate = total_rate_monthly / DAYS_PER_MONTH_FOR_RATE / 100
    financial_cost = float(vehicle.get("purchase_price", 0) or 0) * daily_rate * charge_days
    return round(financial_cost, 2)


async def calculate_vehicle_financial_cost(vehicle: dict) -> float:
    """Calculate financial cost using monthly rate (TIIE mensual + spread mensual)."""
    entry_date = _coerce_utc_datetime(vehicle.get("entry_date"))
    if not entry_date:
        return 0.0
    end_date = _coerce_utc_datetime(vehicle.get("exit_date")) or datetime.now(timezone.utc)
    return await calculate_vehicle_financial_cost_in_period(vehicle, entry_date, end_date)

async def enrich_vehicle(vehicle: dict) -> dict:
    """Enrich vehicle with agency, brand, group info and calculations"""
    result = serialize_doc(vehicle)
    
    # Get agency info
    if vehicle.get("agency_id"):
        agency = await db.agencies.find_one({"_id": ObjectId(vehicle["agency_id"])})
        if agency:
            result["agency_name"] = agency["name"]
            result["brand_id"] = agency.get("brand_id")
            result["group_id"] = agency.get("group_id")
            
            # Get brand name
            if agency.get("brand_id"):
                brand = await db.brands.find_one({"_id": ObjectId(agency["brand_id"])})
                if brand:
                    result["brand_name"] = brand["name"]
    
    # Calculate aging
    entry_date = vehicle.get("entry_date")
    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    elif isinstance(entry_date, datetime) and entry_date.tzinfo is None:
        entry_date = entry_date.replace(tzinfo=timezone.utc)
    
    if vehicle.get("exit_date"):
        end_date = vehicle["exit_date"]
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        elif isinstance(end_date, datetime) and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)
    
    result["aging_days"] = (end_date - entry_date).days
    result["financial_cost"] = await calculate_vehicle_financial_cost(vehicle)

    # Enrich sold vehicles with sale/commission info.
    vehicle_id = result.get("id")
    if vehicle_id:
        sale_doc = await db.sales.find_one({"vehicle_id": vehicle_id}, sort=[("sale_date", -1)])
        if sale_doc:
            serialized_sale = serialize_doc(sale_doc)
            result["sale_commission"] = round(float(serialized_sale.get("commission", 0) or 0), 2)
            result["sale_price"] = round(float(serialized_sale.get("sale_price", 0) or 0), 2)
            result["effective_revenue"] = round(_sale_effective_revenue(serialized_sale), 2)
            result["sale_date"] = serialized_sale.get("sale_date")
            seller_id = serialized_sale.get("seller_id")
            if seller_id and ObjectId.is_valid(seller_id):
                seller = await db.users.find_one({"_id": ObjectId(seller_id)})
                if seller:
                    result["sold_by_name"] = seller.get("name")
    
    return result

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
        is_price_bulletin_active=_is_price_bulletin_active,
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
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float(default)
    if numeric != numeric:  # NaN
        return float(default)
    return max(0.0, numeric)

def _sale_effective_revenue(sale: Dict[str, Any]) -> float:
    explicit = sale.get("effective_revenue")
    if explicit is not None:
        return _to_non_negative_float(explicit, 0.0)
    sale_price = _to_non_negative_float(sale.get("sale_price"), 0.0)
    brand_incentive = _to_non_negative_float(sale.get("brand_incentive_amount"), 0.0)
    return round(sale_price + brand_incentive, 2)

def _sale_commission_base_price(sale: Dict[str, Any]) -> float:
    explicit = sale.get("commission_base_price")
    if explicit is not None:
        return _to_non_negative_float(explicit, 0.0)
    sale_price = _to_non_negative_float(sale.get("sale_price"), 0.0)
    brand_incentive = _to_non_negative_float(sale.get("brand_incentive_amount"), 0.0)
    return round(sale_price + brand_incentive, 2)

def _normalize_commission_matrix_volume_tiers(tiers: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in tiers or []:
        payload = row or {}
        try:
            min_units = int(payload.get("min_units"))
        except (TypeError, ValueError):
            continue
        min_units = max(1, min_units)

        max_units_raw = payload.get("max_units")
        max_units: Optional[int] = None
        if max_units_raw not in (None, ""):
            try:
                parsed_max = int(max_units_raw)
                if parsed_max > 0:
                    max_units = max(min_units, parsed_max)
            except (TypeError, ValueError):
                max_units = None

        bonus_per_unit = _to_non_negative_float(payload.get("bonus_per_unit"), 0.0)
        if bonus_per_unit <= 0:
            continue

        dedupe_key = f"{min_units}:{max_units if max_units is not None else 'inf'}:{round(bonus_per_unit, 6)}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append({
            "min_units": min_units,
            "max_units": max_units,
            "bonus_per_unit": bonus_per_unit,
        })

    normalized.sort(key=lambda row: (int(row.get("min_units") or 0), int(row.get("max_units") or 10**9)))
    return normalized

def _normalize_commission_matrix_general(general: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = general or {}
    return {
        "global_percentage": _to_non_negative_float(payload.get("global_percentage"), 0.0),
        "global_per_unit_bonus": _to_non_negative_float(payload.get("global_per_unit_bonus"), 0.0),
        "global_aged_61_90_bonus": _to_non_negative_float(payload.get("global_aged_61_90_bonus"), 0.0),
        "global_aged_90_plus_bonus": _to_non_negative_float(payload.get("global_aged_90_plus_bonus"), 0.0),
        "volume_tiers": _normalize_commission_matrix_volume_tiers(payload.get("volume_tiers")),
    }

def _normalize_commission_matrix_models(models: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in models or []:
        model_name = str((row or {}).get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        if model_key in seen:
            continue
        seen.add(model_key)
        raw_model_percentage = (row or {}).get("model_percentage")
        model_percentage = None
        if raw_model_percentage is not None and str(raw_model_percentage).strip() != "":
            model_percentage = _to_non_negative_float(raw_model_percentage, 0.0)
        normalized.append({
            "model": model_name,
            "model_percentage": model_percentage,
            "model_bonus": _to_non_negative_float((row or {}).get("model_bonus"), 0.0),
            "aged_61_90_bonus": _to_non_negative_float((row or {}).get("aged_61_90_bonus"), 0.0),
            "aged_90_plus_bonus": _to_non_negative_float((row or {}).get("aged_90_plus_bonus"), 0.0),
            "plant_incentive_share_pct": min(100.0, _to_non_negative_float((row or {}).get("plant_incentive_share_pct"), COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT)),
        })
    return normalized

def _get_catalog_models_for_brand(brand_name: Optional[str]) -> List[Dict[str, Any]]:
    make_name = str(brand_name or "").strip()
    if not make_name:
        return []
    try:
        catalog = _build_catalog_tree_from_source(all_years=True)
        make_entry = _find_catalog_make(catalog, make_name)
        if not make_entry:
            return []
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for model_entry in make_entry.get("models", []):
            model_name = str(model_entry.get("name") or "").strip()
            if not model_name:
                continue
            key = model_name.casefold()
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "model": model_name,
                "min_msrp": _to_non_negative_float(_parse_catalog_price(model_entry.get("min_msrp")), 0.0),
            })
        rows.sort(key=lambda item: item["model"].lower())
        return rows
    except Exception:
        return []

def _build_matrix_models_response(
    catalog_models: List[Dict[str, Any]],
    overrides: List[Dict[str, Any]],
    default_percentage: float,
) -> List[Dict[str, Any]]:
    override_map: Dict[str, Dict[str, Any]] = {
        str(item.get("model") or "").strip().casefold(): item
        for item in overrides
        if str(item.get("model") or "").strip()
    }
    rows: List[Dict[str, Any]] = []
    used: set[str] = set()
    for item in catalog_models:
        model_name = str(item.get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        used.add(model_key)
        override = override_map.get(model_key) or {}
        override_model_percentage = override.get("model_percentage")
        model_percentage = (
            _to_non_negative_float(override_model_percentage, default_percentage)
            if override_model_percentage is not None
            else _to_non_negative_float(default_percentage, 0.0)
        )
        rows.append({
            "model": model_name,
            "min_msrp": _to_non_negative_float(item.get("min_msrp"), 0.0),
            "model_percentage": model_percentage,
            "model_bonus": _to_non_negative_float(override.get("model_bonus"), 0.0),
            "aged_61_90_bonus": _to_non_negative_float(override.get("aged_61_90_bonus"), 0.0),
            "aged_90_plus_bonus": _to_non_negative_float(override.get("aged_90_plus_bonus"), 0.0),
            "plant_incentive_share_pct": min(100.0, _to_non_negative_float(override.get("plant_incentive_share_pct"), COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT)),
            "source": "catalog",
        })

    # Preserve custom rows not present in current catalog.
    custom_rows = []
    for override in overrides:
        model_name = str(override.get("model") or "").strip()
        if not model_name:
            continue
        model_key = model_name.casefold()
        if model_key in used:
            continue
        custom_rows.append({
            "model": model_name,
            "min_msrp": 0.0,
            "model_percentage": _to_non_negative_float(override.get("model_percentage"), _to_non_negative_float(default_percentage, 0.0)),
            "model_bonus": _to_non_negative_float(override.get("model_bonus"), 0.0),
            "aged_61_90_bonus": _to_non_negative_float(override.get("aged_61_90_bonus"), 0.0),
            "aged_90_plus_bonus": _to_non_negative_float(override.get("aged_90_plus_bonus"), 0.0),
            "plant_incentive_share_pct": min(100.0, _to_non_negative_float(override.get("plant_incentive_share_pct"), COMMISSION_MATRIX_DEFAULT_PLANT_SHARE_PCT)),
            "source": "custom",
        })
    custom_rows.sort(key=lambda item: item["model"].lower())
    rows.extend(custom_rows)
    return rows

def _resolve_matrix_volume_bonus_per_unit(volume_tiers: Optional[List[Dict[str, Any]]], seller_month_units: int) -> float:
    units = max(0, int(seller_month_units or 0))
    if units <= 0:
        return 0.0
    best_match: Optional[Dict[str, Any]] = None
    for tier in volume_tiers or []:
        min_units = int(tier.get("min_units") or 0)
        max_units = tier.get("max_units")
        if units < min_units:
            continue
        if max_units is not None and units > int(max_units):
            continue
        if best_match is None or min_units > int(best_match.get("min_units") or 0):
            best_match = tier
    if not best_match:
        return 0.0
    return _to_non_negative_float(best_match.get("bonus_per_unit"), 0.0)

async def _serialize_commission_matrix(agency: Dict[str, Any], matrix_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    brand_name = ""
    group_name = ""
    if agency.get("brand_id") and ObjectId.is_valid(str(agency["brand_id"])):
        brand = await db.brands.find_one({"_id": ObjectId(str(agency["brand_id"]))})
        if brand:
            brand_name = str(brand.get("name") or "")
    if agency.get("group_id") and ObjectId.is_valid(str(agency["group_id"])):
        group = await db.groups.find_one({"_id": ObjectId(str(agency["group_id"]))})
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

    agency = await db.agencies.find_one({"_id": ObjectId(agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    matrix_doc = await db.commission_matrices.find_one({"agency_id": agency_id})
    return await _serialize_commission_matrix(agency, matrix_doc)

async def upsert_commission_matrix(payload: CommissionMatrixUpsert, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_MATRIX_EDITOR_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")
    if not ObjectId.is_valid(payload.agency_id):
        raise HTTPException(status_code=400, detail="Invalid agency_id")

    agency = await db.agencies.find_one({"_id": ObjectId(payload.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    now = datetime.now(timezone.utc)
    normalized_general = _normalize_commission_matrix_general(payload.general.model_dump())
    normalized_models = _normalize_commission_matrix_models([model.model_dump() for model in payload.models])

    await db.commission_matrices.update_one(
        {"agency_id": payload.agency_id},
        {
            "$set": {
                "agency_id": payload.agency_id,
                "brand_id": agency.get("brand_id"),
                "group_id": agency.get("group_id"),
                "general": normalized_general,
                "models": normalized_models,
                "updated_at": now,
                "updated_by": current_user.get("id"),
            },
            "$setOnInsert": {
                "created_at": now,
                "created_by": current_user.get("id"),
            },
        },
        upsert=True,
    )

    matrix_doc = await db.commission_matrices.find_one({"agency_id": payload.agency_id})

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
    
    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(rule_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")
    
    now = datetime.now(timezone.utc)
    rule_doc = {
        "agency_id": rule_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "name": rule_data.name,
        "rule_type": rule_data.rule_type,
        "value": rule_data.value,
        "min_units": rule_data.min_units,
        "max_units": rule_data.max_units,
        "approval_status": COMMISSION_PENDING,
        "approval_comment": None,
        "created_by": current_user.get("id"),
        "submitted_by": current_user.get("id"),
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "created_at": now,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }
    result = await db.commission_rules.insert_one(rule_doc)
    rule_doc["id"] = str(result.inserted_id)

    await log_audit_event(
        request=request,
        current_user=current_user,
        action="create_commission_rule",
        entity_type="commission_rule",
        entity_id=str(result.inserted_id),
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
    status = str(serialized.get("approval_status") or "").strip().lower()
    if status not in {COMMISSION_PENDING, COMMISSION_APPROVED, COMMISSION_REJECTED}:
        # Backward compatibility for pre-approval records.
        serialized["approval_status"] = COMMISSION_APPROVED
    else:
        serialized["approval_status"] = status

    if rule_doc.get("agency_id"):
        agency = await db.agencies.find_one({"_id": ObjectId(rule_doc["agency_id"])})
        if agency:
            serialized["agency_name"] = agency["name"]
    if rule_doc.get("brand_id"):
        brand = await db.brands.find_one({"_id": ObjectId(rule_doc["brand_id"])})
        if brand:
            serialized["brand_name"] = brand["name"]
    if rule_doc.get("group_id"):
        group = await db.groups.find_one({"_id": ObjectId(rule_doc["group_id"])})
        if group:
            serialized["group_name"] = group["name"]

    if rule_doc.get("submitted_by") and ObjectId.is_valid(rule_doc["submitted_by"]):
        submitter = await db.users.find_one({"_id": ObjectId(rule_doc["submitted_by"])})
        if submitter:
            serialized["submitted_by_name"] = submitter.get("name")
    if rule_doc.get("approved_by") and ObjectId.is_valid(rule_doc["approved_by"]):
        approver = await db.users.find_one({"_id": ObjectId(rule_doc["approved_by"])})
        if approver:
            serialized["approved_by_name"] = approver.get("name")
    if rule_doc.get("rejected_by") and ObjectId.is_valid(rule_doc["rejected_by"]):
        rejector = await db.users.find_one({"_id": ObjectId(rule_doc["rejected_by"])})
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
    
    rules = await db.commission_rules.find(query).to_list(1000)
    result = [await _serialize_commission_rule(rule) for rule in rules]
    return result

async def update_commission_rule(rule_id: str, rule_data: CommissionRuleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user.get("role") not in COMMISSION_PROPOSER_ROLES:
        raise HTTPException(status_code=403, detail="Not authorized")

    if not ObjectId.is_valid(rule_id):
        raise HTTPException(status_code=400, detail="Invalid rule_id")
    
    previous = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
    now = datetime.now(timezone.utc)
    await db.commission_rules.update_one({"_id": ObjectId(rule_id)}, {"$set": {
        "name": rule_data.name,
        "rule_type": rule_data.rule_type,
        "value": rule_data.value,
        "min_units": rule_data.min_units,
        "max_units": rule_data.max_units,
        "approval_status": COMMISSION_PENDING,
        "approval_comment": None,
        "submitted_by": current_user.get("id"),
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }})
    rule = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})

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

    previous = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")

    decision = str(approval.decision or "").strip().lower()
    if decision not in {COMMISSION_APPROVED, COMMISSION_REJECTED}:
        raise HTTPException(status_code=400, detail="Decision must be approved or rejected")

    now = datetime.now(timezone.utc)
    update_fields: Dict[str, Any] = {
        "approval_status": decision,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }

    if decision == COMMISSION_APPROVED:
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

    await db.commission_rules.update_one({"_id": ObjectId(rule_id)}, {"$set": update_fields})
    rule = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})

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
    
    previous = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
    previous_status = str(previous.get("approval_status") or "").strip().lower()
    if role in COMMISSION_PROPOSER_ROLES and previous_status == COMMISSION_APPROVED:
        raise HTTPException(status_code=403, detail="Solo gerencia general puede borrar reglas aprobadas")
    await db.commission_rules.delete_one({"_id": ObjectId(rule_id)})

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
    agency = await db.agencies.find_one({"_id": ObjectId(payload.agency_id)})
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
            seller = await db.users.find_one({"_id": ObjectId(seller_id)})
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

    estimated_commission = _calculate_commission_from_rules(
        rules,
        units=payload.units,
        average_ticket=payload.average_ticket,
        average_fi_revenue=payload.average_fi_revenue,
    )
    difference = round(estimated_commission - payload.target_commission, 2)

    suggested_units: Optional[int] = None
    max_units_limit = max(payload.units + 300, 500)
    for candidate_units in range(0, max_units_limit + 1):
        candidate_commission = _calculate_commission_from_rules(
            rules,
            units=candidate_units,
            average_ticket=payload.average_ticket,
            average_fi_revenue=payload.average_fi_revenue,
        )
        if candidate_commission >= payload.target_commission:
            suggested_units = candidate_units
            break

    return {
        "agency_id": payload.agency_id,
        "seller_id": seller_id,
        "target_commission": payload.target_commission,
        "units": payload.units,
        "average_ticket": payload.average_ticket,
        "average_fi_revenue": payload.average_fi_revenue,
        "estimated_commission": estimated_commission,
        "difference_vs_target": difference,
        "suggested_units_to_target": suggested_units,
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

    agency = await db.agencies.find_one({"_id": ObjectId(payload.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    _ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

    seller = await db.users.find_one({"_id": ObjectId(payload.seller_id)})
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    _ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
    if seller.get("role") != UserRole.SELLER:
        raise HTTPException(status_code=400, detail="Selected user is not a seller")
    if seller.get("agency_id") != payload.agency_id:
        raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

    start_date = datetime(payload.year, payload.month, 1, tzinfo=timezone.utc)
    if payload.month == 12:
        end_date = datetime(payload.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(payload.year, payload.month + 1, 1, tzinfo=timezone.utc)

    sales_query = {
        "agency_id": payload.agency_id,
        "seller_id": payload.seller_id,
        "sale_date": {"$gte": start_date, "$lt": end_date},
    }
    sales = await db.sales.find(sales_query).to_list(10000)
    snapshot = {
        "sales_count": len(sales),
        "sales_total": round(sum(_sale_effective_revenue(s) for s in sales), 2),
        "fi_revenue_total": round(sum(s.get("fi_revenue", 0) for s in sales), 2),
        "commission_total": round(sum(s.get("commission", 0) for s in sales), 2),
        "generated_at": datetime.now(timezone.utc),
    }

    now = datetime.now(timezone.utc)
    existing = await db.commission_closures.find_one({
        "seller_id": payload.seller_id,
        "agency_id": payload.agency_id,
        "month": payload.month,
        "year": payload.year,
    })
    if existing and str(existing.get("approval_status") or "").strip().lower() == COMMISSION_APPROVED:
        raise HTTPException(status_code=409, detail="Approved closure cannot be modified")

    closure_doc = {
        "seller_id": payload.seller_id,
        "agency_id": payload.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "month": payload.month,
        "year": payload.year,
        "snapshot": snapshot,
        "approval_status": COMMISSION_PENDING,
        "approval_comment": None,
        "created_by": current_user.get("id"),
        "submitted_by": current_user.get("id"),
        "submitted_at": now,
        "approved_by": None,
        "approved_at": None,
        "rejected_by": None,
        "rejected_at": None,
        "created_at": now,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }

    if existing:
        await db.commission_closures.update_one(
            {"_id": existing["_id"]},
            {"$set": closure_doc},
        )
        closure = await db.commission_closures.find_one({"_id": existing["_id"]})
        entity_id = str(existing["_id"])
        action = "update_commission_closure"
    else:
        result = await db.commission_closures.insert_one(closure_doc)
        closure = await db.commission_closures.find_one({"_id": result.inserted_id})
        entity_id = str(result.inserted_id)
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

    closures = await db.commission_closures.find(query).to_list(1000)
    enriched: List[Dict[str, Any]] = []
    for closure in closures:
        status = str(closure.get("approval_status") or "").strip().lower()
        if status not in {COMMISSION_PENDING, COMMISSION_APPROVED, COMMISSION_REJECTED}:
            closure["approval_status"] = COMMISSION_APPROVED

        if closure.get("seller_id") and ObjectId.is_valid(closure["seller_id"]):
            seller = await db.users.find_one({"_id": ObjectId(closure["seller_id"])})
            if seller:
                closure["seller_name"] = seller.get("name")
        if closure.get("agency_id") and ObjectId.is_valid(closure["agency_id"]):
            agency = await db.agencies.find_one({"_id": ObjectId(closure["agency_id"])})
            if agency:
                closure["agency_name"] = agency.get("name")
        if closure.get("brand_id") and ObjectId.is_valid(closure["brand_id"]):
            brand = await db.brands.find_one({"_id": ObjectId(closure["brand_id"])})
            if brand:
                closure["brand_name"] = brand.get("name")
        if closure.get("group_id") and ObjectId.is_valid(closure["group_id"]):
            group = await db.groups.find_one({"_id": ObjectId(closure["group_id"])})
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

    previous = await db.commission_closures.find_one({"_id": ObjectId(closure_id)})
    _ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este cierre")

    decision = str(approval.decision or "").strip().lower()
    if decision not in {COMMISSION_APPROVED, COMMISSION_REJECTED}:
        raise HTTPException(status_code=400, detail="Decision must be approved or rejected")

    now = datetime.now(timezone.utc)
    update_fields: Dict[str, Any] = {
        "approval_status": decision,
        "updated_at": now,
        "updated_by": current_user.get("id"),
    }
    if decision == COMMISSION_APPROVED:
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

    await db.commission_closures.update_one({"_id": ObjectId(closure_id)}, {"$set": update_fields})
    closure = await db.commission_closures.find_one({"_id": ObjectId(closure_id)})

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
            UserCreate=UserCreate,
            UserLogin=UserLogin,
            PasswordResetRequest=PasswordResetRequest,
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

# CORS helpers
def _normalize_origin(origin: Optional[str]) -> Optional[str]:
    if not origin:
        return None
    value = origin.strip().rstrip("/")
    return value or None

def _build_allowed_origins() -> List[str]:
    origins = {
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    }
    frontend_origin = _normalize_origin(os.environ.get("FRONTEND_URL"))
    if frontend_origin:
        origins.add(frontend_origin)
    return sorted(origins)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=_build_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== STARTUP ==============

async def seed_admin():
    """Seed admin user on startup"""
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@autoconnect.com")
    admin_password = os.environ.get("ADMIN_PASSWORD", "Admin123!")
    
    existing = await db.users.find_one({"email": admin_email})
    if existing is None:
        hashed = hash_password(admin_password)
        await db.users.insert_one({
            "email": admin_email,
            "password_hash": hashed,
            "name": "Admin",
            "role": UserRole.APP_ADMIN,
            "group_id": None,
            "brand_id": None,
            "agency_id": None,
            "created_at": datetime.now(timezone.utc)
        })
        logger.info(f"Admin user created: {admin_email}")
    elif not verify_password(admin_password, existing["password_hash"]):
        await db.users.update_one(
            {"email": admin_email},
            {"$set": {"password_hash": hash_password(admin_password)}}
        )
        logger.info(f"Admin password updated: {admin_email}")

async def create_indexes():
    """Create MongoDB indexes"""
    await db.users.create_index("email", unique=True)
    await db.vehicles.create_index("vin")
    await db.vehicles.create_index("agency_id")
    await db.vehicles.create_index("status")
    await db.sales.create_index("seller_id")
    await db.sales.create_index("agency_id")
    await db.sales.create_index("sale_date")
    await db.audit_logs.create_index("created_at")
    await db.audit_logs.create_index("agency_id")
    await db.audit_logs.create_index("group_id")
    await db.audit_logs.create_index("actor_id")

@app.on_event("startup")
async def startup():
    await create_indexes()
    backfill_summary = await backfill_agency_locations()
    logger.info(
        "Agency location backfill: checked=%s updated=%s city=%s postal_code=%s",
        backfill_summary["checked"],
        backfill_summary["updated"],
        backfill_summary["filled_city"],
        backfill_summary["filled_postal_code"],
    )
    await seed_admin()
    logger.info("AutoConnect API started")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
