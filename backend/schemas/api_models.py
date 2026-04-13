from typing import Any, Dict, List, Optional

from pydantic import BaseModel, EmailStr, Field


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
    tiie_rate: Optional[float] = None
    spread: Optional[float] = None
    grace_days: int = 0
    name: str


class FinancialRateBulkApplyRequest(BaseModel):
    group_id: str


class FinancialRateResponse(BaseModel):
    id: str
    group_id: str
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    tiie_rate: Optional[float] = None
    spread: Optional[float] = None
    total_rate: Optional[float] = None
    effective_tiie_rate: Optional[float] = None
    effective_spread: Optional[float] = None
    effective_total_rate: Optional[float] = None
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
    vehicle_type: str
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
    status: str
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
    decision: str
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
    rule_type: str
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
    decision: str
    comment: Optional[str] = None


class CommissionClosureCreate(BaseModel):
    seller_id: str
    agency_id: str
    month: int = Field(..., ge=1, le=12)
    year: int = Field(..., ge=2000, le=2100)


class CommissionClosureApprovalAction(BaseModel):
    decision: str
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
    group_id: Optional[str] = None
    fiscal_close_day: Optional[int] = Field(default=None, ge=1, le=31)
    industry_close_day: Optional[int] = Field(default=None, ge=1, le=31)
    industry_close_month_offset: int = Field(default=0, ge=0, le=1)
