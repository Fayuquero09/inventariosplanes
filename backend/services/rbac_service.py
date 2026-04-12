from typing import Any, Dict, Optional

from fastapi import HTTPException


ROLE_APP_ADMIN = "app_admin"
ROLE_APP_USER = "app_user"
ROLE_GROUP_ADMIN = "group_admin"
ROLE_GROUP_FINANCE_MANAGER = "group_finance_manager"
ROLE_BRAND_ADMIN = "brand_admin"
ROLE_BRAND_USER = "brand_user"
ROLE_AGENCY_ADMIN = "agency_admin"
ROLE_AGENCY_SALES_MANAGER = "agency_sales_manager"
ROLE_AGENCY_GENERAL_MANAGER = "agency_general_manager"
ROLE_AGENCY_COMMERCIAL_MANAGER = "agency_commercial_manager"
ROLE_AGENCY_USER = "agency_user"
ROLE_SELLER = "seller"


APP_LEVEL_ROLES = {ROLE_APP_ADMIN, ROLE_APP_USER}
BRAND_SCOPED_ROLES = {ROLE_BRAND_ADMIN, ROLE_BRAND_USER}
AGENCY_SCOPED_ROLES = {
    ROLE_AGENCY_ADMIN,
    ROLE_AGENCY_SALES_MANAGER,
    ROLE_AGENCY_GENERAL_MANAGER,
    ROLE_AGENCY_COMMERCIAL_MANAGER,
    ROLE_AGENCY_USER,
    ROLE_SELLER,
}
CORP_STRUCTURE_ROLES = {
    ROLE_APP_ADMIN,
    ROLE_GROUP_ADMIN,
}
CORP_FINANCE_ROLES = {
    ROLE_APP_ADMIN,
    ROLE_GROUP_FINANCE_MANAGER,
}
DEALER_GENERAL_EFFECTIVE_ROLES = {
    ROLE_AGENCY_GENERAL_MANAGER,
    ROLE_AGENCY_ADMIN,  # legacy write role
    ROLE_AGENCY_COMMERCIAL_MANAGER,  # legacy approver equivalent
}
DEALER_SALES_EFFECTIVE_ROLES = {
    ROLE_AGENCY_SALES_MANAGER,
}
DEALER_SELLER_ROLE = ROLE_SELLER
DEALER_LEGACY_READONLY_ROLE = ROLE_AGENCY_USER
DEALER_USER_MANAGER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES | DEALER_SALES_EFFECTIVE_ROLES

DEALER_GENERAL_ASSIGNABLE_ROLES = {
    ROLE_AGENCY_SALES_MANAGER,
    ROLE_SELLER,
    ROLE_AGENCY_USER,  # legacy read-only compatibility
}
DEALER_SALES_ASSIGNABLE_ROLES = {
    ROLE_SELLER,
}

WRITE_AUDIT_ROLES = {
    ROLE_APP_ADMIN,
    ROLE_GROUP_ADMIN,
    ROLE_GROUP_FINANCE_MANAGER,
    ROLE_BRAND_ADMIN,
    ROLE_AGENCY_ADMIN,
    ROLE_AGENCY_SALES_MANAGER,
    ROLE_AGENCY_GENERAL_MANAGER,
    ROLE_AGENCY_COMMERCIAL_MANAGER,
    ROLE_SELLER,
}

FINANCIAL_RATE_MANAGER_ROLES = CORP_FINANCE_ROLES
PRICE_BULLETIN_EDITOR_ROLES = {
    ROLE_APP_ADMIN,
    ROLE_GROUP_ADMIN,
    ROLE_GROUP_FINANCE_MANAGER,
    ROLE_BRAND_ADMIN,
    ROLE_AGENCY_GENERAL_MANAGER,
    ROLE_AGENCY_SALES_MANAGER,
    ROLE_AGENCY_ADMIN,
    ROLE_AGENCY_COMMERCIAL_MANAGER,
}
OBJECTIVE_EDITOR_ROLES = DEALER_SALES_EFFECTIVE_ROLES
OBJECTIVE_APPROVER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES
COMMISSION_PROPOSER_ROLES = DEALER_SALES_EFFECTIVE_ROLES
COMMISSION_APPROVER_ROLES = DEALER_GENERAL_EFFECTIVE_ROLES
COMMISSION_MATRIX_EDITOR_ROLES = (
    DEALER_SALES_EFFECTIVE_ROLES
    | DEALER_GENERAL_EFFECTIVE_ROLES
    | {
        ROLE_APP_ADMIN,
        ROLE_GROUP_ADMIN,
        ROLE_GROUP_FINANCE_MANAGER,
        ROLE_BRAND_ADMIN,
    }
)

ACTION_USERS_MANAGE = "users.manage"
ACTION_AUDIT_LOGS_READ = "audit-logs.read"

ACTION_ROLE_MATRIX = {
    ACTION_USERS_MANAGE: {ROLE_APP_ADMIN, ROLE_GROUP_ADMIN} | DEALER_USER_MANAGER_ROLES,
    ACTION_AUDIT_LOGS_READ: {
        ROLE_APP_ADMIN,
        ROLE_GROUP_ADMIN,
        ROLE_GROUP_FINANCE_MANAGER,
    }
    | DEALER_USER_MANAGER_ROLES,
}


def can_action_role(action: str, role: Optional[str]) -> bool:
    allowed = ACTION_ROLE_MATRIX.get(action)
    if not allowed:
        return False
    return role in allowed


def require_action_role(action: str, role: Optional[str], detail: str = "Not authorized") -> None:
    if not can_action_role(action, role):
        raise HTTPException(status_code=403, detail=detail)


def same_scope_id(left: Optional[str], right: Optional[str]) -> bool:
    if left is None or right is None:
        return False
    return str(left) == str(right)


def empty_scope_query() -> Dict[str, Any]:
    return {"_id": "__none__"}


def is_app_level_role(role: Optional[str]) -> bool:
    return role in APP_LEVEL_ROLES


def is_brand_scoped_role(role: Optional[str]) -> bool:
    return role in BRAND_SCOPED_ROLES


def is_agency_scoped_role(role: Optional[str]) -> bool:
    return role in AGENCY_SCOPED_ROLES


def is_corp_structure_role(role: Optional[str]) -> bool:
    return role in CORP_STRUCTURE_ROLES


def is_corp_finance_role(role: Optional[str]) -> bool:
    return role in CORP_FINANCE_ROLES


def is_dealer_general_effective_role(role: Optional[str]) -> bool:
    return role in DEALER_GENERAL_EFFECTIVE_ROLES


def is_dealer_sales_effective_role(role: Optional[str]) -> bool:
    return role in DEALER_SALES_EFFECTIVE_ROLES


def is_dealer_user_manager_role(role: Optional[str]) -> bool:
    return role in DEALER_USER_MANAGER_ROLES


def get_dealer_assignable_roles(role: Optional[str]) -> set[str]:
    if is_dealer_general_effective_role(role):
        return DEALER_GENERAL_ASSIGNABLE_ROLES
    if is_dealer_sales_effective_role(role):
        return DEALER_SALES_ASSIGNABLE_ROLES
    return set()


def build_scope_query(current_user: Dict[str, Any]) -> Dict[str, Any]:
    role = current_user.get("role")
    if is_app_level_role(role):
        return {}

    user_group_id = current_user.get("group_id")
    if not user_group_id:
        return empty_scope_query()

    query: Dict[str, Any] = {"group_id": user_group_id}

    if is_brand_scoped_role(role):
        user_brand_id = current_user.get("brand_id")
        if not user_brand_id:
            return empty_scope_query()
        query["brand_id"] = user_brand_id

    if is_agency_scoped_role(role):
        user_agency_id = current_user.get("agency_id")
        if not user_agency_id:
            return empty_scope_query()
        query["agency_id"] = user_agency_id
        if current_user.get("brand_id"):
            query["brand_id"] = current_user["brand_id"]

    return query


def scope_query_has_access(query: Dict[str, Any]) -> bool:
    return query.get("_id") != "__none__"


def validate_scope_filters(
    current_user: Dict[str, Any],
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
) -> None:
    role = current_user.get("role")
    if is_app_level_role(role):
        return

    user_group_id = current_user.get("group_id")
    user_brand_id = current_user.get("brand_id")
    user_agency_id = current_user.get("agency_id")

    if group_id and user_group_id and not same_scope_id(group_id, user_group_id):
        raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")

    if is_brand_scoped_role(role):
        if brand_id and user_brand_id and not same_scope_id(brand_id, user_brand_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta marca")

    if is_agency_scoped_role(role):
        if brand_id and user_brand_id and not same_scope_id(brand_id, user_brand_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta marca")
        if agency_id and user_agency_id and not same_scope_id(agency_id, user_agency_id):
            raise HTTPException(status_code=403, detail="No tienes acceso a esta agencia")


def ensure_doc_scope_access(
    current_user: Dict[str, Any],
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
    if is_app_level_role(role):
        return

    user_group_id = current_user.get("group_id")
    user_brand_id = current_user.get("brand_id")
    user_agency_id = current_user.get("agency_id")

    if user_group_id and not same_scope_id(doc.get(group_field), user_group_id):
        raise HTTPException(status_code=403, detail=detail)

    if is_brand_scoped_role(role):
        if user_brand_id and not same_scope_id(doc.get(brand_field), user_brand_id):
            raise HTTPException(status_code=403, detail=detail)

    if is_agency_scoped_role(role):
        if user_agency_id and not same_scope_id(doc.get(agency_field), user_agency_id):
            raise HTTPException(status_code=403, detail=detail)
