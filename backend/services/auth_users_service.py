from typing import Any, Callable, Dict, Optional, Tuple

from fastapi import HTTPException


async def resolve_register_hierarchy_scope(
    *,
    db: Any,
    user_data: Any,
    find_brand_by_id: Callable[[Any, str], Any],
    find_agency_by_id: Callable[[Any, str], Any],
) -> None:
    if user_data.brand_id:
        brand = await find_brand_by_id(db, user_data.brand_id)
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")
        if user_data.group_id and brand.get("group_id") != user_data.group_id:
            raise HTTPException(status_code=400, detail="Brand does not belong to selected group")
        if not user_data.group_id:
            user_data.group_id = brand.get("group_id")

    if not user_data.agency_id:
        return

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


def build_users_query_for_actor(
    *,
    actor_role: Optional[str],
    current_user: Dict[str, Any],
    group_admin_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
) -> Tuple[Dict[str, Any], bool]:
    query: Dict[str, Any] = {}
    if actor_role == group_admin_role and current_user.get("group_id"):
        query["group_id"] = current_user["group_id"]
        return query, False

    if not is_dealer_user_manager_role(actor_role):
        return query, False

    if not current_user.get("agency_id"):
        return {}, True
    query["agency_id"] = current_user["agency_id"]
    return query, False


def build_audit_logs_query_for_actor(
    *,
    actor_role: Optional[str],
    current_user: Dict[str, Any],
    agency_id: Optional[str],
    group_id: Optional[str],
    actor_id: Optional[str],
    group_admin_role: str,
    group_finance_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
) -> Tuple[Dict[str, Any], bool]:
    query: Dict[str, Any] = {}

    if actor_role in [group_admin_role, group_finance_role]:
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return {}, True
        query["group_id"] = user_group_id
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    elif is_dealer_user_manager_role(actor_role):
        user_agency_id = current_user.get("agency_id")
        if not user_agency_id:
            return {}, True
        query["agency_id"] = user_agency_id
        if agency_id and agency_id != user_agency_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a esta agencia")
        user_group_id = current_user.get("group_id")
        if group_id and user_group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    elif group_id:
        query["group_id"] = group_id

    if agency_id and not is_dealer_user_manager_role(actor_role):
        query["agency_id"] = agency_id
    if actor_id:
        query["actor_id"] = actor_id
    return query, False
