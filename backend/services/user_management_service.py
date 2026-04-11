from typing import Any, Callable, Dict, Optional, Sequence, Tuple


def normalize_user_email(email: Any) -> str:
    return str(email or "").strip().lower()


def normalize_optional_position(position: Any) -> Optional[str]:
    return str(position or "").strip() or None


def ensure_user_management_role(
    *,
    actor_role: Optional[str],
    app_admin_role: str,
    group_admin_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
) -> None:
    if actor_role not in {app_admin_role, group_admin_role} and not is_dealer_user_manager_role(actor_role):
        raise PermissionError("Not authorized")


def apply_register_scope_constraints(
    *,
    current_user: Dict[str, Any],
    user_data: Any,
    app_admin_role: str,
    app_user_role: str,
    group_admin_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
    get_dealer_assignable_roles: Callable[[Optional[str]], set[str]],
) -> None:
    actor_role = current_user.get("role")
    if actor_role == group_admin_role:
        if user_data.role in [app_admin_role, app_user_role]:
            raise PermissionError("Group admin cannot create app-level users")

        if not current_user.get("group_id"):
            raise PermissionError("Group admin has no assigned group")

        if user_data.group_id and user_data.group_id != current_user["group_id"]:
            raise PermissionError("Cannot create users outside your group")
        user_data.group_id = current_user["group_id"]
        return

    if not is_dealer_user_manager_role(actor_role):
        return

    assignable_roles = get_dealer_assignable_roles(actor_role)
    if user_data.role not in assignable_roles:
        raise PermissionError("Role is not assignable for this dealer manager")
    if not current_user.get("agency_id"):
        raise PermissionError("Dealer manager has no assigned agency")

    if user_data.group_id and user_data.group_id != current_user.get("group_id"):
        raise PermissionError("Cannot create users outside your group")
    if user_data.brand_id and user_data.brand_id != current_user.get("brand_id"):
        raise PermissionError("Cannot create users outside your brand")
    if user_data.agency_id and user_data.agency_id != current_user.get("agency_id"):
        raise PermissionError("Cannot create users outside your agency")

    user_data.group_id = current_user.get("group_id")
    user_data.brand_id = current_user.get("brand_id")
    user_data.agency_id = current_user.get("agency_id")


def validate_role_scope_requirements(
    *,
    role: str,
    brand_id: Optional[str],
    agency_id: Optional[str],
    brand_scoped_roles: Sequence[str],
    agency_scoped_roles: Sequence[str],
) -> None:
    if role in brand_scoped_roles and not brand_id:
        raise ValueError("Brand role requires brand_id")
    if role in agency_scoped_roles and not agency_id:
        raise ValueError("Agency role requires agency_id")


def build_user_document(
    *,
    email: str,
    password_hash: str,
    name: str,
    position: Optional[str],
    role: str,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    created_at: Any,
) -> Dict[str, Any]:
    return {
        "email": email,
        "password_hash": password_hash,
        "name": name,
        "position": position,
        "role": role,
        "group_id": group_id,
        "brand_id": brand_id,
        "agency_id": agency_id,
        "created_at": created_at,
    }


def extract_new_password_and_payload(data: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any]]:
    payload = dict(data or {})
    raw_new_password = payload.pop("new_password", None)
    if raw_new_password is None:
        return None, payload

    new_password = str(raw_new_password)
    if len(new_password) < 8:
        raise ValueError("Password must be at least 8 characters")
    return new_password, payload


def sanitize_user_update_data(data: Dict[str, Any]) -> Dict[str, Any]:
    update_data = {
        key: value
        for key, value in (data or {}).items()
        if key not in ["id", "_id", "password_hash", "password", "email", "confirm_new_password"]
    }
    if "position" in update_data:
        update_data["position"] = normalize_optional_position(update_data.get("position"))
    return update_data


def enforce_update_scope_permissions(
    *,
    current_user: Dict[str, Any],
    existing_user: Dict[str, Any],
    update_data: Dict[str, Any],
    app_admin_role: str,
    app_user_role: str,
    group_admin_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
    get_dealer_assignable_roles: Callable[[Optional[str]], set[str]],
    same_scope_id: Callable[[Any, Any], bool],
) -> Dict[str, Any]:
    actor_role = current_user.get("role")

    if str(existing_user.get("_id")) == str(current_user.get("id")) and actor_role != app_admin_role:
        raise PermissionError("Self-edit is only allowed for app admin")
    if existing_user.get("role") == app_admin_role and actor_role != app_admin_role:
        raise PermissionError("Cannot edit app admin users")

    if actor_role == group_admin_role:
        current_group_id = current_user.get("group_id")
        if not current_group_id:
            raise PermissionError("Group admin has no assigned group")
        if not same_scope_id(existing_user.get("group_id"), current_group_id):
            raise PermissionError("Cannot edit users outside your group")
        if update_data.get("group_id") and not same_scope_id(update_data["group_id"], current_group_id):
            raise PermissionError("Cannot move users outside your group")
        requested_role = update_data.get("role")
        if requested_role == app_admin_role:
            raise PermissionError("Group admin cannot assign app admin role")
        if requested_role == app_user_role and existing_user.get("role") != app_user_role:
            raise PermissionError("Group admin cannot assign app user role")

    if is_dealer_user_manager_role(actor_role):
        current_agency_id = current_user.get("agency_id")
        if not current_agency_id:
            raise PermissionError("Dealer manager has no assigned agency")
        if not same_scope_id(existing_user.get("agency_id"), current_agency_id):
            raise PermissionError("Cannot edit users outside your agency")

        assignable_roles = get_dealer_assignable_roles(actor_role)
        existing_role = existing_user.get("role")
        if existing_role not in assignable_roles:
            raise PermissionError("Cannot edit this user role from your dealer scope")

        if update_data.get("role") and update_data["role"] not in assignable_roles:
            raise PermissionError("Role is not assignable for this dealer manager")
        if update_data.get("agency_id") and not same_scope_id(update_data["agency_id"], current_agency_id):
            raise PermissionError("Cannot move users outside your agency")

        update_data["group_id"] = current_user.get("group_id")
        update_data["brand_id"] = current_user.get("brand_id")
        update_data["agency_id"] = current_agency_id

    return update_data


def build_user_update_audit_changes(update_data: Dict[str, Any]) -> Dict[str, Any]:
    audit_changes = {key: value for key, value in (update_data or {}).items() if key != "password_hash"}
    if "password_hash" in (update_data or {}):
        audit_changes["password_updated"] = True
    return audit_changes


def enforce_delete_scope_permissions(
    *,
    current_user: Dict[str, Any],
    existing_user: Dict[str, Any],
    app_admin_role: str,
    group_admin_role: str,
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
    get_dealer_assignable_roles: Callable[[Optional[str]], set[str]],
    same_scope_id: Callable[[Any, Any], bool],
) -> None:
    actor_role = current_user.get("role")
    if existing_user.get("role") == app_admin_role and actor_role != app_admin_role:
        raise PermissionError("Cannot delete app admin users")

    if str(existing_user.get("_id")) == str(current_user.get("id")):
        raise ValueError("Cannot delete your own user")

    if actor_role == group_admin_role:
        current_group_id = current_user.get("group_id")
        if not current_group_id:
            raise PermissionError("Group admin has no assigned group")
        if not same_scope_id(existing_user.get("group_id"), current_group_id):
            raise PermissionError("Cannot delete users outside your group")

    if not is_dealer_user_manager_role(actor_role):
        return

    current_agency_id = current_user.get("agency_id")
    if not current_agency_id:
        raise PermissionError("Dealer manager has no assigned agency")
    if not same_scope_id(existing_user.get("agency_id"), current_agency_id):
        raise PermissionError("Cannot delete users outside your agency")
    assignable_roles = get_dealer_assignable_roles(actor_role)
    if existing_user.get("role") not in assignable_roles:
        raise PermissionError("Cannot delete this user role from your dealer scope")
