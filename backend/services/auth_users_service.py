from datetime import datetime, timezone
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


def normalize_login_email(email: Any) -> str:
    return str(email or "").strip().lower()


def _format_created_at(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


async def login_user(
    db: Any,
    *,
    user_data: Any,
    find_user_by_email: Callable[[Any, str], Any],
    verify_password: Callable[[str, str], bool],
    create_access_token: Callable[[str, str, str], str],
    create_refresh_token: Callable[[str], str],
) -> Dict[str, Any]:
    email = normalize_login_email(user_data.email)
    user = await find_user_by_email(db, email)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    password_raw = user_data.password or ""
    password_trimmed = password_raw.strip()
    if not (
        verify_password(password_raw, user["password_hash"])
        or (password_trimmed != password_raw and verify_password(password_trimmed, user["password_hash"]))
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id = str(user["_id"])
    access_token = create_access_token(user_id, email, user["role"])
    refresh_token = create_refresh_token(user_id)
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user_payload": {
            "id": user_id,
            "email": user.get("email"),
            "name": user.get("name"),
            "position": user.get("position"),
            "role": user.get("role"),
            "group_id": user.get("group_id"),
            "brand_id": user.get("brand_id"),
            "agency_id": user.get("agency_id"),
            "created_at": _format_created_at(user.get("created_at")),
            "token": access_token,
        },
    }


async def reset_password_flow(
    db: Any,
    *,
    payload: Any,
    find_user_by_email: Callable[[Any, str], Any],
    update_user_password_hash: Callable[[Any, str, str], Any],
    hash_password: Callable[[str], str],
) -> Dict[str, Any]:
    email = normalize_login_email(payload.email)
    if len(payload.new_password or "") < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    user = await find_user_by_email(db, email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await update_user_password_hash(db, str(user["_id"]), hash_password(payload.new_password))
    return {"message": "Password updated successfully"}


def _decode_google_credential(credential: str) -> Dict[str, Any]:
    import base64
    import json

    parts = credential.split(".")
    payload = parts[1]
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding
    decoded = base64.urlsafe_b64decode(payload)
    return json.loads(decoded)


async def google_auth_flow(
    db: Any,
    *,
    credential: str,
    find_user_by_email: Callable[[Any, str], Any],
    create_user: Callable[[Any, Dict[str, Any]], str],
    create_access_token: Callable[[str, str, str], str],
    create_refresh_token: Callable[[str], str],
    app_user_role: str,
) -> Dict[str, Any]:
    if not credential:
        raise HTTPException(status_code=400, detail="No credential provided")

    try:
        google_user = _decode_google_credential(credential)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid Google credential: {str(exc)}") from exc

    email = normalize_login_email(google_user.get("email", ""))
    name = google_user.get("name", "")

    user = await find_user_by_email(db, email)
    if not user:
        user_doc = {
            "email": email,
            "password_hash": "",
            "name": name,
            "position": None,
            "role": app_user_role,
            "group_id": None,
            "brand_id": None,
            "agency_id": None,
            "google_id": google_user.get("sub"),
            "created_at": datetime.now(timezone.utc),
        }
        user_id = await create_user(db, user_doc)
        user = {**user_doc, "_id": user_id}
    else:
        user_id = str(user["_id"])

    access_token = create_access_token(user_id, email, user.get("role", app_user_role))
    refresh_token = create_refresh_token(user_id)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user_payload": {
            "id": user_id,
            "email": email,
            "name": user.get("name", name),
            "position": user.get("position"),
            "role": user.get("role", app_user_role),
            "group_id": user.get("group_id"),
            "brand_id": user.get("brand_id"),
            "agency_id": user.get("agency_id"),
            "created_at": _format_created_at(user.get("created_at", datetime.now(timezone.utc))),
            "token": access_token,
        },
    }
