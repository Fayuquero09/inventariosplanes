from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request, Response


@dataclass(frozen=True)
class AuthUsersHandlerBundle:
    register: Callable[[Any, Request], Awaitable[Any]]
    login: Callable[[Any, Response], Awaitable[Any]]
    logout: Callable[[Response], Awaitable[Any]]
    reset_password: Callable[[Any], Awaitable[Any]]
    get_me: Callable[[Request], Awaitable[Any]]
    google_auth: Callable[[Request, Response], Awaitable[Any]]
    get_users: Callable[[Request], Awaitable[Any]]
    update_user: Callable[[str, Request], Awaitable[Any]]
    delete_user: Callable[[str, Request], Awaitable[Any]]
    get_audit_logs: Callable[..., Awaitable[Any]]
    get_sellers: Callable[..., Awaitable[Any]]


def build_auth_users_route_handlers(
    *,
    db: Any,
    object_id_cls: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    app_admin_role: str,
    app_user_role: str,
    group_admin_role: str,
    group_finance_role: str,
    seller_role: str,
    brand_admin_role: str,
    brand_user_role: str,
    agency_admin_role: str,
    agency_sales_manager_role: str,
    agency_general_manager_role: str,
    agency_commercial_manager_role: str,
    agency_user_role: str,
    action_users_manage: str,
    action_audit_logs_read: str,
    require_action_role: Callable[..., None],
    is_dealer_user_manager_role: Callable[[Optional[str]], bool],
    get_dealer_assignable_roles: Callable[[str], Sequence[str]],
    same_scope_id: Callable[[Any, Any], bool],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    apply_register_scope_constraints: Callable[..., None],
    normalize_user_email: Callable[[str], str],
    resolve_register_hierarchy_scope: Callable[..., Awaitable[None]],
    validate_role_scope_requirements: Callable[..., None],
    normalize_optional_position: Callable[[Optional[str]], Optional[str]],
    build_user_document: Callable[..., Dict[str, Any]],
    find_user_by_email: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    create_user: Callable[..., Awaitable[str]],
    hash_password: Callable[[str], str],
    login_user: Callable[..., Awaitable[Dict[str, Any]]],
    verify_password: Callable[..., bool],
    create_access_token: Callable[..., str],
    create_refresh_token: Callable[..., str],
    reset_password_flow: Callable[..., Awaitable[Any]],
    google_auth_flow: Callable[..., Awaitable[Dict[str, Any]]],
    build_users_query_for_actor: Callable[..., Any],
    list_users: Callable[..., Awaitable[Any]],
    extract_new_password_and_payload: Callable[..., Any],
    sanitize_user_update_data: Callable[..., Dict[str, Any]],
    find_user_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    enforce_update_scope_permissions: Callable[..., Dict[str, Any]],
    update_user_by_id: Callable[..., Awaitable[Any]],
    build_user_update_audit_changes: Callable[..., Dict[str, Any]],
    enforce_delete_scope_permissions: Callable[..., None],
    delete_user_by_id: Callable[..., Awaitable[Any]],
    build_audit_logs_query_for_actor: Callable[..., Any],
    list_audit_logs: Callable[..., Awaitable[Any]],
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    serialize_doc: Callable[[Any], Any],
    log_audit_event: Callable[..., Awaitable[None]],
    update_user_password_hash: Callable[..., Awaitable[Any]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id_register: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
) -> AuthUsersHandlerBundle:
    async def register(user_data: Any, request: Request):
        current_user = await get_current_user(request)
        actor_role = current_user.get("role")
        require_action_role(action_users_manage, actor_role, detail="Not authorized")
        try:
            apply_register_scope_constraints(
                current_user=current_user,
                user_data=user_data,
                app_admin_role=app_admin_role,
                app_user_role=app_user_role,
                group_admin_role=group_admin_role,
                is_dealer_user_manager_role=is_dealer_user_manager_role,
                get_dealer_assignable_roles=get_dealer_assignable_roles,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

        email = normalize_user_email(user_data.email)
        existing = await find_user_by_email(db, email)
        if existing:
            raise HTTPException(status_code=400, detail="Email already registered")

        await resolve_register_hierarchy_scope(
            db=db,
            user_data=user_data,
            find_brand_by_id=find_brand_by_id,
            find_agency_by_id=find_agency_by_id_register,
        )

        try:
            validate_role_scope_requirements(
                role=user_data.role,
                brand_id=user_data.brand_id,
                agency_id=user_data.agency_id,
                brand_scoped_roles=[brand_admin_role, brand_user_role],
                agency_scoped_roles=[
                    agency_admin_role,
                    agency_sales_manager_role,
                    agency_general_manager_role,
                    agency_commercial_manager_role,
                    agency_user_role,
                    seller_role,
                ],
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        normalized_position = normalize_optional_position(user_data.position)

        user_doc = build_user_document(
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
            "created_at": user_doc["created_at"].isoformat(),
        }

    async def login(user_data: Any, response: Response):
        login_result = await login_user(
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

    async def reset_password(payload: Any):
        return await reset_password_flow(
            db,
            payload=payload,
            find_user_by_email=find_user_by_email,
            update_user_password_hash=update_user_password_hash,
            hash_password=hash_password,
        )

    async def get_me(request: Request):
        return await get_current_user(request)

    async def google_auth(request: Request, response: Response):
        data = await request.json()
        credential = data.get("credential")
        try:
            auth_result = await google_auth_flow(
                db,
                credential=credential,
                find_user_by_email=find_user_by_email,
                create_user=create_user,
                create_access_token=create_access_token,
                create_refresh_token=create_refresh_token,
                app_user_role=app_user_role,
            )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid Google credential: {str(exc)}") from exc

        response.set_cookie(key="access_token", value=auth_result["access_token"], httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
        response.set_cookie(key="refresh_token", value=auth_result["refresh_token"], httponly=True, secure=False, samesite="lax", max_age=604800, path="/")

        return auth_result["user_payload"]

    async def get_users(request: Request):
        current_user = await get_current_user(request)
        actor_role = current_user.get("role")
        require_action_role(action_users_manage, actor_role, detail="Not authorized")

        query, should_return_empty = build_users_query_for_actor(
            actor_role=actor_role,
            current_user=current_user,
            group_admin_role=group_admin_role,
            is_dealer_user_manager_role=is_dealer_user_manager_role,
        )
        if should_return_empty:
            return []

        users = await list_users(db, query, include_password_hash=False, limit=1000)
        return [serialize_doc(u) for u in users]

    async def update_user(user_id: str, request: Request):
        current_user = await get_current_user(request)
        actor_role = current_user.get("role")
        require_action_role(action_users_manage, actor_role, detail="Not authorized")

        data = await request.json()
        try:
            new_password, payload_without_password = extract_new_password_and_payload(data)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        update_data = sanitize_user_update_data(payload_without_password)

        if not object_id_cls.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user_id")
        existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        try:
            update_data = enforce_update_scope_permissions(
                current_user=current_user,
                existing_user=existing_user,
                update_data=update_data,
                app_admin_role=app_admin_role,
                app_user_role=app_user_role,
                group_admin_role=group_admin_role,
                is_dealer_user_manager_role=is_dealer_user_manager_role,
                get_dealer_assignable_roles=get_dealer_assignable_roles,
                same_scope_id=same_scope_id,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

        if new_password:
            update_data["password_hash"] = hash_password(new_password)

        if update_data:
            await update_user_by_id(db, user_id, update_data)
        user = await find_user_by_id(db, user_id, include_password_hash=False)

        audit_changes = build_user_update_audit_changes(update_data)

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
        require_action_role(action_users_manage, actor_role, detail="Not authorized")

        if not object_id_cls.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user_id")

        existing_user = await find_user_by_id(db, user_id, include_password_hash=False)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        try:
            enforce_delete_scope_permissions(
                current_user=current_user,
                existing_user=existing_user,
                app_admin_role=app_admin_role,
                group_admin_role=group_admin_role,
                is_dealer_user_manager_role=is_dealer_user_manager_role,
                get_dealer_assignable_roles=get_dealer_assignable_roles,
                same_scope_id=same_scope_id,
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
        limit: int = 100,
    ):
        current_user = await get_current_user(request)
        actor_role = current_user.get("role")
        require_action_role(action_audit_logs_read, actor_role, detail="Not authorized")

        safe_limit = max(1, min(limit, 500))
        query, should_return_empty = build_audit_logs_query_for_actor(
            actor_role=actor_role,
            current_user=current_user,
            agency_id=agency_id,
            group_id=group_id,
            actor_id=actor_id,
            group_admin_role=group_admin_role,
            group_finance_role=group_finance_role,
            is_dealer_user_manager_role=is_dealer_user_manager_role,
        )
        if should_return_empty:
            return []

        logs = await list_audit_logs(db, query, limit=safe_limit)
        return [serialize_doc(item) for item in logs]

    async def get_sellers(
        request: Request,
        agency_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        current_user = await get_current_user(request)

        query = {"role": seller_role}
        scope_query = build_scope_query(current_user)
        if not scope_query_has_access(scope_query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        query.update({k: v for k, v in scope_query.items() if k in {"group_id", "brand_id", "agency_id"}})

        if group_id:
            query["group_id"] = group_id
        if brand_id:
            query["brand_id"] = brand_id
        if agency_id:
            query["agency_id"] = agency_id

        sellers = await list_users(db, query, include_password_hash=False, limit=1000)

        result = []
        for seller in sellers:
            serialized = serialize_doc(seller)
            if seller.get("agency_id"):
                agency = await find_agency_by_id(db, seller["agency_id"])
                if agency:
                    serialized["agency_name"] = agency["name"]
            result.append(serialized)

        return result

    return AuthUsersHandlerBundle(
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
    )
