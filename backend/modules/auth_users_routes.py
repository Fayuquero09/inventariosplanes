from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request, Response
from schemas.api_models import PasswordResetRequest, UserCreate, UserLogin


@dataclass(frozen=True)
class AuthUsersRouteHandlers:
    register: Callable[[UserCreate, Request], Awaitable[Any]]
    login: Callable[[UserLogin, Response], Awaitable[Any]]
    logout: Callable[[Response], Awaitable[Any]]
    reset_password: Callable[[PasswordResetRequest], Awaitable[Any]]
    get_me: Callable[[Request], Awaitable[Any]]
    google_auth: Callable[[Request, Response], Awaitable[Any]]
    get_users: Callable[[Request], Awaitable[Any]]
    update_user: Callable[[str, Request], Awaitable[Any]]
    delete_user: Callable[[str, Request], Awaitable[Any]]
    get_audit_logs: Callable[..., Awaitable[Any]]
    get_sellers: Callable[..., Awaitable[Any]]


def register_auth_users_routes(router: APIRouter, handlers: AuthUsersRouteHandlers) -> None:
    @router.post("/auth/register")
    async def register_route(user_data: UserCreate, request: Request):
        return await handlers.register(user_data, request)

    @router.post("/auth/login")
    async def login_route(user_data: UserLogin, response: Response):
        return await handlers.login(user_data, response)

    @router.post("/auth/logout")
    async def logout_route(response: Response):
        return await handlers.logout(response)

    @router.post("/auth/reset-password")
    async def reset_password_route(payload: PasswordResetRequest):
        return await handlers.reset_password(payload)

    @router.get("/auth/me")
    async def get_me_route(request: Request):
        return await handlers.get_me(request)

    @router.post("/auth/google")
    async def google_auth_route(request: Request, response: Response):
        return await handlers.google_auth(request, response)

    @router.get("/users")
    async def get_users_route(request: Request):
        return await handlers.get_users(request)

    @router.put("/users/{user_id}")
    async def update_user_route(user_id: str, request: Request):
        return await handlers.update_user(user_id, request)

    @router.delete("/users/{user_id}")
    async def delete_user_route(user_id: str, request: Request):
        return await handlers.delete_user(user_id, request)

    @router.get("/audit-logs")
    async def get_audit_logs_route(
        request: Request,
        agency_id: Optional[str] = None,
        group_id: Optional[str] = None,
        actor_id: Optional[str] = None,
        limit: int = 100,
    ):
        return await handlers.get_audit_logs(
            request=request,
            agency_id=agency_id,
            group_id=group_id,
            actor_id=actor_id,
            limit=limit,
        )

    @router.get("/sellers")
    async def get_sellers_route(
        request: Request,
        agency_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        return await handlers.get_sellers(
            request=request,
            agency_id=agency_id,
            brand_id=brand_id,
            group_id=group_id,
        )
