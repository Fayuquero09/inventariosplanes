from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional


@dataclass(frozen=True)
class AuthRuntimeHelperBundle:
    hash_password: Callable[[str], str]
    verify_password: Callable[[str, str], bool]
    create_access_token: Callable[[str, str, str], str]
    create_refresh_token: Callable[[str], str]
    get_current_user: Callable[[Any], Awaitable[dict]]
    get_optional_user: Callable[[Any], Awaitable[Optional[dict]]]


def build_auth_runtime_helper_bundle(
    *,
    db: Any,
    get_jwt_secret: Callable[[], str],
    jwt_algorithm: str,
    access_token_expires_minutes: int,
    refresh_token_expires_days: int,
    hash_password_service: Callable[[str], str],
    verify_password_service: Callable[[str, str], bool],
    create_access_token_service: Callable[..., str],
    create_refresh_token_service: Callable[..., str],
    get_current_user_service: Callable[..., Awaitable[dict]],
    get_optional_user_service: Callable[..., Awaitable[Optional[dict]]],
) -> AuthRuntimeHelperBundle:
    def hash_password(password: str) -> str:
        return hash_password_service(password)

    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return verify_password_service(plain_password, hashed_password)

    def create_access_token(user_id: str, email: str, role: str) -> str:
        return create_access_token_service(
            user_id=user_id,
            email=email,
            role=role,
            jwt_secret=get_jwt_secret(),
            jwt_algorithm=jwt_algorithm,
            expires_minutes=access_token_expires_minutes,
        )

    def create_refresh_token(user_id: str) -> str:
        return create_refresh_token_service(
            user_id=user_id,
            jwt_secret=get_jwt_secret(),
            jwt_algorithm=jwt_algorithm,
            expires_days=refresh_token_expires_days,
        )

    async def get_current_user(request: Any) -> dict:
        return await get_current_user_service(
            request=request,
            db=db,
            jwt_secret=get_jwt_secret(),
            jwt_algorithm=jwt_algorithm,
        )

    async def get_optional_user(request: Any) -> Optional[dict]:
        return await get_optional_user_service(
            request=request,
            db=db,
            jwt_secret=get_jwt_secret(),
            jwt_algorithm=jwt_algorithm,
        )

    return AuthRuntimeHelperBundle(
        hash_password=hash_password,
        verify_password=verify_password,
        create_access_token=create_access_token,
        create_refresh_token=create_refresh_token,
        get_current_user=get_current_user,
        get_optional_user=get_optional_user,
    )
