from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
import jwt
from fastapi import HTTPException, Request
from bson import ObjectId


def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(
    *,
    user_id: str,
    email: str,
    role: str,
    jwt_secret: str,
    jwt_algorithm: str,
    expires_minutes: int,
) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=expires_minutes),
        "type": "access",
    }
    return jwt.encode(payload, jwt_secret, algorithm=jwt_algorithm)


def create_refresh_token(
    *,
    user_id: str,
    jwt_secret: str,
    jwt_algorithm: str,
    expires_days: int,
) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=expires_days),
        "type": "refresh",
    }
    return jwt.encode(payload, jwt_secret, algorithm=jwt_algorithm)


async def get_current_user(
    *,
    request: Request,
    db: Any,
    jwt_secret: str,
    jwt_algorithm: str,
) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=[jwt_algorithm])
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


async def get_optional_user(
    *,
    request: Request,
    db: Any,
    jwt_secret: str,
    jwt_algorithm: str,
) -> Optional[dict]:
    try:
        return await get_current_user(
            request=request,
            db=db,
            jwt_secret=jwt_secret,
            jwt_algorithm=jwt_algorithm,
        )
    except Exception:
        return None
