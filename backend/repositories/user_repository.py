from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def find_user_by_email(db: Any, email: str) -> Optional[Dict[str, Any]]:
    return await db.users.find_one({"email": str(email).strip().lower()})


async def create_user(db: Any, user_doc: Dict[str, Any]) -> str:
    result = await db.users.insert_one(user_doc)
    return str(result.inserted_id)


async def find_user_by_id(
    db: Any,
    user_id: str,
    *,
    include_password_hash: bool = True,
) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(user_id)):
        return None
    projection = None if include_password_hash else {"password_hash": 0}
    return await db.users.find_one({"_id": ObjectId(str(user_id))}, projection)


async def update_user_by_id(db: Any, user_id: str, update_fields: Dict[str, Any]) -> int:
    if not ObjectId.is_valid(str(user_id)):
        return 0
    if not update_fields:
        return 0
    result = await db.users.update_one({"_id": ObjectId(str(user_id))}, {"$set": update_fields})
    return int(result.modified_count or 0)


async def update_user_password_hash(db: Any, user_id: str, password_hash: str) -> int:
    return await update_user_by_id(db, user_id, {"password_hash": password_hash})


async def delete_user_by_id(db: Any, user_id: str) -> int:
    if not ObjectId.is_valid(str(user_id)):
        return 0
    result = await db.users.delete_one({"_id": ObjectId(str(user_id))})
    return int(result.deleted_count or 0)


async def list_users(
    db: Any,
    query: Dict[str, Any],
    *,
    include_password_hash: bool = False,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    projection = None if include_password_hash else {"password_hash": 0}
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.users.find(query, projection).to_list(safe_limit)


async def list_audit_logs(
    db: Any,
    query: Dict[str, Any],
    *,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 100, 500)
    return await db.audit_logs.find(query).sort("created_at", -1).to_list(safe_limit)

