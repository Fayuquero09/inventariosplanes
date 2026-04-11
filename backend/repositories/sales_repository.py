from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int = 1000, max_limit: int = 10000) -> int:
    return max(1, min(int(limit or default), max_limit))


def _as_object_id(value: Any) -> Optional[ObjectId]:
    text = str(value or "").strip()
    if not ObjectId.is_valid(text):
        return None
    return ObjectId(text)


async def find_vehicle_by_id(db: Any, vehicle_id: Any) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(vehicle_id)
    if object_id is None:
        return None
    return await db.vehicles.find_one({"_id": object_id})


async def find_user_by_id(db: Any, user_id: Any) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(user_id)
    if object_id is None:
        return None
    return await db.users.find_one({"_id": object_id})


async def insert_sale(db: Any, sale_doc: Dict[str, Any]) -> str:
    result = await db.sales.insert_one(sale_doc)
    return str(result.inserted_id)


async def update_vehicle_fields(db: Any, vehicle_id: Any, fields: Dict[str, Any]) -> int:
    if not fields:
        return 0
    object_id = _as_object_id(vehicle_id)
    if object_id is None:
        return 0
    result = await db.vehicles.update_one({"_id": object_id}, {"$set": fields})
    return int(result.modified_count or 0)


async def list_sales(db: Any, query: Dict[str, Any], *, limit: int = 1000) -> List[Dict[str, Any]]:
    return await db.sales.find(query).to_list(_safe_limit(limit))
