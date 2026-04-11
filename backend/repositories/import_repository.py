from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int = 10000, max_limit: int = 50000) -> int:
    return max(1, min(int(limit or default), max_limit))


def _as_object_id(value: Any) -> Optional[ObjectId]:
    text = str(value or "").strip()
    if not ObjectId.is_valid(text):
        return None
    return ObjectId(text)


async def list_groups(db: Any, *, limit: int = 10000) -> List[Dict[str, Any]]:
    return await db.groups.find({}).to_list(_safe_limit(limit))


async def list_brands(db: Any, *, limit: int = 10000) -> List[Dict[str, Any]]:
    return await db.brands.find({}).to_list(_safe_limit(limit))


async def list_agencies(db: Any, *, limit: int = 10000) -> List[Dict[str, Any]]:
    return await db.agencies.find({}).to_list(_safe_limit(limit))


async def list_users_email_role(db: Any, *, limit: int = 10000) -> List[Dict[str, Any]]:
    return await db.users.find({}, {"email": 1, "role": 1}).to_list(_safe_limit(limit))


async def update_group_fields(db: Any, group_id: Any, fields: Dict[str, Any]) -> int:
    if not fields:
        return 0
    object_id = _as_object_id(group_id)
    if object_id is None:
        return 0
    result = await db.groups.update_one({"_id": object_id}, {"$set": fields})
    return int(result.modified_count or 0)


async def insert_group(db: Any, group_doc: Dict[str, Any]) -> str:
    result = await db.groups.insert_one(group_doc)
    return str(result.inserted_id)


async def update_brand_fields(db: Any, brand_id: Any, fields: Dict[str, Any]) -> int:
    if not fields:
        return 0
    object_id = _as_object_id(brand_id)
    if object_id is None:
        return 0
    result = await db.brands.update_one({"_id": object_id}, {"$set": fields})
    return int(result.modified_count or 0)


async def insert_brand(db: Any, brand_doc: Dict[str, Any]) -> str:
    result = await db.brands.insert_one(brand_doc)
    return str(result.inserted_id)


async def update_agency_fields(db: Any, agency_id: Any, fields: Dict[str, Any]) -> int:
    if not fields:
        return 0
    object_id = _as_object_id(agency_id)
    if object_id is None:
        return 0
    result = await db.agencies.update_one({"_id": object_id}, {"$set": fields})
    return int(result.modified_count or 0)


async def insert_agency(db: Any, agency_doc: Dict[str, Any]) -> str:
    result = await db.agencies.insert_one(agency_doc)
    return str(result.inserted_id)


async def update_user_fields(db: Any, user_id: Any, fields: Dict[str, Any]) -> int:
    if not fields:
        return 0
    object_id = _as_object_id(user_id)
    if object_id is None:
        return 0
    result = await db.users.update_one({"_id": object_id}, {"$set": fields})
    return int(result.modified_count or 0)


async def insert_user(db: Any, user_doc: Dict[str, Any]) -> str:
    result = await db.users.insert_one(user_doc)
    return str(result.inserted_id)


async def find_agency_by_id(db: Any, agency_id: Any) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(agency_id)
    if object_id is None:
        return None
    return await db.agencies.find_one({"_id": object_id})


async def insert_vehicle(db: Any, vehicle_doc: Dict[str, Any]) -> str:
    result = await db.vehicles.insert_one(vehicle_doc)
    return str(result.inserted_id)


async def find_vehicle_by_id(db: Any, vehicle_id: Any) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(vehicle_id)
    if object_id is None:
        return None
    return await db.vehicles.find_one({"_id": object_id})


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
