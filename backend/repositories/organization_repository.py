from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def find_group_by_id(db: Any, group_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(group_id)):
        return None
    return await db.groups.find_one({"_id": ObjectId(str(group_id))})


async def insert_group(db: Any, group_doc: Dict[str, Any]) -> str:
    result = await db.groups.insert_one(group_doc)
    return str(result.inserted_id)


async def list_groups(db: Any, query: Dict[str, Any], *, limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.groups.find(query).to_list(safe_limit)


async def update_group_by_id(db: Any, group_id: str, update_fields: Dict[str, Any]) -> int:
    if not ObjectId.is_valid(str(group_id)):
        return 0
    result = await db.groups.update_one({"_id": ObjectId(str(group_id))}, {"$set": update_fields})
    return int(result.modified_count or 0)


async def delete_group_by_id(db: Any, group_id: str) -> int:
    if not ObjectId.is_valid(str(group_id)):
        return 0
    result = await db.groups.delete_one({"_id": ObjectId(str(group_id))})
    return int(result.deleted_count or 0)


async def find_brand_by_id(db: Any, brand_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(brand_id)):
        return None
    return await db.brands.find_one({"_id": ObjectId(str(brand_id))})


async def insert_brand(db: Any, brand_doc: Dict[str, Any]) -> str:
    result = await db.brands.insert_one(brand_doc)
    return str(result.inserted_id)


async def list_brands(db: Any, query: Dict[str, Any], *, limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.brands.find(query).to_list(safe_limit)


async def list_brands_by_ids(db: Any, brand_ids: List[str], *, limit: int = 1000) -> List[Dict[str, Any]]:
    valid_ids = [ObjectId(str(brand_id)) for brand_id in brand_ids if ObjectId.is_valid(str(brand_id))]
    if not valid_ids:
        return []
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.brands.find({"_id": {"$in": valid_ids}}).to_list(safe_limit)


async def update_brand_by_id(db: Any, brand_id: str, update_fields: Dict[str, Any]) -> int:
    if not ObjectId.is_valid(str(brand_id)):
        return 0
    result = await db.brands.update_one({"_id": ObjectId(str(brand_id))}, {"$set": update_fields})
    return int(result.modified_count or 0)


async def delete_brand_by_id(db: Any, brand_id: str) -> int:
    if not ObjectId.is_valid(str(brand_id)):
        return 0
    result = await db.brands.delete_one({"_id": ObjectId(str(brand_id))})
    return int(result.deleted_count or 0)


async def find_agency_by_id(db: Any, agency_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(agency_id)):
        return None
    return await db.agencies.find_one({"_id": ObjectId(str(agency_id))})


async def insert_agency(db: Any, agency_doc: Dict[str, Any]) -> str:
    result = await db.agencies.insert_one(agency_doc)
    return str(result.inserted_id)


async def list_agencies(db: Any, query: Dict[str, Any], *, limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 10000)
    return await db.agencies.find(query).to_list(safe_limit)


async def update_agency_by_id(db: Any, agency_id: str, update_fields: Dict[str, Any]) -> int:
    if not ObjectId.is_valid(str(agency_id)):
        return 0
    result = await db.agencies.update_one({"_id": ObjectId(str(agency_id))}, {"$set": update_fields})
    return int(result.modified_count or 0)

