from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def list_sales_objectives(db: Any, *, query: Dict[str, Any], limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.sales_objectives.find(query).to_list(safe_limit)


async def find_user_by_id(db: Any, user_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(user_id)):
        return None
    return await db.users.find_one({"_id": ObjectId(str(user_id))})


async def find_agency_by_id(db: Any, agency_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(agency_id)):
        return None
    return await db.agencies.find_one({"_id": ObjectId(str(agency_id))})


async def find_brand_by_id(db: Any, brand_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(brand_id)):
        return None
    return await db.brands.find_one({"_id": ObjectId(str(brand_id))})


async def find_group_by_id(db: Any, group_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(group_id)):
        return None
    return await db.groups.find_one({"_id": ObjectId(str(group_id))})


async def list_sales(db: Any, *, query: Dict[str, Any], limit: int = 20000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 20000, 100000)
    return await db.sales.find(query).to_list(safe_limit)


async def list_price_bulletins(db: Any, *, query: Dict[str, Any], limit: int = 3000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 3000, 10000)
    return await db.price_bulletins.find(query).sort(
        [
            ("effective_from", -1),
            ("updated_at", -1),
            ("created_at", -1),
        ]
    ).to_list(safe_limit)
