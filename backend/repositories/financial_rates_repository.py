from typing import Any, Dict, List, Optional

from bson import ObjectId


def _as_object_id(value: Any) -> Optional[ObjectId]:
    if not ObjectId.is_valid(str(value or "")):
        return None
    return ObjectId(str(value))


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def find_financial_rate_by_id(db: Any, rate_id: str) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(rate_id)
    if object_id is None:
        return None
    return await db.financial_rates.find_one({"_id": object_id})


async def find_latest_financial_rate(
    db: Any,
    *,
    group_id: Any,
    brand_id: Any = None,
    agency_id: Any = None,
) -> Optional[Dict[str, Any]]:
    return await db.financial_rates.find_one(
        {
            "group_id": group_id,
            "brand_id": brand_id,
            "agency_id": agency_id,
        },
        sort=[("created_at", -1)],
    )


async def list_financial_rates(db: Any, *, query: Dict[str, Any], limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.financial_rates.find(query).to_list(safe_limit)


async def insert_financial_rate(db: Any, rate_doc: Dict[str, Any]) -> str:
    result = await db.financial_rates.insert_one(rate_doc)
    return str(result.inserted_id)


async def update_financial_rate_by_id(db: Any, *, rate_id: str, set_fields: Dict[str, Any]) -> int:
    object_id = _as_object_id(rate_id)
    if object_id is None:
        return 0
    result = await db.financial_rates.update_one({"_id": object_id}, {"$set": set_fields})
    return int(result.modified_count or 0)


async def delete_financial_rate_by_id(db: Any, *, rate_id: str) -> int:
    object_id = _as_object_id(rate_id)
    if object_id is None:
        return 0
    result = await db.financial_rates.delete_one({"_id": object_id})
    return int(result.deleted_count or 0)


async def list_brands_for_group(db: Any, *, group_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.brands.find({"group_id": group_id}).to_list(safe_limit)


async def list_brand_financial_rates_for_group(db: Any, *, group_id: str, limit: int = 5000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 5000, 10000)
    return await db.financial_rates.find(
        {"group_id": group_id, "agency_id": None, "brand_id": {"$ne": None}}
    ).to_list(safe_limit)


async def insert_many_financial_rates(db: Any, docs: List[Dict[str, Any]]) -> int:
    if not docs:
        return 0
    result = await db.financial_rates.insert_many(docs)
    return len(result.inserted_ids or [])
