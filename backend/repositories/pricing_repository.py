from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId


def _as_object_id(value: Any) -> Optional[ObjectId]:
    text = str(value or "").strip()
    if not ObjectId.is_valid(text):
        return None
    return ObjectId(text)


async def list_price_bulletins_for_model(
    db: Any,
    *,
    query: Dict[str, Any],
    limit: int = 200,
) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 200), 2000))
    return await db.price_bulletins.find(query).sort(
        [
            ("effective_from", -1),
            ("updated_at", -1),
            ("created_at", -1),
        ]
    ).to_list(safe_limit)


async def list_price_bulletins(
    db: Any,
    *,
    query: Dict[str, Any],
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 5000), 10000))
    return await db.price_bulletins.find(query).sort(
        [
            ("effective_from", -1),
            ("updated_at", -1),
            ("created_at", -1),
        ]
    ).to_list(safe_limit)


async def find_group_by_id(db: Any, group_id: str) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(group_id)
    if object_id is None:
        return None
    return await db.groups.find_one({"_id": object_id})


async def find_brand_by_id(db: Any, brand_id: str) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(brand_id)
    if object_id is None:
        return None
    return await db.brands.find_one({"_id": object_id})


async def find_agency_by_id(db: Any, agency_id: str) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(agency_id)
    if object_id is None:
        return None
    return await db.agencies.find_one({"_id": object_id})


async def upsert_price_bulletin(
    db: Any,
    *,
    query: Dict[str, Any],
    set_fields: Dict[str, Any],
    created_at: datetime,
    created_by: Optional[str],
) -> None:
    await db.price_bulletins.update_one(
        query,
        {
            "$set": set_fields,
            "$setOnInsert": {
                "created_at": created_at,
                "created_by": created_by,
            },
        },
        upsert=True,
    )


async def find_price_bulletin_by_id(db: Any, bulletin_id: str) -> Optional[Dict[str, Any]]:
    object_id = _as_object_id(bulletin_id)
    if object_id is None:
        return None
    return await db.price_bulletins.find_one({"_id": object_id})


async def delete_price_bulletin_by_id(db: Any, bulletin_id: str) -> int:
    object_id = _as_object_id(bulletin_id)
    if object_id is None:
        return 0
    result = await db.price_bulletins.delete_one({"_id": object_id})
    return int(result.deleted_count or 0)


async def list_sales_for_repricing(
    db: Any,
    *,
    query: Dict[str, Any],
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 50000), 100000))
    return await db.sales.find(query).to_list(safe_limit)


async def list_vehicles_by_ids(
    db: Any,
    *,
    vehicle_ids: List[str],
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    object_ids = [ObjectId(vehicle_id) for vehicle_id in vehicle_ids if ObjectId.is_valid(str(vehicle_id or ""))]
    if not object_ids:
        return []
    safe_limit = max(1, min(int(limit or 50000), 100000))
    return await db.vehicles.find({"_id": {"$in": object_ids}}).to_list(safe_limit)


async def update_sale_fields(
    db: Any,
    *,
    sale_id: Any,
    set_fields: Dict[str, Any],
) -> int:
    object_id = _as_object_id(sale_id)
    if object_id is None or not set_fields:
        return 0
    result = await db.sales.update_one({"_id": object_id}, {"$set": set_fields})
    return int(result.modified_count or 0)
