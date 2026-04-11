from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId


async def find_brand_group_id(db: Any, brand_id: str) -> Optional[str]:
    if not ObjectId.is_valid(str(brand_id)):
        return None
    brand = await db.brands.find_one({"_id": ObjectId(str(brand_id))})
    if brand and brand.get("group_id"):
        return str(brand.get("group_id"))
    return None


async def find_agency_group_id(db: Any, agency_id: str) -> Optional[str]:
    if not ObjectId.is_valid(str(agency_id)):
        return None
    agency = await db.agencies.find_one({"_id": ObjectId(str(agency_id))})
    if agency and agency.get("group_id"):
        return str(agency.get("group_id"))
    return None


async def find_monthly_close(
    db: Any,
    *,
    year: int,
    month: int,
    group_id: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if group_id:
        group_doc = await db.dashboard_monthly_closes.find_one(
            {
                "year": int(year),
                "month": int(month),
                "group_id": str(group_id),
            }
        )
        if group_doc:
            return group_doc, "group"

    global_doc = await db.dashboard_monthly_closes.find_one(
        {
            "year": int(year),
            "month": int(month),
            "group_id": None,
        }
    )
    if global_doc:
        return global_doc, "global"

    return None, "none"


async def list_global_monthly_closes_by_year(db: Any, *, year: int, limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 1000), 5000))
    docs = await db.dashboard_monthly_closes.find(
        {
            "year": int(year),
            "group_id": None,
        }
    ).to_list(safe_limit)
    return docs


async def upsert_global_monthly_close(
    db: Any,
    *,
    year: int,
    month: int,
    fiscal_close_day: Optional[int],
    industry_close_day: Optional[int],
    industry_close_month_offset: int,
    updated_by: Optional[str],
    now: datetime,
) -> Optional[Dict[str, Any]]:
    query = {
        "year": int(year),
        "month": int(month),
        "group_id": None,
    }
    update_doc = {
        "fiscal_close_day": fiscal_close_day,
        "industry_close_day": industry_close_day,
        "industry_close_month_offset": int(industry_close_month_offset or 0),
        "updated_at": now,
        "updated_by": updated_by,
    }
    await db.dashboard_monthly_closes.update_one(
        query,
        {
            "$set": update_doc,
            "$setOnInsert": {
                "created_at": now,
            },
        },
        upsert=True,
    )
    return await db.dashboard_monthly_closes.find_one(query)


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def list_vehicles(db: Any, *, query: Dict[str, Any], limit: int = 10000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 10000, 50000)
    return await db.vehicles.find(query).to_list(safe_limit)


async def list_sales(db: Any, *, query: Dict[str, Any], limit: int = 10000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 10000, 50000)
    return await db.sales.find(query).to_list(safe_limit)


async def count_sales(db: Any, *, query: Dict[str, Any]) -> int:
    return int(await db.sales.count_documents(query))


async def list_vehicles_by_ids(db: Any, *, vehicle_ids: List[str], limit: int = 10000) -> List[Dict[str, Any]]:
    object_ids = [ObjectId(str(vehicle_id)) for vehicle_id in vehicle_ids if ObjectId.is_valid(str(vehicle_id))]
    if not object_ids:
        return []
    safe_limit = _safe_limit(limit, 10000, 50000)
    return await db.vehicles.find({"_id": {"$in": object_ids}}).to_list(safe_limit)


async def list_agencies_by_brand_id(db: Any, *, brand_id: str, limit: int = 5000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 5000, 20000)
    return await db.agencies.find({"brand_id": brand_id}).to_list(safe_limit)


async def list_agencies_by_group_id(db: Any, *, group_id: str, limit: int = 10000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 10000, 50000)
    return await db.agencies.find({"group_id": group_id}).to_list(safe_limit)


async def count_users(db: Any, *, query: Dict[str, Any]) -> int:
    return int(await db.users.count_documents(query))


async def find_user_by_id(db: Any, user_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(user_id)):
        return None
    return await db.users.find_one({"_id": ObjectId(str(user_id))})


async def list_similar_sold_vehicles(
    db: Any,
    *,
    model: Any,
    trim: Any,
    color: Any,
    group_id: Any,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 100, 1000)
    query = {
        "model": model,
        "trim": trim,
        "color": color,
        "status": "sold",
        "group_id": group_id,
    }
    return await db.vehicles.find(query).to_list(safe_limit)
