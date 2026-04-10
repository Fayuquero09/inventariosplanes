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

