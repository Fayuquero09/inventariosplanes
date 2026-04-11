from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId


def _safe_limit(limit: int, default: int, max_limit: int) -> int:
    return max(1, min(int(limit or default), max_limit))


async def list_active_rules_by_agency(
    db: Any,
    *,
    agency_id: str,
    approved_status: str,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.commission_rules.find(
        {
            "agency_id": agency_id,
            "$or": [
                {"approval_status": {"$exists": False}},
                {"approval_status": approved_status},
            ],
        }
    ).to_list(safe_limit)


async def find_commission_matrix_by_agency(db: Any, *, agency_id: str) -> Optional[Dict[str, Any]]:
    return await db.commission_matrices.find_one({"agency_id": agency_id})


async def upsert_commission_matrix_by_agency(
    db: Any,
    *,
    agency_id: str,
    set_fields: Dict[str, Any],
    set_on_insert: Dict[str, Any],
) -> None:
    await db.commission_matrices.update_one(
        {"agency_id": agency_id},
        {"$set": set_fields, "$setOnInsert": set_on_insert},
        upsert=True,
    )


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


async def find_user_by_id(db: Any, user_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(user_id)):
        return None
    return await db.users.find_one({"_id": ObjectId(str(user_id))})


async def insert_commission_rule(db: Any, rule_doc: Dict[str, Any]) -> str:
    result = await db.commission_rules.insert_one(rule_doc)
    return str(result.inserted_id)


async def find_commission_rule_by_id(db: Any, rule_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(rule_id)):
        return None
    return await db.commission_rules.find_one({"_id": ObjectId(str(rule_id))})


async def list_commission_rules(db: Any, *, query: Dict[str, Any], limit: int = 1000) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.commission_rules.find(query).to_list(safe_limit)


async def update_commission_rule_by_id(db: Any, *, rule_id: str, set_fields: Dict[str, Any]) -> None:
    await db.commission_rules.update_one(
        {"_id": ObjectId(str(rule_id))},
        {"$set": set_fields},
    )


async def delete_commission_rule_by_id(db: Any, *, rule_id: str) -> None:
    await db.commission_rules.delete_one({"_id": ObjectId(str(rule_id))})


async def list_sales_for_closure(
    db: Any,
    *,
    agency_id: str,
    seller_id: str,
    start_date: datetime,
    end_date: datetime,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 10000, 100000)
    return await db.sales.find(
        {
            "agency_id": agency_id,
            "seller_id": seller_id,
            "sale_date": {"$gte": start_date, "$lt": end_date},
        }
    ).to_list(safe_limit)


async def find_commission_closure_by_scope(
    db: Any,
    *,
    seller_id: str,
    agency_id: str,
    month: int,
    year: int,
) -> Optional[Dict[str, Any]]:
    return await db.commission_closures.find_one(
        {
            "seller_id": seller_id,
            "agency_id": agency_id,
            "month": month,
            "year": year,
        }
    )


async def insert_commission_closure(db: Any, closure_doc: Dict[str, Any]) -> str:
    result = await db.commission_closures.insert_one(closure_doc)
    return str(result.inserted_id)


async def find_commission_closure_by_id(db: Any, closure_id: str) -> Optional[Dict[str, Any]]:
    if not ObjectId.is_valid(str(closure_id)):
        return None
    return await db.commission_closures.find_one({"_id": ObjectId(str(closure_id))})


async def update_commission_closure_by_id(db: Any, *, closure_id: str, set_fields: Dict[str, Any]) -> None:
    await db.commission_closures.update_one(
        {"_id": ObjectId(str(closure_id))},
        {"$set": set_fields},
    )


async def list_commission_closures(
    db: Any,
    *,
    query: Dict[str, Any],
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    safe_limit = _safe_limit(limit, 1000, 5000)
    return await db.commission_closures.find(query).to_list(safe_limit)


async def count_seller_sales_since(
    db: Any,
    *,
    seller_id: str,
    agency_id: str,
    since: datetime,
) -> int:
    return int(
        await db.sales.count_documents(
            {
                "seller_id": seller_id,
                "agency_id": agency_id,
                "sale_date": {"$gte": since},
            }
        )
    )
