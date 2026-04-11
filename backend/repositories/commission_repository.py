from datetime import datetime
from typing import Any, Dict, List, Optional


async def list_active_rules_by_agency(
    db: Any,
    *,
    agency_id: str,
    approved_status: str,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit or 1000), 5000))
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
