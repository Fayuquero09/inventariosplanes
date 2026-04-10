from typing import Any, Dict, List


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

