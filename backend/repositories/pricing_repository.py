from typing import Any, Dict, List


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
