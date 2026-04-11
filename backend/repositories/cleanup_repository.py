from typing import Any, Dict, List


def _safe_limit(limit: int, default: int = 10000, max_limit: int = 50000) -> int:
    return max(1, min(int(limit or default), max_limit))


async def list_ids(
    db: Any,
    *,
    collection_name: str,
    query: Dict[str, Any],
    limit: int = 10000,
) -> List[str]:
    collection = getattr(db, collection_name)
    docs = await collection.find(query, {"_id": 1}).to_list(_safe_limit(limit))
    return [str(doc["_id"]) for doc in docs if doc.get("_id") is not None]


async def count_documents_by_or_filters(
    db: Any,
    *,
    collection_name: str,
    filters: List[Dict[str, Any]],
) -> int:
    if not filters:
        return 0
    collection = getattr(db, collection_name)
    return int(await collection.count_documents({"$or": filters}))


async def count_documents(
    db: Any,
    *,
    collection_name: str,
    query: Dict[str, Any],
) -> int:
    collection = getattr(db, collection_name)
    return int(await collection.count_documents(query))


async def delete_documents_by_or_filters(
    db: Any,
    *,
    collection_name: str,
    filters: List[Dict[str, Any]],
) -> int:
    if not filters:
        return 0
    collection = getattr(db, collection_name)
    result = await collection.delete_many({"$or": filters})
    return int(result.deleted_count or 0)


async def delete_documents(
    db: Any,
    *,
    collection_name: str,
    query: Dict[str, Any],
) -> int:
    collection = getattr(db, collection_name)
    result = await collection.delete_many(query)
    return int(result.deleted_count or 0)

