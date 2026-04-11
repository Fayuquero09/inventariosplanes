from typing import Any, Dict, List

from repositories.cleanup_repository import (
    count_documents,
    count_documents_by_or_filters,
    delete_documents,
    delete_documents_by_or_filters,
    list_ids,
)


def format_dependency_messages(dependency_counts: Dict[str, int]) -> List[str]:
    return [f"{count} {name}" for name, count in dependency_counts.items() if int(count or 0) > 0]


async def build_group_delete_context(db: Any, group_id: str) -> Dict[str, Any]:
    brand_ids = await list_ids(db, collection_name="brands", query={"group_id": group_id}, limit=10000)

    agency_filters: List[Dict[str, Any]] = [{"group_id": group_id}]
    if brand_ids:
        agency_filters.append({"brand_id": {"$in": brand_ids}})
    agency_ids = await list_ids(db, collection_name="agencies", query={"$or": agency_filters}, limit=10000)

    user_filters: List[Dict[str, Any]] = [{"group_id": group_id}]
    if brand_ids:
        user_filters.append({"brand_id": {"$in": brand_ids}})
    if agency_ids:
        user_filters.append({"agency_id": {"$in": agency_ids}})
    user_ids = await list_ids(db, collection_name="users", query={"$or": user_filters}, limit=10000)

    vehicle_filters: List[Dict[str, Any]] = [{"group_id": group_id}]
    if brand_ids:
        vehicle_filters.append({"brand_id": {"$in": brand_ids}})
    if agency_ids:
        vehicle_filters.append({"agency_id": {"$in": agency_ids}})
    vehicle_ids = await list_ids(db, collection_name="vehicles", query={"$or": vehicle_filters}, limit=10000)

    rates_filters: List[Dict[str, Any]] = [{"group_id": group_id}]
    if brand_ids:
        rates_filters.append({"brand_id": {"$in": brand_ids}})
    if agency_ids:
        rates_filters.append({"agency_id": {"$in": agency_ids}})

    objectives_filters: List[Dict[str, Any]] = [{"group_id": group_id}]
    if brand_ids:
        objectives_filters.append({"brand_id": {"$in": brand_ids}})
    if agency_ids:
        objectives_filters.append({"agency_id": {"$in": agency_ids}})
    if user_ids:
        objectives_filters.append({"seller_id": {"$in": user_ids}})

    sales_filters: List[Dict[str, Any]] = []
    if vehicle_ids:
        sales_filters.append({"vehicle_id": {"$in": vehicle_ids}})
    if user_ids:
        sales_filters.append({"seller_id": {"$in": user_ids}})
    if agency_ids:
        sales_filters.append({"agency_id": {"$in": agency_ids}})

    return {
        "group_id": group_id,
        "brand_ids": brand_ids,
        "agency_ids": agency_ids,
        "user_ids": user_ids,
        "vehicle_ids": vehicle_ids,
        "agency_filters": agency_filters,
        "user_filters": user_filters,
        "vehicle_filters": vehicle_filters,
        "rates_filters": rates_filters,
        "objectives_filters": objectives_filters,
        "sales_filters": sales_filters,
    }


async def summarize_group_dependencies(db: Any, context: Dict[str, Any]) -> Dict[str, int]:
    agency_ids = context.get("agency_ids", [])
    dependency_counts = {
        "marcas": len(context.get("brand_ids", [])),
        "agencias": len(agency_ids),
        "usuarios": len(context.get("user_ids", [])),
        "tasas financieras": await count_documents_by_or_filters(
            db,
            collection_name="financial_rates",
            filters=context.get("rates_filters", []),
        ),
        "objetivos": await count_documents_by_or_filters(
            db,
            collection_name="sales_objectives",
            filters=context.get("objectives_filters", []),
        ),
        "vehiculos": len(context.get("vehicle_ids", [])),
        "ventas": await count_documents_by_or_filters(
            db,
            collection_name="sales",
            filters=context.get("sales_filters", []),
        ) if context.get("sales_filters") else 0,
        "reglas de comision": await count_documents(
            db,
            collection_name="commission_rules",
            query={"agency_id": {"$in": agency_ids}},
        ) if agency_ids else 0,
    }
    return dependency_counts


async def execute_group_cascade_delete(db: Any, context: Dict[str, Any]) -> Dict[str, int]:
    deleted_counts = {
        "sales": 0,
        "commission_rules": 0,
        "sales_objectives": 0,
        "financial_rates": 0,
        "vehicles": 0,
        "users": 0,
        "agencies": 0,
        "brands": 0,
        "groups": 0,
    }

    sales_filters = context.get("sales_filters", [])
    agency_ids = context.get("agency_ids", [])
    objectives_filters = context.get("objectives_filters", [])
    rates_filters = context.get("rates_filters", [])
    vehicle_filters = context.get("vehicle_filters", [])
    user_filters = context.get("user_filters", [])
    agency_filters = context.get("agency_filters", [])

    if sales_filters:
        deleted_counts["sales"] = await delete_documents_by_or_filters(
            db,
            collection_name="sales",
            filters=sales_filters,
        )
    if agency_ids:
        deleted_counts["commission_rules"] = await delete_documents(
            db,
            collection_name="commission_rules",
            query={"agency_id": {"$in": agency_ids}},
        )
    deleted_counts["sales_objectives"] = await delete_documents_by_or_filters(
        db,
        collection_name="sales_objectives",
        filters=objectives_filters,
    )
    deleted_counts["financial_rates"] = await delete_documents_by_or_filters(
        db,
        collection_name="financial_rates",
        filters=rates_filters,
    )
    deleted_counts["vehicles"] = await delete_documents_by_or_filters(
        db,
        collection_name="vehicles",
        filters=vehicle_filters,
    )
    deleted_counts["users"] = await delete_documents_by_or_filters(
        db,
        collection_name="users",
        filters=user_filters,
    )
    deleted_counts["agencies"] = await delete_documents_by_or_filters(
        db,
        collection_name="agencies",
        filters=agency_filters,
    )
    deleted_counts["brands"] = await delete_documents(
        db,
        collection_name="brands",
        query={"group_id": context.get("group_id")},
    )

    return deleted_counts


async def build_brand_delete_context(db: Any, brand_id: str) -> Dict[str, Any]:
    agencies = await list_ids(db, collection_name="agencies", query={"brand_id": brand_id}, limit=10000)

    user_filters: List[Dict[str, Any]] = [{"brand_id": brand_id}]
    if agencies:
        user_filters.append({"agency_id": {"$in": agencies}})
    users = await list_ids(db, collection_name="users", query={"$or": user_filters}, limit=10000)

    vehicle_filters: List[Dict[str, Any]] = [{"brand_id": brand_id}]
    if agencies:
        vehicle_filters.append({"agency_id": {"$in": agencies}})
    vehicles = await list_ids(db, collection_name="vehicles", query={"$or": vehicle_filters}, limit=10000)

    rates_filters: List[Dict[str, Any]] = [{"brand_id": brand_id}]
    if agencies:
        rates_filters.append({"agency_id": {"$in": agencies}})

    objectives_filters: List[Dict[str, Any]] = [{"brand_id": brand_id}]
    if agencies:
        objectives_filters.append({"agency_id": {"$in": agencies}})
    if users:
        objectives_filters.append({"seller_id": {"$in": users}})

    sales_filters: List[Dict[str, Any]] = []
    if vehicles:
        sales_filters.append({"vehicle_id": {"$in": vehicles}})
    if users:
        sales_filters.append({"seller_id": {"$in": users}})
    if agencies:
        sales_filters.append({"agency_id": {"$in": agencies}})

    return {
        "brand_id": brand_id,
        "agency_ids": agencies,
        "user_ids": users,
        "vehicle_ids": vehicles,
        "user_filters": user_filters,
        "vehicle_filters": vehicle_filters,
        "rates_filters": rates_filters,
        "objectives_filters": objectives_filters,
        "sales_filters": sales_filters,
    }


async def summarize_brand_dependencies(db: Any, context: Dict[str, Any]) -> Dict[str, int]:
    agency_ids = context.get("agency_ids", [])
    dependency_counts = {
        "agencias": len(agency_ids),
        "usuarios": len(context.get("user_ids", [])),
        "tasas financieras": await count_documents_by_or_filters(
            db,
            collection_name="financial_rates",
            filters=context.get("rates_filters", []),
        ),
        "objetivos": await count_documents_by_or_filters(
            db,
            collection_name="sales_objectives",
            filters=context.get("objectives_filters", []),
        ),
        "vehiculos": len(context.get("vehicle_ids", [])),
        "ventas": await count_documents_by_or_filters(
            db,
            collection_name="sales",
            filters=context.get("sales_filters", []),
        ) if context.get("sales_filters") else 0,
        "reglas de comision": await count_documents(
            db,
            collection_name="commission_rules",
            query={"agency_id": {"$in": agency_ids}},
        ) if agency_ids else 0,
    }
    return dependency_counts


async def execute_brand_cascade_delete(db: Any, context: Dict[str, Any]) -> Dict[str, int]:
    deleted_counts = {
        "sales": 0,
        "commission_rules": 0,
        "sales_objectives": 0,
        "financial_rates": 0,
        "vehicles": 0,
        "users": 0,
        "agencies": 0,
        "brands": 0,
    }

    agency_ids = context.get("agency_ids", [])
    if context.get("sales_filters"):
        deleted_counts["sales"] = await delete_documents_by_or_filters(
            db,
            collection_name="sales",
            filters=context.get("sales_filters", []),
        )
    if agency_ids:
        deleted_counts["commission_rules"] = await delete_documents(
            db,
            collection_name="commission_rules",
            query={"agency_id": {"$in": agency_ids}},
        )
    deleted_counts["sales_objectives"] = await delete_documents_by_or_filters(
        db,
        collection_name="sales_objectives",
        filters=context.get("objectives_filters", []),
    )
    deleted_counts["financial_rates"] = await delete_documents_by_or_filters(
        db,
        collection_name="financial_rates",
        filters=context.get("rates_filters", []),
    )
    deleted_counts["vehicles"] = await delete_documents_by_or_filters(
        db,
        collection_name="vehicles",
        filters=context.get("vehicle_filters", []),
    )
    deleted_counts["users"] = await delete_documents_by_or_filters(
        db,
        collection_name="users",
        filters=context.get("user_filters", []),
    )
    deleted_counts["agencies"] = await delete_documents(
        db,
        collection_name="agencies",
        query={"brand_id": context.get("brand_id")},
    )

    return deleted_counts

