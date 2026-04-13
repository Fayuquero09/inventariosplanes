from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional


async def enrich_vehicle(
    vehicle: Dict[str, Any],
    *,
    serialize_doc: Callable[[Dict[str, Any]], Dict[str, Any]],
    find_agency_by_id: Callable[[str], Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[[str], Awaitable[Optional[Dict[str, Any]]]],
    calculate_vehicle_financial_cost: Callable[[Dict[str, Any]], Awaitable[float]],
    find_latest_sale_for_vehicle: Callable[[str], Awaitable[Optional[Dict[str, Any]]]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    is_valid_object_id: Callable[[str], bool],
    find_user_by_id: Callable[[str], Awaitable[Optional[Dict[str, Any]]]],
) -> Dict[str, Any]:
    result = serialize_doc(vehicle)

    agency_id = vehicle.get("agency_id")
    if agency_id:
        agency = await find_agency_by_id(str(agency_id))
        if agency:
            result["agency_name"] = agency.get("name")
            result["brand_id"] = agency.get("brand_id")
            result["group_id"] = agency.get("group_id")

            brand_id = agency.get("brand_id")
            if brand_id:
                brand = await find_brand_by_id(str(brand_id))
                if brand:
                    result["brand_name"] = brand.get("name")

    entry_date = vehicle.get("entry_date")
    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    elif isinstance(entry_date, datetime) and entry_date.tzinfo is None:
        entry_date = entry_date.replace(tzinfo=timezone.utc)

    if vehicle.get("exit_date"):
        end_date = vehicle["exit_date"]
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        elif isinstance(end_date, datetime) and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)

    result["aging_days"] = (end_date - entry_date).days
    result["financial_cost"] = await calculate_vehicle_financial_cost(vehicle)

    vehicle_id = result.get("id")
    if vehicle_id:
        sale_doc = await find_latest_sale_for_vehicle(vehicle_id)
        if sale_doc:
            serialized_sale = serialize_doc(sale_doc)
            result["sale_commission"] = round(float(serialized_sale.get("commission", 0) or 0), 2)
            result["sale_price"] = round(float(serialized_sale.get("sale_price", 0) or 0), 2)
            result["effective_revenue"] = round(sale_effective_revenue(serialized_sale), 2)
            result["sale_date"] = serialized_sale.get("sale_date")
            seller_id = serialized_sale.get("seller_id")
            if seller_id and is_valid_object_id(str(seller_id)):
                seller = await find_user_by_id(str(seller_id))
                if seller:
                    result["sold_by_name"] = seller.get("name")

    return result
