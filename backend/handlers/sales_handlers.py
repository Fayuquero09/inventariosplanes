from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class SalesHandlerBundle:
    create_sale: Callable[[Any, Request], Awaitable[Any]]
    get_sales: Callable[..., Awaitable[Any]]


def build_sales_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    sale_creator_roles: Sequence[str],
    seller_role: str,
    find_sales_vehicle_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    ensure_doc_scope_access: Callable[..., None],
    find_user_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    create_sale_record: Callable[..., Awaitable[Dict[str, Any]]],
    calculate_commission: Callable[..., Awaitable[float]],
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override: Callable[..., Dict[str, Any]],
    extract_active_aging_incentive_plan: Callable[..., Optional[Dict[str, Any]]],
    apply_aging_plan_to_effective_pricing: Callable[..., Any],
    to_non_negative_float: Callable[[Any, float], float],
    log_audit_event: Callable[..., Awaitable[None]],
    serialize_doc: Callable[[Any], Any],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    list_sales_with_enrichment: Callable[..., Awaitable[Any]],
) -> SalesHandlerBundle:
    async def create_sale(sale_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in sale_creator_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        vehicle = await find_sales_vehicle_by_id(db, sale_data.vehicle_id)
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")

        if current_user["role"] == seller_role and sale_data.seller_id != current_user.get("id"):
            raise HTTPException(status_code=403, detail="Seller can only register own sales")
        if sale_data.seller_id:
            target_seller = await find_user_by_id(db, sale_data.seller_id)
            if not target_seller:
                raise HTTPException(status_code=404, detail="Seller not found")
            ensure_doc_scope_access(
                current_user,
                target_seller,
                detail="No tienes acceso a este vendedor",
            )
        creation_result = await create_sale_record(
            db,
            sale_data=sale_data.model_dump(),
            vehicle=vehicle,
            calculate_commission=calculate_commission,
            resolve_effective_sale_pricing_for_model=resolve_effective_sale_pricing_for_model,
            apply_manual_sale_price_override=apply_manual_sale_price_override,
            extract_active_aging_incentive_plan=extract_active_aging_incentive_plan,
            apply_aging_plan_to_effective_pricing=apply_aging_plan_to_effective_pricing,
            to_non_negative_float=to_non_negative_float,
        )
        sale_doc = creation_result["sale_doc"]
        sale_id = creation_result["sale_id"]
        effective_pricing = creation_result["effective_pricing"]
        applied_aging = creation_result["applied_aging"]
        resolved_sale_price = creation_result["resolved_sale_price"]
        base_commission = creation_result["base_commission"]
        commission = sale_doc.get("commission", 0)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_sale",
            entity_type="sale",
            entity_id=str(sale_id),
            group_id=sale_doc.get("group_id"),
            brand_id=sale_doc.get("brand_id"),
            agency_id=sale_doc.get("agency_id"),
            details={
                "vehicle_id": sale_data.vehicle_id,
                "seller_id": sale_data.seller_id,
                "sale_price": resolved_sale_price,
                "commission_base_price": round(to_non_negative_float(effective_pricing.get("commission_base_price"), resolved_sale_price), 2),
                "effective_revenue": round(to_non_negative_float(effective_pricing.get("effective_revenue"), resolved_sale_price), 2),
                "brand_incentive_amount": round(to_non_negative_float(effective_pricing.get("brand_incentive_amount"), 0.0), 2),
                "dealer_incentive_amount": round(to_non_negative_float(effective_pricing.get("dealer_incentive_amount"), 0.0), 2),
                "undocumented_dealer_incentive_amount": round(to_non_negative_float(effective_pricing.get("undocumented_dealer_incentive_amount"), 0.0), 2),
                "aging_incentive_sale_discount_amount": round(to_non_negative_float(applied_aging.get("sale_discount_amount"), 0.0), 2),
                "aging_incentive_seller_bonus_amount": round(to_non_negative_float(applied_aging.get("seller_bonus_amount"), 0.0), 2),
                "aging_incentive_total_amount": round(to_non_negative_float(applied_aging.get("total_amount"), 0.0), 2),
                "fi_revenue": sale_data.fi_revenue,
                "plant_incentive": sale_data.plant_incentive,
                "base_commission": base_commission,
                "commission": commission,
            },
        )

        sale_doc["id"] = str(sale_id)
        return serialize_doc(sale_doc)

    async def get_sales(
        request: Request,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id
        if current_user["role"] == seller_role and current_user.get("id"):
            query["seller_id"] = current_user["id"]

        if current_user["role"] == seller_role:
            current_seller_id = current_user.get("id")
            if not current_seller_id:
                return []
            if seller_id and seller_id != current_seller_id:
                raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
            query["seller_id"] = current_seller_id
        elif seller_id:
            query["seller_id"] = seller_id
        if month and year:
            start_date = datetime(year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
            query["sale_date"] = {"$gte": start_date, "$lt": end_date}

        return await list_sales_with_enrichment(
            db,
            query=query,
            serialize_doc=serialize_doc,
            limit=1000,
        )

    return SalesHandlerBundle(
        create_sale=create_sale,
        get_sales=get_sales,
    )
