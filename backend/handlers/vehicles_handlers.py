from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class VehiclesHandlerBundle:
    create_vehicle: Callable[[Any, Request], Awaitable[Any]]
    get_vehicles: Callable[..., Awaitable[Any]]
    get_vehicle: Callable[[str, Request], Awaitable[Any]]
    apply_vehicle_aging_incentive: Callable[[str, Any, Request], Awaitable[Any]]
    update_vehicle: Callable[[str, Request], Awaitable[Any]]


def build_vehicles_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    vehicle_editor_roles: Sequence[str],
    vehicle_aging_incentive_roles: Sequence[str],
    ensure_allowed_model_year: Callable[[int], None],
    ensure_doc_scope_access: Callable[..., None],
    log_audit_event: Callable[..., Awaitable[None]],
    enrich_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    object_id_cls: Any,
    build_vehicle_aging_suggestion: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    to_non_negative_float: Callable[[Any, float], float],
) -> VehiclesHandlerBundle:
    async def create_vehicle(vehicle_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in vehicle_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        ensure_allowed_model_year(vehicle_data.year)

        agency = await db.agencies.find_one({"_id": object_id_cls(vehicle_data.agency_id)})
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        entry_date = vehicle_data.entry_date
        if entry_date:
            entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
        else:
            entry_date = datetime.now(timezone.utc)

        vehicle_doc = {
            "vin": vehicle_data.vin,
            "model": vehicle_data.model,
            "year": vehicle_data.year,
            "trim": vehicle_data.trim,
            "color": vehicle_data.color,
            "vehicle_type": vehicle_data.vehicle_type,
            "purchase_price": vehicle_data.purchase_price,
            "agency_id": vehicle_data.agency_id,
            "brand_id": agency.get("brand_id"),
            "group_id": agency.get("group_id"),
            "entry_date": entry_date,
            "exit_date": None,
            "status": "in_stock",
            "created_at": datetime.now(timezone.utc),
        }
        result = await db.vehicles.insert_one(vehicle_doc)
        vehicle_doc["_id"] = result.inserted_id

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_vehicle",
            entity_type="vehicle",
            entity_id=str(result.inserted_id),
            group_id=vehicle_doc.get("group_id"),
            brand_id=vehicle_doc.get("brand_id"),
            agency_id=vehicle_doc.get("agency_id"),
            details={
                "vin": vehicle_data.vin,
                "model": vehicle_data.model,
                "year": vehicle_data.year,
                "trim": vehicle_data.trim,
                "color": vehicle_data.color,
                "vehicle_type": vehicle_data.vehicle_type,
                "purchase_price": vehicle_data.purchase_price,
            },
        )
        return await enrich_vehicle(vehicle_doc)

    async def get_vehicles(
        request: Request,
        agency_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        group_id: Optional[str] = None,
        status: Optional[str] = None,
        vehicle_type: Optional[str] = None,
        sold_current_month_only: bool = False,
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id
        if brand_id:
            query["brand_id"] = brand_id
        if group_id:
            query["group_id"] = group_id

        if status:
            query["status"] = status
        if vehicle_type:
            query["vehicle_type"] = vehicle_type

        if sold_current_month_only:
            now = datetime.now(timezone.utc)
            start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            if now.month == 12:
                end_of_month = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end_of_month = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)

            sold_month_clause = {
                "status": "sold",
                "exit_date": {"$gte": start_of_month, "$lt": end_of_month},
            }

            if status == "sold":
                query["exit_date"] = {"$gte": start_of_month, "$lt": end_of_month}
            elif not status:
                query = {
                    "$and": [
                        query,
                        {
                            "$or": [
                                {"status": {"$ne": "sold"}},
                                sold_month_clause,
                            ]
                        },
                    ]
                }

        vehicles = await db.vehicles.find(query).to_list(1000)
        return [await enrich_vehicle(v) for v in vehicles]

    async def get_vehicle(vehicle_id: str, request: Request):
        current_user = await get_current_user(request)
        vehicle = await db.vehicles.find_one({"_id": object_id_cls(vehicle_id)})
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")
        return await enrich_vehicle(vehicle)

    async def apply_vehicle_aging_incentive(
        vehicle_id: str,
        payload: Any,
        request: Request,
    ):
        current_user = await get_current_user(request)
        if current_user.get("role") not in vehicle_aging_incentive_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        vehicle = await db.vehicles.find_one({"_id": object_id_cls(vehicle_id)})
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        ensure_doc_scope_access(current_user, vehicle, detail="No tienes acceso a este vehículo")
        if vehicle.get("status") != "in_stock":
            raise HTTPException(status_code=400, detail="Solo se puede configurar incentivo aging para vehículos en stock")

        enriched_vehicle = await enrich_vehicle(vehicle)
        suggestion = await build_vehicle_aging_suggestion(vehicle, enriched_vehicle=enriched_vehicle)
        if not suggestion:
            raise HTTPException(status_code=400, detail="No hay incentivo sugerido para este vehículo en este momento")

        sale_discount_amount = round(to_non_negative_float(payload.sale_discount_amount, 0.0), 2)
        seller_bonus_amount = round(to_non_negative_float(payload.seller_bonus_amount, 0.0), 2)
        total_amount = round(sale_discount_amount + seller_bonus_amount, 2)
        if total_amount <= 0:
            raise HTTPException(status_code=400, detail="Debes capturar un monto mayor a cero para venta o vendedor")

        suggested_amount = round(to_non_negative_float(suggestion.get("suggested_bonus"), 0.0), 2)
        if total_amount - suggested_amount > 0.01:
            raise HTTPException(
                status_code=400,
                detail=f"El total aplicado ({total_amount}) no puede ser mayor al sugerido ({suggested_amount})",
            )

        plan_doc = {
            "active": True,
            "suggested_amount": suggested_amount,
            "sale_discount_amount": sale_discount_amount,
            "seller_bonus_amount": seller_bonus_amount,
            "total_amount": total_amount,
            "avg_days_to_sell": int(suggestion.get("avg_days_to_sell") or 0),
            "current_aging": int(suggestion.get("current_aging") or 0),
            "reason": suggestion.get("reason"),
            "notes": (payload.notes or "").strip() or None,
            "configured_by": current_user.get("id"),
            "configured_by_name": current_user.get("name"),
            "configured_at": datetime.now(timezone.utc),
            "applied_sale_id": None,
            "applied_at": None,
            "applied_sale_discount_amount": 0.0,
            "applied_seller_bonus_amount": 0.0,
        }

        await db.vehicles.update_one({"_id": object_id_cls(vehicle_id)}, {"$set": {"aging_incentive_plan": plan_doc}})
        updated_vehicle = await db.vehicles.find_one({"_id": object_id_cls(vehicle_id)})

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="apply_vehicle_aging_incentive",
            entity_type="vehicle",
            entity_id=vehicle_id,
            group_id=vehicle.get("group_id"),
            brand_id=vehicle.get("brand_id"),
            agency_id=vehicle.get("agency_id"),
            details={
                "vin": vehicle.get("vin"),
                "model": vehicle.get("model"),
                "trim": vehicle.get("trim"),
                "suggested_amount": suggested_amount,
                "sale_discount_amount": sale_discount_amount,
                "seller_bonus_amount": seller_bonus_amount,
                "total_amount": total_amount,
                "notes": plan_doc.get("notes"),
            },
        )

        return await enrich_vehicle(updated_vehicle)

    async def update_vehicle(vehicle_id: str, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in vehicle_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        data = await request.json()
        update_data = {k: v for k, v in data.items() if k not in ["id", "_id"]}

        previous = await db.vehicles.find_one({"_id": object_id_cls(vehicle_id)})
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este vehículo")

        if update_data.get("agency_id"):
            target_agency = await db.agencies.find_one({"_id": object_id_cls(update_data["agency_id"])})
            if not target_agency:
                raise HTTPException(status_code=404, detail="Agency not found")
            ensure_doc_scope_access(
                current_user,
                target_agency,
                agency_field="_id",
                detail="No puedes mover este vehículo a otra agencia",
            )
            update_data["group_id"] = target_agency.get("group_id")
            update_data["brand_id"] = target_agency.get("brand_id")

        await db.vehicles.update_one({"_id": object_id_cls(vehicle_id)}, {"$set": update_data})
        vehicle = await db.vehicles.find_one({"_id": object_id_cls(vehicle_id)})

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_vehicle",
            entity_type="vehicle",
            entity_id=vehicle_id,
            group_id=vehicle.get("group_id") if vehicle else previous.get("group_id") if previous else None,
            brand_id=vehicle.get("brand_id") if vehicle else previous.get("brand_id") if previous else None,
            agency_id=vehicle.get("agency_id") if vehicle else previous.get("agency_id") if previous else None,
            details={
                "changes": update_data,
                "vin": vehicle.get("vin") if vehicle else previous.get("vin") if previous else None,
                "status": vehicle.get("status") if vehicle else previous.get("status") if previous else None,
            },
        )
        return await enrich_vehicle(vehicle)

    return VehiclesHandlerBundle(
        create_vehicle=create_vehicle,
        get_vehicles=get_vehicles,
        get_vehicle=get_vehicle,
        apply_vehicle_aging_incentive=apply_vehicle_aging_incentive,
        update_vehicle=update_vehicle,
    )
