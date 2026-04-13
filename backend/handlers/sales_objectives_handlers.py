from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class SalesObjectivesHandlerBundle:
    create_sales_objective: Callable[[Any, Request], Awaitable[Any]]
    get_sales_objectives: Callable[..., Awaitable[Any]]
    get_sales_objective_suggestion: Callable[..., Awaitable[Any]]
    update_sales_objective: Callable[[str, Any, Request], Awaitable[Any]]
    approve_sales_objective: Callable[[str, Any, Request], Awaitable[Any]]


def build_sales_objectives_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    objective_editor_roles: Sequence[str],
    objective_approver_roles: Sequence[str],
    objective_draft: str,
    objective_approved: str,
    objective_pending: str,
    objective_rejected: str,
    seller_role: str,
    object_id_cls: Any,
    ensure_doc_scope_access: Callable[..., None],
    log_audit_event: Callable[..., Awaitable[None]],
    serialize_doc: Callable[[Any], Any],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    list_sales_objectives_with_progress: Callable[..., Awaitable[Any]],
    sale_effective_revenue: Callable[[Dict[str, Any]], float],
    list_sales_objectives: Callable[..., Awaitable[Any]],
    find_user_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_group_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    list_sales: Callable[..., Awaitable[Any]],
    build_sales_objective_suggestion: Callable[..., Awaitable[Any]],
    add_months_ym: Callable[[int, int, int], Any],
    to_non_negative_float: Callable[[Any, float], float],
    is_price_bulletin_active: Callable[..., bool],
    build_catalog_tree_from_source: Callable[..., Dict[str, Any]],
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_catalog_price: Callable[[Any], Optional[float]],
    list_price_bulletins: Callable[..., Awaitable[Any]],
) -> SalesObjectivesHandlerBundle:
    async def create_sales_objective(objective_data: Any, request: Request):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if role not in objective_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(objective_data.agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")

        agency = await db.agencies.find_one({"_id": object_id_cls(objective_data.agency_id)})
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        seller_id = None
        if objective_data.seller_id:
            if not object_id_cls.is_valid(objective_data.seller_id):
                raise HTTPException(status_code=400, detail="Invalid seller_id")
            seller = await db.users.find_one({"_id": object_id_cls(objective_data.seller_id)})
            if not seller:
                raise HTTPException(status_code=404, detail="Seller not found")
            ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
            if seller.get("role") != seller_role:
                raise HTTPException(status_code=400, detail="Selected user is not a seller")
            if str(seller.get("agency_id") or "") != objective_data.agency_id:
                raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")
            seller_id = objective_data.seller_id

        normalized_vehicle_line = str(objective_data.vehicle_line or "").strip() or None

        now = datetime.now(timezone.utc)
        current_user_id = current_user.get("id")
        is_draft = bool(objective_data.save_as_draft)
        approval_status = objective_draft if is_draft else objective_approved
        objective_doc = {
            "seller_id": seller_id,
            "agency_id": objective_data.agency_id,
            "brand_id": agency.get("brand_id"),
            "group_id": agency.get("group_id"),
            "month": objective_data.month,
            "year": objective_data.year,
            "units_target": objective_data.units_target,
            "revenue_target": objective_data.revenue_target,
            "vehicle_line": normalized_vehicle_line,
            "approval_status": approval_status,
            "approval_comment": None,
            "created_by": current_user_id,
            "approved_by": None if is_draft else current_user_id,
            "approved_at": None if is_draft else now,
            "rejected_by": None,
            "rejected_at": None,
            "created_at": now,
            "updated_at": now,
            "updated_by": current_user_id,
        }
        result = await db.sales_objectives.insert_one(objective_doc)
        objective_doc["id"] = str(result.inserted_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_sales_objective",
            entity_type="sales_objective",
            entity_id=str(result.inserted_id),
            group_id=objective_doc.get("group_id"),
            brand_id=objective_doc.get("brand_id"),
            agency_id=objective_doc.get("agency_id"),
            details={
                "seller_id": seller_id,
                "month": objective_data.month,
                "year": objective_data.year,
                "units_target": objective_data.units_target,
                "revenue_target": objective_data.revenue_target,
                "vehicle_line": normalized_vehicle_line,
                "approval_status": approval_status,
                "save_as_draft": bool(objective_data.save_as_draft),
            },
        )
        return serialize_doc(objective_doc)

    async def get_sales_objectives(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
        include_seller_objectives: bool = False,
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if current_user["role"] == seller_role:
            current_seller_id = current_user.get("id")
            if not current_seller_id:
                return []
            if seller_id and seller_id != current_seller_id:
                raise HTTPException(status_code=403, detail="No tienes acceso a este vendedor")
            query["seller_id"] = current_seller_id
        elif seller_id:
            query["seller_id"] = seller_id
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id

        if month:
            query["month"] = month
        if year:
            query["year"] = year

        if not seller_id and not include_seller_objectives and "seller_id" not in query:
            query["$or"] = [
                {"seller_id": None},
                {"seller_id": {"$exists": False}},
            ]

        return await list_sales_objectives_with_progress(
            db,
            objectives_query=query,
            objective_approved=objective_approved,
            objective_pending=objective_pending,
            serialize_doc=serialize_doc,
            sale_effective_revenue=sale_effective_revenue,
            list_sales_objectives=list_sales_objectives,
            find_user_by_id=find_user_by_id,
            find_agency_by_id=find_agency_by_id,
            find_brand_by_id=find_brand_by_id,
            find_group_by_id=find_group_by_id,
            list_sales=list_sales,
        )

    async def get_sales_objective_suggestion(
        request: Request,
        agency_id: str,
        seller_id: str,
        month: Optional[int] = None,
        year: Optional[int] = None,
        lookback_months: int = 6,
    ):
        current_user = await get_current_user(request)
        now = datetime.now(timezone.utc)
        target_month = int(month or now.month)
        target_year = int(year or now.year)
        safe_lookback = max(3, min(int(lookback_months), 24))

        if target_month < 1 or target_month > 12:
            raise HTTPException(status_code=400, detail="Month must be between 1 and 12")
        if target_year < 2000 or target_year > 2100:
            raise HTTPException(status_code=400, detail="Year must be between 2000 and 2100")
        if not object_id_cls.is_valid(agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")
        if not object_id_cls.is_valid(seller_id):
            raise HTTPException(status_code=400, detail="Invalid seller_id")

        agency = await find_agency_by_id(db, agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        seller = await find_user_by_id(db, seller_id)
        if not seller:
            raise HTTPException(status_code=404, detail="Seller not found")
        ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
        if seller.get("role") != seller_role:
            raise HTTPException(status_code=400, detail="Selected user is not a seller")
        if str(seller.get("agency_id") or "") != agency_id:
            raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

        return await build_sales_objective_suggestion(
            db,
            agency_id=agency_id,
            seller_id=seller_id,
            target_month=target_month,
            target_year=target_year,
            safe_lookback=safe_lookback,
            agency=agency,
            seller=seller,
            add_months_ym=add_months_ym,
            sale_effective_revenue=sale_effective_revenue,
            to_non_negative_float=to_non_negative_float,
            is_price_bulletin_active=is_price_bulletin_active,
            build_catalog_tree_from_source=build_catalog_tree_from_source,
            find_catalog_make=find_catalog_make,
            parse_catalog_price=parse_catalog_price,
            list_sales=list_sales,
            list_price_bulletins=list_price_bulletins,
            find_brand_by_id=find_brand_by_id,
        )

    async def update_sales_objective(objective_id: str, objective_data: Any, request: Request):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if role not in objective_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(objective_id):
            raise HTTPException(status_code=400, detail="Invalid objective_id")

        previous = await db.sales_objectives.find_one({"_id": object_id_cls(objective_id)})
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este objetivo")
        normalized_vehicle_line = str(objective_data.vehicle_line or "").strip() or None
        now = datetime.now(timezone.utc)
        is_draft = bool(objective_data.save_as_draft)
        update_fields: Dict[str, Any] = {
            "units_target": objective_data.units_target,
            "revenue_target": objective_data.revenue_target,
            "vehicle_line": normalized_vehicle_line,
            "updated_at": now,
            "updated_by": current_user.get("id"),
        }

        update_fields.update({
            "approval_status": objective_draft if is_draft else objective_approved,
            "approved_by": None if is_draft else current_user.get("id"),
            "approved_at": None if is_draft else now,
            "rejected_by": None,
            "rejected_at": None,
            "approval_comment": None,
        })

        await db.sales_objectives.update_one({"_id": object_id_cls(objective_id)}, {"$set": update_fields})
        objective = await db.sales_objectives.find_one({"_id": object_id_cls(objective_id)})

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_sales_objective",
            entity_type="sales_objective",
            entity_id=objective_id,
            group_id=objective.get("group_id") if objective else previous.get("group_id") if previous else None,
            brand_id=objective.get("brand_id") if objective else previous.get("brand_id") if previous else None,
            agency_id=objective.get("agency_id") if objective else previous.get("agency_id") if previous else None,
            details={
                "before": {
                    "units_target": previous.get("units_target") if previous else None,
                    "revenue_target": previous.get("revenue_target") if previous else None,
                    "vehicle_line": previous.get("vehicle_line") if previous else None,
                    "approval_status": previous.get("approval_status") if previous else None,
                },
                "after": {
                    "units_target": objective.get("units_target") if objective else None,
                    "revenue_target": objective.get("revenue_target") if objective else None,
                    "vehicle_line": objective.get("vehicle_line") if objective else None,
                    "approval_status": objective.get("approval_status") if objective else None,
                },
            },
        )
        return serialize_doc(objective)

    async def approve_sales_objective(
        objective_id: str,
        approval: Any,
        request: Request,
    ):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if role not in objective_approver_roles:
            raise HTTPException(status_code=403, detail="Solo Gerente General operativo del dealer puede aprobar")

        if not object_id_cls.is_valid(objective_id):
            raise HTTPException(status_code=400, detail="Invalid objective_id")

        previous = await db.sales_objectives.find_one({"_id": object_id_cls(objective_id)})
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este objetivo")

        decision = str(approval.decision or "").strip().lower()
        if decision not in {objective_approved, objective_rejected}:
            raise HTTPException(status_code=400, detail="Decision must be approved or rejected")

        now = datetime.now(timezone.utc)
        update_fields: Dict[str, Any] = {
            "approval_status": decision,
            "updated_at": now,
            "updated_by": current_user.get("id"),
        }

        if decision == objective_approved:
            update_fields.update({
                "approved_by": current_user.get("id"),
                "approved_at": now,
                "rejected_by": None,
                "rejected_at": None,
                "approval_comment": None,
            })
        else:
            comment = str(approval.comment or "").strip()
            if not comment:
                raise HTTPException(status_code=400, detail="Rejection requires a comment")
            update_fields.update({
                "approved_by": None,
                "approved_at": None,
                "rejected_by": current_user.get("id"),
                "rejected_at": now,
                "approval_comment": comment,
            })

        await db.sales_objectives.update_one({"_id": object_id_cls(objective_id)}, {"$set": update_fields})
        objective = await db.sales_objectives.find_one({"_id": object_id_cls(objective_id)})

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="approve_sales_objective",
            entity_type="sales_objective",
            entity_id=objective_id,
            group_id=objective.get("group_id") if objective else previous.get("group_id") if previous else None,
            brand_id=objective.get("brand_id") if objective else previous.get("brand_id") if previous else None,
            agency_id=objective.get("agency_id") if objective else previous.get("agency_id") if previous else None,
            details={
                "before_status": previous.get("approval_status") if previous else None,
                "after_status": objective.get("approval_status") if objective else None,
                "comment": update_fields.get("approval_comment"),
            },
        )
        return serialize_doc(objective)

    return SalesObjectivesHandlerBundle(
        create_sales_objective=create_sales_objective,
        get_sales_objectives=get_sales_objectives,
        get_sales_objective_suggestion=get_sales_objective_suggestion,
        update_sales_objective=update_sales_objective,
        approve_sales_objective=approve_sales_objective,
    )
