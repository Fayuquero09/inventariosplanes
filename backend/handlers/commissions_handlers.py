from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class CommissionsHandlerBundle:
    get_commission_matrix: Callable[..., Awaitable[Any]]
    upsert_commission_matrix: Callable[[Any, Request], Awaitable[Any]]
    create_commission_rule: Callable[[Any, Request], Awaitable[Any]]
    get_commission_rules: Callable[..., Awaitable[Any]]
    update_commission_rule: Callable[[str, Any, Request], Awaitable[Any]]
    approve_commission_rule: Callable[[str, Any, Request], Awaitable[Any]]
    delete_commission_rule: Callable[[str, Request], Awaitable[Any]]
    commission_simulator: Callable[[Any, Request], Awaitable[Any]]
    create_commission_closure: Callable[[Any, Request], Awaitable[Any]]
    get_commission_closures: Callable[..., Awaitable[Any]]
    approve_commission_closure: Callable[[str, Any, Request], Awaitable[Any]]


def build_commissions_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    object_id_cls: Any,
    ensure_doc_scope_access: Callable[..., None],
    log_audit_event: Callable[..., Awaitable[None]],
    serialize_doc: Callable[[Any], Any],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    normalize_commission_matrix_general: Callable[[Optional[Dict[str, Any]]], Dict[str, Any]],
    normalize_commission_matrix_models: Callable[[Optional[List[Dict[str, Any]]]], List[Dict[str, Any]]],
    get_catalog_models_for_brand: Callable[[Optional[str]], List[Dict[str, Any]]],
    build_matrix_models_response: Callable[[List[Dict[str, Any]], List[Dict[str, Any]], float], List[Dict[str, Any]]],
    to_non_negative_float: Callable[[Any, float], float],
    normalize_commission_status: Callable[..., str],
    commission_matrix_editor_roles: Sequence[str],
    commission_proposer_roles: Sequence[str],
    commission_approver_roles: Sequence[str],
    commission_pending: str,
    commission_approved: str,
    commission_rejected: str,
    dealer_sales_effective_roles: Sequence[str],
    dealer_general_effective_roles: Sequence[str],
    dealer_seller_role: str,
    seller_role: str,
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_group_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_user_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_commission_matrix_by_agency: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    upsert_commission_matrix_by_agency: Callable[..., Awaitable[Any]],
    build_commission_matrix_upsert_fields: Callable[..., Dict[str, Any]],
    build_commission_rule_doc: Callable[..., Dict[str, Any]],
    insert_commission_rule: Callable[..., Awaitable[str]],
    list_commission_rules: Callable[..., Awaitable[Any]],
    find_commission_rule_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    build_commission_rule_update_fields: Callable[..., Dict[str, Any]],
    update_commission_rule_by_id: Callable[..., Awaitable[Any]],
    build_commission_approval_update_fields: Callable[..., Dict[str, Any]],
    delete_commission_rule_by_id: Callable[..., Awaitable[Any]],
    list_active_rules_by_agency: Callable[..., Awaitable[Any]],
    build_commission_simulator_projection: Callable[..., Dict[str, Any]],
    calculate_commission_from_rules: Callable[..., float],
    build_month_bounds: Callable[[int, int], Any],
    list_sales_for_closure: Callable[..., Awaitable[Any]],
    build_commission_closure_snapshot: Callable[..., Dict[str, Any]],
    find_commission_closure_by_scope: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    build_commission_closure_doc: Callable[..., Dict[str, Any]],
    update_commission_closure_by_id: Callable[..., Awaitable[Any]],
    find_commission_closure_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    insert_commission_closure: Callable[..., Awaitable[str]],
    list_commission_closures: Callable[..., Awaitable[Any]],
) -> CommissionsHandlerBundle:
    async def _serialize_commission_matrix(
        agency: Dict[str, Any],
        matrix_doc: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        brand_name = ""
        group_name = ""
        if agency.get("brand_id"):
            brand = await find_brand_by_id(db, str(agency["brand_id"]))
            if brand:
                brand_name = str(brand.get("name") or "")
        if agency.get("group_id"):
            group = await find_group_by_id(db, str(agency["group_id"]))
            if group:
                group_name = str(group.get("name") or "")

        general = normalize_commission_matrix_general((matrix_doc or {}).get("general"))
        overrides = normalize_commission_matrix_models((matrix_doc or {}).get("models"))
        catalog_models = get_catalog_models_for_brand(brand_name)
        models_response = build_matrix_models_response(catalog_models, overrides, general.get("global_percentage", 0.0))

        return {
            "agency_id": str(agency.get("_id")),
            "agency_name": agency.get("name"),
            "brand_id": str(agency.get("brand_id")) if agency.get("brand_id") else None,
            "brand_name": brand_name,
            "group_id": str(agency.get("group_id")) if agency.get("group_id") else None,
            "group_name": group_name,
            "general": general,
            "models": models_response,
            "updated_at": matrix_doc.get("updated_at").isoformat() if matrix_doc and isinstance(matrix_doc.get("updated_at"), datetime) else None,
            "updated_by": matrix_doc.get("updated_by") if matrix_doc else None,
        }

    async def _serialize_commission_rule(rule_doc: Dict[str, Any]) -> Dict[str, Any]:
        serialized = serialize_doc(rule_doc)
        serialized["approval_status"] = normalize_commission_status(
            serialized.get("approval_status"),
            pending_status=commission_pending,
            approved_status=commission_approved,
            rejected_status=commission_rejected,
        )

        if rule_doc.get("agency_id"):
            agency = await find_agency_by_id(db, rule_doc["agency_id"])
            if agency:
                serialized["agency_name"] = agency["name"]
        if rule_doc.get("brand_id"):
            brand = await find_brand_by_id(db, rule_doc["brand_id"])
            if brand:
                serialized["brand_name"] = brand["name"]
        if rule_doc.get("group_id"):
            group = await find_group_by_id(db, rule_doc["group_id"])
            if group:
                serialized["group_name"] = group["name"]

        if rule_doc.get("submitted_by") and object_id_cls.is_valid(rule_doc["submitted_by"]):
            submitter = await find_user_by_id(db, rule_doc["submitted_by"])
            if submitter:
                serialized["submitted_by_name"] = submitter.get("name")
        if rule_doc.get("approved_by") and object_id_cls.is_valid(rule_doc["approved_by"]):
            approver = await find_user_by_id(db, rule_doc["approved_by"])
            if approver:
                serialized["approved_by_name"] = approver.get("name")
        if rule_doc.get("rejected_by") and object_id_cls.is_valid(rule_doc["rejected_by"]):
            rejector = await find_user_by_id(db, rule_doc["rejected_by"])
            if rejector:
                serialized["rejected_by_name"] = rejector.get("name")
        return serialized

    async def get_commission_matrix(request: Request, agency_id: str):
        current_user = await get_current_user(request)
        if not object_id_cls.is_valid(agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")

        agency = await find_agency_by_id(db, agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        matrix_doc = await find_commission_matrix_by_agency(db, agency_id=agency_id)
        return await _serialize_commission_matrix(agency, matrix_doc)

    async def upsert_commission_matrix(payload: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") not in commission_matrix_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")
        if not object_id_cls.is_valid(payload.agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")

        agency = await find_agency_by_id(db, payload.agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        now = datetime.now(timezone.utc)
        normalized_general = normalize_commission_matrix_general(payload.general.model_dump())
        normalized_models = normalize_commission_matrix_models([model.model_dump() for model in payload.models])

        matrix_upsert_payload = build_commission_matrix_upsert_fields(
            agency_id=payload.agency_id,
            brand_id=agency.get("brand_id"),
            group_id=agency.get("group_id"),
            normalized_general=normalized_general,
            normalized_models=normalized_models,
            current_user_id=current_user.get("id"),
            now=now,
        )
        await upsert_commission_matrix_by_agency(
            db,
            agency_id=payload.agency_id,
            set_fields=matrix_upsert_payload["set_fields"],
            set_on_insert=matrix_upsert_payload["set_on_insert"],
        )

        matrix_doc = await find_commission_matrix_by_agency(db, agency_id=payload.agency_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="upsert_commission_matrix",
            entity_type="commission_matrix",
            entity_id=str(matrix_doc.get("_id")) if matrix_doc else None,
            group_id=str(agency.get("group_id")) if agency.get("group_id") else None,
            brand_id=str(agency.get("brand_id")) if agency.get("brand_id") else None,
            agency_id=payload.agency_id,
            details={
                "general": normalized_general,
                "models_count": len(normalized_models),
            },
        )

        return await _serialize_commission_matrix(agency, matrix_doc)

    async def create_commission_rule(rule_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") not in commission_proposer_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(rule_data.agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")

        agency = await find_agency_by_id(db, rule_data.agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        now = datetime.now(timezone.utc)
        rule_doc = build_commission_rule_doc(
            agency_id=rule_data.agency_id,
            brand_id=agency.get("brand_id"),
            group_id=agency.get("group_id"),
            name=rule_data.name,
            rule_type=rule_data.rule_type,
            value=rule_data.value,
            min_units=rule_data.min_units,
            max_units=rule_data.max_units,
            current_user_id=current_user.get("id"),
            now=now,
            pending_status=commission_pending,
        )
        inserted_rule_id = await insert_commission_rule(db, rule_doc)
        rule_doc["id"] = inserted_rule_id

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_commission_rule",
            entity_type="commission_rule",
            entity_id=inserted_rule_id,
            group_id=rule_doc.get("group_id"),
            brand_id=rule_doc.get("brand_id"),
            agency_id=rule_doc.get("agency_id"),
            details={
                "name": rule_data.name,
                "rule_type": rule_data.rule_type,
                "value": rule_data.value,
                "min_units": rule_data.min_units,
                "max_units": rule_data.max_units,
                "approval_status": commission_pending,
            },
        )
        return serialize_doc(rule_doc)

    async def get_commission_rules(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id

        rules = await list_commission_rules(db, query=query, limit=1000)
        result = [await _serialize_commission_rule(rule) for rule in rules]
        return result

    async def update_commission_rule(rule_id: str, rule_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") not in commission_proposer_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(rule_id):
            raise HTTPException(status_code=400, detail="Invalid rule_id")

        previous = await find_commission_rule_by_id(db, rule_id)
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
        now = datetime.now(timezone.utc)
        update_fields = build_commission_rule_update_fields(
            name=rule_data.name,
            rule_type=rule_data.rule_type,
            value=rule_data.value,
            min_units=rule_data.min_units,
            max_units=rule_data.max_units,
            current_user_id=current_user.get("id"),
            now=now,
            pending_status=commission_pending,
        )
        await update_commission_rule_by_id(
            db,
            rule_id=rule_id,
            set_fields=update_fields,
        )
        rule = await find_commission_rule_by_id(db, rule_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_commission_rule",
            entity_type="commission_rule",
            entity_id=rule_id,
            group_id=rule.get("group_id") if rule else previous.get("group_id") if previous else None,
            brand_id=rule.get("brand_id") if rule else previous.get("brand_id") if previous else None,
            agency_id=rule.get("agency_id") if rule else previous.get("agency_id") if previous else None,
            details={
                "before": {
                    "name": previous.get("name") if previous else None,
                    "rule_type": previous.get("rule_type") if previous else None,
                    "value": previous.get("value") if previous else None,
                    "min_units": previous.get("min_units") if previous else None,
                    "max_units": previous.get("max_units") if previous else None,
                    "approval_status": previous.get("approval_status") if previous else None,
                },
                "after": {
                    "name": rule.get("name") if rule else None,
                    "rule_type": rule.get("rule_type") if rule else None,
                    "value": rule.get("value") if rule else None,
                    "min_units": rule.get("min_units") if rule else None,
                    "max_units": rule.get("max_units") if rule else None,
                    "approval_status": rule.get("approval_status") if rule else None,
                },
            },
        )
        return await _serialize_commission_rule(rule)

    async def approve_commission_rule(
        rule_id: str,
        approval: Any,
        request: Request,
    ):
        current_user = await get_current_user(request)
        if current_user.get("role") not in commission_approver_roles:
            raise HTTPException(status_code=403, detail="Solo gerente general operativo puede aprobar reglas")

        if not object_id_cls.is_valid(rule_id):
            raise HTTPException(status_code=400, detail="Invalid rule_id")

        previous = await find_commission_rule_by_id(db, rule_id)
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")

        now = datetime.now(timezone.utc)
        try:
            update_fields = build_commission_approval_update_fields(
                decision=approval.decision,
                comment=approval.comment,
                current_user_id=current_user.get("id"),
                now=now,
                approved_status=commission_approved,
                rejected_status=commission_rejected,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        await update_commission_rule_by_id(
            db,
            rule_id=rule_id,
            set_fields=update_fields,
        )
        rule = await find_commission_rule_by_id(db, rule_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="approve_commission_rule",
            entity_type="commission_rule",
            entity_id=rule_id,
            group_id=rule.get("group_id") if rule else previous.get("group_id") if previous else None,
            brand_id=rule.get("brand_id") if rule else previous.get("brand_id") if previous else None,
            agency_id=rule.get("agency_id") if rule else previous.get("agency_id") if previous else None,
            details={
                "before_status": previous.get("approval_status") if previous else None,
                "after_status": rule.get("approval_status") if rule else None,
                "comment": update_fields.get("approval_comment"),
            },
        )
        return await _serialize_commission_rule(rule)

    async def delete_commission_rule(rule_id: str, request: Request):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if role not in commission_proposer_roles and role not in commission_approver_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(rule_id):
            raise HTTPException(status_code=400, detail="Invalid rule_id")

        previous = await find_commission_rule_by_id(db, rule_id)
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta regla")
        previous_status = str(previous.get("approval_status") or "").strip().lower()
        if role in commission_proposer_roles and previous_status == commission_approved:
            raise HTTPException(status_code=403, detail="Solo gerencia general puede borrar reglas aprobadas")
        await delete_commission_rule_by_id(db, rule_id=rule_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="delete_commission_rule",
            entity_type="commission_rule",
            entity_id=rule_id,
            group_id=previous.get("group_id") if previous else None,
            brand_id=previous.get("brand_id") if previous else None,
            agency_id=previous.get("agency_id") if previous else None,
            details={
                "name": previous.get("name") if previous else None,
                "rule_type": previous.get("rule_type") if previous else None,
                "value": previous.get("value") if previous else None,
            },
        )
        return {"message": "Rule deleted"}

    async def commission_simulator(payload: Any, request: Request):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if (
            role not in dealer_sales_effective_roles
            and role not in dealer_general_effective_roles
            and role != dealer_seller_role
        ):
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(payload.agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")
        agency = await find_agency_by_id(db, payload.agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        if role == dealer_seller_role:
            seller_id = current_user.get("id")
            if not seller_id:
                raise HTTPException(status_code=400, detail="Seller identity not found")
        else:
            seller_id = payload.seller_id
            if seller_id and not object_id_cls.is_valid(seller_id):
                raise HTTPException(status_code=400, detail="Invalid seller_id")
            if seller_id:
                seller = await find_user_by_id(db, seller_id)
                if not seller:
                    raise HTTPException(status_code=404, detail="Seller not found")
                ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
                if seller.get("role") != seller_role:
                    raise HTTPException(status_code=400, detail="Selected user is not a seller")
                if seller.get("agency_id") != payload.agency_id:
                    raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

        rules = await list_active_rules_by_agency(
            db,
            agency_id=payload.agency_id,
            approved_status=commission_approved,
            limit=1000,
        )

        projection = build_commission_simulator_projection(
            rules=rules,
            units=payload.units,
            average_ticket=payload.average_ticket,
            average_fi_revenue=payload.average_fi_revenue,
            target_commission=payload.target_commission,
            calculate_commission_from_rules=calculate_commission_from_rules,
        )

        return {
            "agency_id": payload.agency_id,
            "seller_id": seller_id,
            "target_commission": payload.target_commission,
            "units": payload.units,
            "average_ticket": payload.average_ticket,
            "average_fi_revenue": payload.average_fi_revenue,
            "estimated_commission": projection["estimated_commission"],
            "difference_vs_target": projection["difference_vs_target"],
            "suggested_units_to_target": projection["suggested_units_to_target"],
        }

    async def create_commission_closure(payload: Any, request: Request):
        current_user = await get_current_user(request)
        role = current_user.get("role")
        if role not in commission_proposer_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(payload.agency_id):
            raise HTTPException(status_code=400, detail="Invalid agency_id")
        if not object_id_cls.is_valid(payload.seller_id):
            raise HTTPException(status_code=400, detail="Invalid seller_id")

        agency = await find_agency_by_id(db, payload.agency_id)
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, agency, agency_field="_id", detail="No tienes acceso a esta agencia")

        seller = await find_user_by_id(db, payload.seller_id)
        if not seller:
            raise HTTPException(status_code=404, detail="Seller not found")
        ensure_doc_scope_access(current_user, seller, detail="No tienes acceso a este vendedor")
        if seller.get("role") != seller_role:
            raise HTTPException(status_code=400, detail="Selected user is not a seller")
        if seller.get("agency_id") != payload.agency_id:
            raise HTTPException(status_code=400, detail="Seller does not belong to selected agency")

        start_date, end_date = build_month_bounds(payload.year, payload.month)
        sales = await list_sales_for_closure(
            db,
            agency_id=payload.agency_id,
            seller_id=payload.seller_id,
            start_date=start_date,
            end_date=end_date,
            limit=10000,
        )
        now = datetime.now(timezone.utc)
        snapshot = build_commission_closure_snapshot(
            sales=sales,
            now=now,
        )

        existing = await find_commission_closure_by_scope(
            db,
            seller_id=payload.seller_id,
            agency_id=payload.agency_id,
            month=payload.month,
            year=payload.year,
        )
        if existing and str(existing.get("approval_status") or "").strip().lower() == commission_approved:
            raise HTTPException(status_code=409, detail="Approved closure cannot be modified")

        closure_doc = build_commission_closure_doc(
            seller_id=payload.seller_id,
            agency_id=payload.agency_id,
            brand_id=agency.get("brand_id"),
            group_id=agency.get("group_id"),
            month=payload.month,
            year=payload.year,
            snapshot=snapshot,
            current_user_id=current_user.get("id"),
            now=now,
            pending_status=commission_pending,
        )

        if existing:
            await update_commission_closure_by_id(
                db,
                closure_id=str(existing["_id"]),
                set_fields=closure_doc,
            )
            closure = await find_commission_closure_by_id(db, str(existing["_id"]))
            entity_id = str(existing["_id"])
            action = "update_commission_closure"
        else:
            entity_id = await insert_commission_closure(db, closure_doc)
            closure = await find_commission_closure_by_id(db, entity_id)
            action = "create_commission_closure"

        await log_audit_event(
            request=request,
            current_user=current_user,
            action=action,
            entity_type="commission_closure",
            entity_id=entity_id,
            group_id=closure.get("group_id"),
            brand_id=closure.get("brand_id"),
            agency_id=closure.get("agency_id"),
            details={
                "seller_id": payload.seller_id,
                "month": payload.month,
                "year": payload.year,
                "snapshot": snapshot,
                "approval_status": commission_pending,
            },
        )
        return serialize_doc(closure)

    async def get_commission_closures(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id, agency_id=agency_id)
        if group_id:
            query["group_id"] = group_id
        if brand_id:
            query["brand_id"] = brand_id
        if agency_id:
            query["agency_id"] = agency_id
        if seller_id:
            query["seller_id"] = seller_id
        if month:
            query["month"] = month
        if year:
            query["year"] = year

        closures = await list_commission_closures(db, query=query, limit=1000)
        enriched: List[Dict[str, Any]] = []
        for closure in closures:
            closure["approval_status"] = normalize_commission_status(
                closure.get("approval_status"),
                pending_status=commission_pending,
                approved_status=commission_approved,
                rejected_status=commission_rejected,
            )

            if closure.get("seller_id") and object_id_cls.is_valid(closure["seller_id"]):
                seller = await find_user_by_id(db, closure["seller_id"])
                if seller:
                    closure["seller_name"] = seller.get("name")
            if closure.get("agency_id") and object_id_cls.is_valid(closure["agency_id"]):
                agency = await find_agency_by_id(db, closure["agency_id"])
                if agency:
                    closure["agency_name"] = agency.get("name")
            if closure.get("brand_id") and object_id_cls.is_valid(closure["brand_id"]):
                brand = await find_brand_by_id(db, closure["brand_id"])
                if brand:
                    closure["brand_name"] = brand.get("name")
            if closure.get("group_id") and object_id_cls.is_valid(closure["group_id"]):
                group = await find_group_by_id(db, closure["group_id"])
                if group:
                    closure["group_name"] = group.get("name")

            enriched.append(closure)
        return [serialize_doc(c) for c in enriched]

    async def approve_commission_closure(
        closure_id: str,
        approval: Any,
        request: Request,
    ):
        current_user = await get_current_user(request)
        if current_user.get("role") not in commission_approver_roles:
            raise HTTPException(status_code=403, detail="Solo gerente general operativo puede aprobar cierres")

        if not object_id_cls.is_valid(closure_id):
            raise HTTPException(status_code=400, detail="Invalid closure_id")

        previous = await find_commission_closure_by_id(db, closure_id)
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este cierre")

        now = datetime.now(timezone.utc)
        try:
            update_fields = build_commission_approval_update_fields(
                decision=approval.decision,
                comment=approval.comment,
                current_user_id=current_user.get("id"),
                now=now,
                approved_status=commission_approved,
                rejected_status=commission_rejected,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        await update_commission_closure_by_id(
            db,
            closure_id=closure_id,
            set_fields=update_fields,
        )
        closure = await find_commission_closure_by_id(db, closure_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="approve_commission_closure",
            entity_type="commission_closure",
            entity_id=closure_id,
            group_id=closure.get("group_id") if closure else previous.get("group_id") if previous else None,
            brand_id=closure.get("brand_id") if closure else previous.get("brand_id") if previous else None,
            agency_id=closure.get("agency_id") if closure else previous.get("agency_id") if previous else None,
            details={
                "before_status": previous.get("approval_status") if previous else None,
                "after_status": closure.get("approval_status") if closure else None,
                "comment": update_fields.get("approval_comment"),
            },
        )
        return serialize_doc(closure)

    return CommissionsHandlerBundle(
        get_commission_matrix=get_commission_matrix,
        upsert_commission_matrix=upsert_commission_matrix,
        create_commission_rule=create_commission_rule,
        get_commission_rules=get_commission_rules,
        update_commission_rule=update_commission_rule,
        approve_commission_rule=approve_commission_rule,
        delete_commission_rule=delete_commission_rule,
        commission_simulator=commission_simulator,
        create_commission_closure=create_commission_closure,
        get_commission_closures=get_commission_closures,
        approve_commission_closure=approve_commission_closure,
    )
