from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class FinancialRatesHandlerBundle:
    create_financial_rate: Callable[[Any, Request], Awaitable[Any]]
    apply_group_default_financial_rate: Callable[[Any, Request], Awaitable[Any]]
    get_financial_rates: Callable[..., Awaitable[Any]]
    update_financial_rate: Callable[[str, Any, Request], Awaitable[Any]]
    delete_financial_rate: Callable[[str, Request], Awaitable[Any]]


def build_financial_rates_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    financial_rate_manager_roles: Sequence[str],
    agency_scoped_roles: Sequence[str],
    resolve_financial_rate_scope: Callable[..., Awaitable[Dict[str, Any]]],
    build_default_financial_rate_name: Callable[..., Awaitable[str]],
    build_financial_rate_record: Callable[..., Dict[str, Any]],
    monthly_to_annual: Callable[[float], float],
    insert_financial_rate_by_doc: Callable[..., Awaitable[str]],
    log_audit_event: Callable[..., Awaitable[None]],
    serialize_doc: Callable[[Any], Any],
    find_latest_financial_rate: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    list_brands_for_group: Callable[..., Awaitable[Any]],
    list_brand_financial_rates_for_group: Callable[..., Awaitable[Any]],
    find_group_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    plan_group_default_rate_docs: Callable[..., Dict[str, Any]],
    extract_rate_components_from_doc: Callable[[Optional[Dict[str, Any]]], Dict[str, Optional[float]]],
    insert_many_financial_rates: Callable[..., Awaitable[int]],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    list_financial_rates: Callable[..., Awaitable[Any]],
    enrich_financial_rate: Callable[..., Awaitable[Dict[str, Any]]],
    resolve_effective_rate_components_for_scope: Callable[..., Awaitable[Dict[str, float]]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    object_id_cls: Any,
    find_financial_rate_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    ensure_doc_scope_access: Callable[..., None],
    build_financial_rate_update_fields: Callable[..., Dict[str, Any]],
    update_financial_rate_by_id: Callable[..., Awaitable[Any]],
    delete_financial_rate_by_id: Callable[..., Awaitable[Any]],
) -> FinancialRatesHandlerBundle:
    async def create_financial_rate(rate_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in financial_rate_manager_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        scope = await resolve_financial_rate_scope(
            current_user=current_user,
            group_id=rate_data.group_id,
            brand_id=rate_data.brand_id,
            agency_id=rate_data.agency_id,
        )

        rate_name = str(rate_data.name or "").strip()
        if not rate_name:
            rate_name = await build_default_financial_rate_name(
                group_id=scope["group_id"],
                brand_id=scope["brand_id"],
                agency_id=scope["agency_id"],
            )

        financial_rate_payload = build_financial_rate_record(
            scope=scope,
            tiie_rate=rate_data.tiie_rate,
            spread=rate_data.spread,
            grace_days=rate_data.grace_days,
            rate_name=rate_name,
            now=datetime.now(timezone.utc),
            monthly_to_annual=monthly_to_annual,
        )
        rate_doc = financial_rate_payload["rate_doc"]
        tiie_monthly = financial_rate_payload["tiie_monthly"]
        spread_monthly = financial_rate_payload["spread_monthly"]

        created_rate_id = await insert_financial_rate_by_doc(db, rate_doc)
        rate_doc["id"] = created_rate_id
        rate_doc["total_rate"] = financial_rate_payload["total_rate"]

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_financial_rate",
            entity_type="financial_rate",
            entity_id=created_rate_id,
            group_id=scope["group_id"],
            brand_id=scope["brand_id"],
            agency_id=scope["agency_id"],
            details={
                "name": rate_name,
                "rate_period": "monthly",
                "tiie_rate": tiie_monthly,
                "spread": spread_monthly,
                "grace_days": rate_data.grace_days,
                "total_rate": rate_doc["total_rate"],
            },
        )
        return serialize_doc(rate_doc)

    async def apply_group_default_financial_rate(
        payload: Any,
        request: Request,
    ):
        current_user = await get_current_user(request)
        if current_user["role"] not in financial_rate_manager_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        scope = await resolve_financial_rate_scope(
            current_user=current_user,
            group_id=payload.group_id,
            brand_id=None,
            agency_id=None,
        )

        group_id = scope["group_id"]
        group_base_rate = await find_latest_financial_rate(
            db,
            group_id=group_id,
            brand_id=None,
            agency_id=None,
        )
        if not group_base_rate:
            raise HTTPException(
                status_code=400,
                detail="Primero crea una tasa general de grupo para poder aplicarla a marcas.",
            )

        brands = await list_brands_for_group(db, group_id=group_id, limit=1000)
        if not brands:
            return {
                "group_id": group_id,
                "created_count": 0,
                "skipped_count": 0,
                "message": "El grupo no tiene marcas para aplicar la tasa.",
            }

        existing_brand_rates = await list_brand_financial_rates_for_group(
            db,
            group_id=group_id,
            limit=5000,
        )
        existing_brand_ids = {
            str(rate.get("brand_id"))
            for rate in existing_brand_rates
            if rate.get("brand_id")
        }

        group_doc = await find_group_by_id(db, group_id)
        group_name = str(group_doc.get("name") or "Grupo") if group_doc else "Grupo"

        planned_defaults = plan_group_default_rate_docs(
            group_id=group_id,
            group_name=group_name,
            group_base_rate=group_base_rate,
            brands=brands,
            existing_brand_ids=existing_brand_ids,
            now=datetime.now(timezone.utc),
            extract_rate_components_from_doc=extract_rate_components_from_doc,
            monthly_to_annual=monthly_to_annual,
        )
        docs_to_insert = planned_defaults["docs_to_insert"]
        skipped_count = planned_defaults["skipped_count"]
        base_tiie = planned_defaults["base_tiie"]
        base_spread = planned_defaults["base_spread"]
        base_grace_days = planned_defaults["base_grace_days"]

        created_count = 0
        if docs_to_insert:
            created_count = await insert_many_financial_rates(db, docs_to_insert)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="apply_group_default_financial_rate",
            entity_type="financial_rate",
            entity_id=str(group_base_rate.get("_id")),
            group_id=group_id,
            details={
                "group_base_rate_id": str(group_base_rate.get("_id")),
                "group_base_rate_name": group_base_rate.get("name"),
                "base_tiie_rate": base_tiie,
                "base_spread": base_spread,
                "base_grace_days": base_grace_days,
                "created_count": created_count,
                "skipped_count": skipped_count,
            },
        )

        return {
            "group_id": group_id,
            "created_count": created_count,
            "skipped_count": skipped_count,
            "message": "Tasa general aplicada a marcas sin tasa propia.",
        }

    async def get_financial_rates(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
    ):
        current_user = await get_current_user(request)
        if current_user.get("role") in agency_scoped_roles:
            raise HTTPException(status_code=403, detail="No autorizado para ver tasas financieras")
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

        rates = await list_financial_rates(db, query=query, limit=1000)
        result = []
        for rate_doc in rates:
            enriched_rate = await enrich_financial_rate(
                db,
                rate_doc=rate_doc,
                serialize_doc=serialize_doc,
                extract_rate_components_from_doc=extract_rate_components_from_doc,
                monthly_to_annual=monthly_to_annual,
                resolve_effective_rate_components_for_scope=resolve_effective_rate_components_for_scope,
                find_group_by_id=find_group_by_id,
                find_brand_by_id=find_brand_by_id,
                find_agency_by_id=find_agency_by_id,
            )
            result.append(enriched_rate)

        return result

    async def update_financial_rate(rate_id: str, rate_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in financial_rate_manager_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(rate_id):
            raise HTTPException(status_code=400, detail="Invalid rate_id")

        previous = await find_financial_rate_by_id(db, rate_id)
        if not previous:
            raise HTTPException(status_code=404, detail="Rate not found")
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta tasa")

        scope = await resolve_financial_rate_scope(
            current_user=current_user,
            group_id=rate_data.group_id,
            brand_id=rate_data.brand_id,
            agency_id=rate_data.agency_id,
        )
        rate_name = str(rate_data.name or "").strip()
        if not rate_name:
            rate_name = await build_default_financial_rate_name(
                group_id=scope["group_id"],
                brand_id=scope["brand_id"],
                agency_id=scope["agency_id"],
            )

        update_payload = build_financial_rate_update_fields(
            scope=scope,
            tiie_rate=rate_data.tiie_rate,
            spread=rate_data.spread,
            grace_days=rate_data.grace_days,
            rate_name=rate_name,
            monthly_to_annual=monthly_to_annual,
        )

        await update_financial_rate_by_id(
            db,
            rate_id=rate_id,
            set_fields=update_payload["update_fields"],
        )

        rate = await find_financial_rate_by_id(db, rate_id)
        if not rate:
            raise HTTPException(status_code=404, detail="Rate not found")
        result = await enrich_financial_rate(
            db,
            rate_doc=rate,
            serialize_doc=serialize_doc,
            extract_rate_components_from_doc=extract_rate_components_from_doc,
            monthly_to_annual=monthly_to_annual,
            resolve_effective_rate_components_for_scope=resolve_effective_rate_components_for_scope,
            find_group_by_id=find_group_by_id,
            find_brand_by_id=find_brand_by_id,
            find_agency_by_id=find_agency_by_id,
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_financial_rate",
            entity_type="financial_rate",
            entity_id=rate_id,
            group_id=rate.get("group_id") if rate else previous.get("group_id") if previous else None,
            brand_id=rate.get("brand_id") if rate else previous.get("brand_id") if previous else None,
            agency_id=rate.get("agency_id") if rate else previous.get("agency_id") if previous else None,
            details={
                "before": {
                    "name": previous.get("name") if previous else None,
                    "tiie_rate": previous.get("tiie_rate") if previous else None,
                    "spread": previous.get("spread") if previous else None,
                    "grace_days": previous.get("grace_days") if previous else None,
                    "group_id": previous.get("group_id") if previous else None,
                    "brand_id": previous.get("brand_id") if previous else None,
                    "agency_id": previous.get("agency_id") if previous else None,
                },
                "after": {
                    "name": rate.get("name") if rate else None,
                    "tiie_rate": rate.get("tiie_rate") if rate else None,
                    "spread": rate.get("spread") if rate else None,
                    "grace_days": rate.get("grace_days") if rate else None,
                    "group_id": rate.get("group_id") if rate else None,
                    "brand_id": rate.get("brand_id") if rate else None,
                    "agency_id": rate.get("agency_id") if rate else None,
                },
            },
        )
        return result

    async def delete_financial_rate(rate_id: str, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in financial_rate_manager_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(rate_id):
            raise HTTPException(status_code=400, detail="Invalid rate_id")

        previous = await find_financial_rate_by_id(db, rate_id)
        if not previous:
            raise HTTPException(status_code=404, detail="Rate not found")
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta tasa")
        await delete_financial_rate_by_id(db, rate_id=rate_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="delete_financial_rate",
            entity_type="financial_rate",
            entity_id=rate_id,
            group_id=previous.get("group_id") if previous else None,
            brand_id=previous.get("brand_id") if previous else None,
            agency_id=previous.get("agency_id") if previous else None,
            details={
                "name": previous.get("name") if previous else None,
                "tiie_rate": previous.get("tiie_rate") if previous else None,
                "spread": previous.get("spread") if previous else None,
                "grace_days": previous.get("grace_days") if previous else None,
            },
        )
        return {"message": "Rate deleted"}

    return FinancialRatesHandlerBundle(
        create_financial_rate=create_financial_rate,
        apply_group_default_financial_rate=apply_group_default_financial_rate,
        get_financial_rates=get_financial_rates,
        update_financial_rate=update_financial_rate,
        delete_financial_rate=delete_financial_rate,
    )
