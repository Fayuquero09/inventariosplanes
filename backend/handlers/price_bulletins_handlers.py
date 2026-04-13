from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class PriceBulletinsHandlerBundle:
    get_price_bulletins: Callable[..., Awaitable[Any]]
    upsert_price_bulletins_bulk: Callable[[Any, Request], Awaitable[Any]]
    delete_price_bulletin: Callable[[str, Request], Awaitable[Any]]


def build_price_bulletins_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    list_price_bulletins_with_enrichment: Callable[..., Awaitable[Any]],
    serialize_doc: Callable[[Any], Any],
    is_price_bulletin_active: Callable[[Dict[str, Any], datetime], bool],
    price_bulletin_editor_roles: Sequence[str],
    resolve_price_bulletin_scope: Callable[..., Awaitable[Dict[str, Any]]],
    normalize_iso_date_string: Callable[..., Optional[str]],
    upsert_price_bulletins_items: Callable[..., Awaitable[Any]],
    reprice_sales_for_price_bulletin: Callable[..., Awaitable[Any]],
    price_item_applies_to_sale: Callable[..., bool],
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override: Callable[..., Dict[str, Any]],
    calculate_commission: Callable[..., Awaitable[float]],
    to_non_negative_float: Callable[[Any, float], float],
    coerce_utc_datetime: Callable[[Any], Optional[datetime]],
    log_audit_event: Callable[..., Awaitable[None]],
    ensure_doc_scope_access: Callable[..., None],
    remove_price_bulletin: Callable[..., Awaitable[Any]],
    object_id_cls: Any,
) -> PriceBulletinsHandlerBundle:
    async def get_price_bulletins(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        model: Optional[str] = None,
        active_only: bool = False,
        latest_per_model: bool = False,
        include_brand_defaults: bool = True,
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

        normalized_agency_id = str(agency_id or "").strip() or None
        if normalized_agency_id:
            if include_brand_defaults:
                query["$or"] = [{"agency_id": normalized_agency_id}, {"agency_id": None}]
            else:
                query["agency_id"] = normalized_agency_id
        else:
            query["agency_id"] = None

        normalized_model = str(model or "").strip()
        if normalized_model:
            query["model"] = normalized_model

        return await list_price_bulletins_with_enrichment(
            db,
            query=query,
            normalized_agency_id=normalized_agency_id,
            active_only=active_only,
            latest_per_model=latest_per_model,
            serialize_doc=serialize_doc,
            is_price_bulletin_active=is_price_bulletin_active,
        )

    async def upsert_price_bulletins_bulk(payload: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") not in price_bulletin_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not payload.items:
            raise HTTPException(status_code=400, detail="At least one item is required")

        scope = await resolve_price_bulletin_scope(
            db,
            current_user=current_user,
            group_id=payload.group_id,
            brand_id=payload.brand_id,
            agency_id=payload.agency_id,
            validate_scope_filters=validate_scope_filters,
        )

        effective_from = normalize_iso_date_string(
            payload.effective_from or datetime.now(timezone.utc).date().isoformat(),
            field_name="effective_from",
            required=True,
        )
        effective_to = normalize_iso_date_string(payload.effective_to, field_name="effective_to", required=False)
        if effective_to and effective_to < effective_from:
            raise HTTPException(status_code=400, detail="effective_to must be on or after effective_from")

        bulletin_name = str(payload.bulletin_name or "").strip()
        if not bulletin_name:
            bulletin_name = f"Boletín {scope['brand_name']} {effective_from}"
        notes = str(payload.notes or "").strip() or None

        now = datetime.now(timezone.utc)
        updated_count, valid_items = await upsert_price_bulletins_items(
            db,
            scope=scope,
            items=payload.items,
            effective_from=effective_from,
            effective_to=effective_to,
            bulletin_name=bulletin_name,
            notes=notes,
            current_user_id=current_user.get("id"),
            now=now,
            to_non_negative_float=to_non_negative_float,
        )

        if updated_count == 0:
            raise HTTPException(status_code=400, detail="No valid items to save")

        repricing_summary = await reprice_sales_for_price_bulletin(
            db,
            scope=scope,
            effective_from=effective_from,
            effective_to=effective_to,
            items=valid_items,
            price_item_applies_to_sale=price_item_applies_to_sale,
            resolve_effective_sale_pricing_for_model=resolve_effective_sale_pricing_for_model,
            apply_manual_sale_price_override=apply_manual_sale_price_override,
            calculate_commission=calculate_commission,
            to_non_negative_float=to_non_negative_float,
            coerce_utc_datetime=coerce_utc_datetime,
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="upsert_price_bulletins_bulk",
            entity_type="price_bulletin",
            entity_id=None,
            group_id=scope["group_id"],
            brand_id=scope["brand_id"],
            agency_id=scope["agency_id"],
            details={
                "bulletin_name": bulletin_name,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "items_count": updated_count,
                "repricing": repricing_summary,
            },
        )

        return {
            "message": "Price bulletins saved",
            "group_id": scope["group_id"],
            "brand_id": scope["brand_id"],
            "agency_id": scope["agency_id"],
            "bulletin_name": bulletin_name,
            "effective_from": effective_from,
            "effective_to": effective_to,
            "items_count": updated_count,
            "repricing": repricing_summary,
        }

    async def delete_price_bulletin(bulletin_id: str, request: Request):
        current_user = await get_current_user(request)
        if current_user.get("role") not in price_bulletin_editor_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(bulletin_id):
            raise HTTPException(status_code=400, detail="Invalid bulletin_id")

        previous = await db.price_bulletins.find_one({"_id": object_id_cls(bulletin_id)})
        if not previous:
            raise HTTPException(status_code=404, detail="Price bulletin not found")
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a este boletín")
        await remove_price_bulletin(
            db,
            bulletin_id=bulletin_id,
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="delete_price_bulletin",
            entity_type="price_bulletin",
            entity_id=bulletin_id,
            group_id=previous.get("group_id"),
            brand_id=previous.get("brand_id"),
            agency_id=previous.get("agency_id"),
            details={
                "model": previous.get("model"),
                "version": previous.get("version"),
                "effective_from": previous.get("effective_from"),
            },
        )
        return {"message": "Price bulletin deleted"}

    return PriceBulletinsHandlerBundle(
        get_price_bulletins=get_price_bulletins,
        upsert_price_bulletins_bulk=upsert_price_bulletins_bulk,
        delete_price_bulletin=delete_price_bulletin,
    )
