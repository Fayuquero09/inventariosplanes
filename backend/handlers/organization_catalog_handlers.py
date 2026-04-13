from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class OrganizationCatalogHandlerBundle:
    create_group: Callable[[Any, Request], Awaitable[Any]]
    get_groups: Callable[[Request], Awaitable[Any]]
    get_group: Callable[[str, Request], Awaitable[Any]]
    update_group: Callable[[str, Any, Request], Awaitable[Any]]
    delete_group: Callable[[str, Request, bool], Awaitable[Any]]
    create_brand: Callable[[Any, Request], Awaitable[Any]]
    get_brands: Callable[..., Awaitable[Any]]
    update_brand: Callable[[str, Any, Request], Awaitable[Any]]
    delete_brand: Callable[[str, Request, bool], Awaitable[Any]]
    create_agency: Callable[[Any, Request], Awaitable[Any]]
    get_agencies: Callable[..., Awaitable[Any]]
    update_agency: Callable[[str, Any, Request], Awaitable[Any]]


def build_organization_catalog_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    app_admin_role: str,
    app_user_role: str,
    group_admin_role: str,
    brand_admin_role: str,
    agency_admin_role: str,
    serialize_doc: Callable[[Any], Any],
    object_id_cls: Any,
    insert_group: Callable[..., Awaitable[str]],
    list_groups: Callable[..., Awaitable[Any]],
    find_group_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    update_group_by_id: Callable[..., Awaitable[Any]],
    delete_group_by_id: Callable[..., Awaitable[Any]],
    build_group_delete_context: Callable[..., Awaitable[Dict[str, Any]]],
    summarize_group_dependencies: Callable[..., Awaitable[Dict[str, int]]],
    format_dependency_messages: Callable[[Dict[str, int]], List[str]],
    execute_group_cascade_delete: Callable[..., Awaitable[Dict[str, int]]],
    insert_brand: Callable[..., Awaitable[str]],
    list_brands: Callable[..., Awaitable[Any]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    update_brand_by_id: Callable[..., Awaitable[Any]],
    delete_brand_by_id: Callable[..., Awaitable[Any]],
    build_brand_delete_context: Callable[..., Awaitable[Dict[str, Any]]],
    summarize_brand_dependencies: Callable[..., Awaitable[Dict[str, int]]],
    execute_brand_cascade_delete: Callable[..., Awaitable[Dict[str, int]]],
    insert_agency: Callable[..., Awaitable[str]],
    list_agencies: Callable[..., Awaitable[Any]],
    list_brands_by_ids: Callable[..., Awaitable[Any]],
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    update_agency_by_id: Callable[..., Awaitable[Any]],
    build_scope_query: Callable[[Dict[str, Any]], Dict[str, Any]],
    scope_query_has_access: Callable[[Dict[str, Any]], bool],
    validate_scope_filters: Callable[..., None],
    ensure_doc_scope_access: Callable[..., None],
    same_scope_id: Callable[[Any, Any], bool],
    is_brand_scoped_role: Callable[[Optional[str]], bool],
    is_agency_scoped_role: Callable[[Optional[str]], bool],
    resolve_logo_url_for_brand: Callable[..., Optional[str]],
    normalize_catalog_text: Callable[[Any], Optional[str]],
    resolve_agency_location: Callable[[Optional[str], Optional[str]], Dict[str, Optional[str]]],
    compose_structured_agency_address: Callable[..., Optional[str]],
    merge_optional_text: Callable[[Optional[str], Optional[str]], Optional[str]],
    merge_optional_float: Callable[[Optional[float], Any], Optional[float]],
    log_audit_event: Callable[..., Awaitable[None]],
) -> OrganizationCatalogHandlerBundle:
    async def create_group(group_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] != app_admin_role:
            raise HTTPException(status_code=403, detail="Not authorized")

        group_doc = {
            "name": group_data.name,
            "description": group_data.description,
            "created_at": datetime.now(timezone.utc),
        }
        group_id = await insert_group(db, group_doc)
        group_doc["id"] = group_id
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_group",
            entity_type="group",
            entity_id=group_id,
            group_id=group_id,
            details={"name": group_data.name, "description": group_data.description},
        )
        return serialize_doc(group_doc)

    async def get_groups(request: Request):
        current_user = await get_current_user(request)
        query: Dict[str, Any] = {}
        if current_user["role"] not in [app_admin_role, app_user_role]:
            if current_user.get("group_id"):
                query["_id"] = object_id_cls(current_user["group_id"])
            else:
                return []

        groups = await list_groups(db, query, limit=1000)
        return [serialize_doc(g) for g in groups]

    async def get_group(group_id: str, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, app_user_role]:
            if current_user.get("group_id") != group_id:
                raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")

        group = await find_group_by_id(db, group_id)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        return serialize_doc(group)

    async def update_group(group_id: str, group_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        previous = await find_group_by_id(db, group_id)
        await update_group_by_id(db, group_id, {"name": group_data.name, "description": group_data.description})
        group = await find_group_by_id(db, group_id)
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_group",
            entity_type="group",
            entity_id=group_id,
            group_id=group_id,
            details={
                "before": {
                    "name": previous.get("name") if previous else None,
                    "description": previous.get("description") if previous else None,
                },
                "after": {
                    "name": group.get("name") if group else None,
                    "description": group.get("description") if group else None,
                },
            },
        )
        return serialize_doc(group)

    async def delete_group(group_id: str, request: Request, cascade: bool = False):
        current_user = await get_current_user(request)
        if current_user["role"] != app_admin_role:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(group_id):
            raise HTTPException(status_code=400, detail="Invalid group_id")

        group = await find_group_by_id(db, group_id)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        context = await build_group_delete_context(db, group_id)
        dependency_counts = await summarize_group_dependencies(db, context)
        dependencies = format_dependency_messages(dependency_counts)
        if dependencies and not cascade:
            raise HTTPException(
                status_code=409,
                detail=f"No se puede borrar el grupo porque tiene registros relacionados: {', '.join(dependencies)}. Usa borrado en cascada para eliminar todo.",
            )

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

        if cascade:
            deleted_counts.update(await execute_group_cascade_delete(db, context))

        deleted_counts["groups"] = await delete_group_by_id(db, group_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="delete_group",
            entity_type="group",
            entity_id=group_id,
            group_id=group_id,
            details={
                "cascade": cascade,
                "deleted": deleted_counts,
                "name": group.get("name"),
            },
        )

        return {
            "message": "Group deleted",
            "cascade": cascade,
            "deleted": deleted_counts,
        }

    async def create_brand(brand_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(brand_data.group_id):
            raise HTTPException(status_code=400, detail="Invalid group_id")

        group = await find_group_by_id(db, brand_data.group_id)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        if current_user["role"] == group_admin_role:
            user_group_id = current_user.get("group_id")
            if not user_group_id or brand_data.group_id != user_group_id:
                raise HTTPException(status_code=403, detail="No tienes acceso para crear marcas fuera de tu grupo")

        brand_doc = {
            "name": brand_data.name,
            "group_id": brand_data.group_id,
            "logo_url": brand_data.logo_url,
            "created_at": datetime.now(timezone.utc),
        }
        brand_id = await insert_brand(db, brand_doc)
        brand_doc["id"] = brand_id
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_brand",
            entity_type="brand",
            entity_id=brand_id,
            group_id=brand_data.group_id,
            details={"name": brand_data.name},
        )
        serialized = serialize_doc(brand_doc)
        if not serialized.get("logo_url"):
            fallback_logo = resolve_logo_url_for_brand(serialized.get("name", ""), request)
            if fallback_logo:
                serialized["logo_url"] = fallback_logo
        return serialized

    async def get_brands(request: Request, group_id: Optional[str] = None):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        query.pop("agency_id", None)
        if is_brand_scoped_role(current_user.get("role")) or is_agency_scoped_role(current_user.get("role")):
            user_brand_id = current_user.get("brand_id")
            if not user_brand_id or not object_id_cls.is_valid(user_brand_id):
                return []
            query["_id"] = object_id_cls(user_brand_id)
            query.pop("brand_id", None)

        validate_scope_filters(current_user, group_id=group_id)
        if group_id:
            query["group_id"] = group_id

        brands = await list_brands(db, query, limit=1000)
        output: List[Dict[str, Any]] = []
        for brand in brands:
            serialized = serialize_doc(brand)
            if not serialized.get("logo_url"):
                fallback_logo = resolve_logo_url_for_brand(serialized.get("name", ""), request)
                if fallback_logo:
                    serialized["logo_url"] = fallback_logo
            output.append(serialized)
        return output

    async def update_brand(brand_id: str, brand_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role, brand_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(brand_id):
            raise HTTPException(status_code=400, detail="Invalid brand_id")
        if not object_id_cls.is_valid(brand_data.group_id):
            raise HTTPException(status_code=400, detail="Invalid group_id")

        brand = await find_brand_by_id(db, brand_id)
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")

        target_group = await find_group_by_id(db, brand_data.group_id)
        if not target_group:
            raise HTTPException(status_code=404, detail="Group not found")

        if current_user["role"] == group_admin_role:
            user_group_id = current_user.get("group_id")
            current_group_id = str(brand.get("group_id") or "")
            if (
                not user_group_id
                or current_group_id != user_group_id
                or brand_data.group_id != user_group_id
            ):
                raise HTTPException(status_code=403, detail="No tienes acceso para modificar esta marca")

        previous = brand
        await update_brand_by_id(
            db,
            brand_id,
            {"name": brand_data.name, "group_id": brand_data.group_id, "logo_url": brand_data.logo_url},
        )
        brand = await find_brand_by_id(db, brand_id)
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_brand",
            entity_type="brand",
            entity_id=brand_id,
            group_id=brand_data.group_id,
            details={
                "before": {
                    "name": previous.get("name"),
                    "group_id": previous.get("group_id"),
                    "logo_url": previous.get("logo_url"),
                },
                "after": {
                    "name": brand.get("name") if brand else None,
                    "group_id": brand.get("group_id") if brand else None,
                    "logo_url": brand.get("logo_url") if brand else None,
                },
            },
        )
        serialized = serialize_doc(brand)
        if not serialized.get("logo_url"):
            fallback_logo = resolve_logo_url_for_brand(serialized.get("name", ""), request)
            if fallback_logo:
                serialized["logo_url"] = fallback_logo
        return serialized

    async def delete_brand(brand_id: str, request: Request, cascade: bool = False):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        if not object_id_cls.is_valid(brand_id):
            raise HTTPException(status_code=400, detail="Invalid brand_id")

        brand = await find_brand_by_id(db, brand_id)
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")

        if current_user["role"] == group_admin_role:
            user_group_id = current_user.get("group_id")
            if not user_group_id or not same_scope_id(brand.get("group_id"), user_group_id):
                raise HTTPException(status_code=403, detail="No tienes acceso para borrar esta marca")

        brand_group_id = str(brand.get("group_id") or "")
        context = await build_brand_delete_context(db, brand_id)
        dependency_counts = await summarize_brand_dependencies(db, context)
        dependencies = format_dependency_messages(dependency_counts)
        if dependencies and not cascade:
            raise HTTPException(
                status_code=409,
                detail=f"No se puede borrar la marca porque tiene registros relacionados: {', '.join(dependencies)}. Usa borrado en cascada para eliminar todo.",
            )

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

        if cascade:
            deleted_counts.update(await execute_brand_cascade_delete(db, context))

        deleted_counts["brands"] = await delete_brand_by_id(db, brand_id)

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="delete_brand",
            entity_type="brand",
            entity_id=brand_id,
            group_id=brand_group_id,
            brand_id=brand_id,
            details={
                "name": brand.get("name"),
                "cascade": cascade,
                "deleted": deleted_counts,
            },
        )

        return {
            "message": "Brand deleted",
            "cascade": cascade,
            "deleted": deleted_counts,
        }

    async def create_agency(agency_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role, brand_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        brand = await find_brand_by_id(db, agency_data.brand_id)
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")

        street = normalize_catalog_text(agency_data.street)
        exterior_number = normalize_catalog_text(agency_data.exterior_number)
        interior_number = normalize_catalog_text(agency_data.interior_number)
        neighborhood = normalize_catalog_text(agency_data.neighborhood)
        municipality = normalize_catalog_text(agency_data.municipality)
        state = normalize_catalog_text(agency_data.state)
        country = normalize_catalog_text(agency_data.country) or "Mexico"

        explicit_city = normalize_catalog_text(agency_data.city) or municipality
        explicit_postal_code = normalize_catalog_text(agency_data.postal_code)
        address = normalize_catalog_text(agency_data.address) or compose_structured_agency_address(
            street=street,
            exterior_number=exterior_number,
            interior_number=interior_number,
            neighborhood=neighborhood,
            city=explicit_city,
            state=state,
            postal_code=explicit_postal_code,
            country=country,
        )
        location = resolve_agency_location(explicit_city, address)
        final_city = explicit_city or location["city"]
        final_postal_code = explicit_postal_code or location["postal_code"]

        agency_doc = {
            "name": agency_data.name,
            "brand_id": agency_data.brand_id,
            "group_id": brand["group_id"],
            "address": address,
            "city": final_city,
            "postal_code": final_postal_code,
            "street": street,
            "exterior_number": exterior_number,
            "interior_number": interior_number,
            "neighborhood": neighborhood,
            "municipality": municipality,
            "state": state,
            "country": country,
            "google_place_id": normalize_catalog_text(agency_data.google_place_id),
            "latitude": agency_data.latitude,
            "longitude": agency_data.longitude,
            "created_at": datetime.now(timezone.utc),
        }
        agency_id = await insert_agency(db, agency_doc)
        agency_doc["id"] = agency_id
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="create_agency",
            entity_type="agency",
            entity_id=agency_id,
            group_id=brand.get("group_id"),
            brand_id=agency_data.brand_id,
            agency_id=agency_id,
            details={
                "name": agency_data.name,
                "city": final_city,
                "state": state,
                "postal_code": final_postal_code,
                "address": address,
                "google_place_id": agency_doc.get("google_place_id"),
                "latitude": agency_doc.get("latitude"),
                "longitude": agency_doc.get("longitude"),
            },
        )
        return serialize_doc(agency_doc)

    async def get_agencies(request: Request, brand_id: Optional[str] = None, group_id: Optional[str] = None):
        current_user = await get_current_user(request)
        query = build_scope_query(current_user)
        if not scope_query_has_access(query):
            return []

        if is_agency_scoped_role(current_user.get("role")):
            user_agency_id = current_user.get("agency_id")
            if not user_agency_id or not object_id_cls.is_valid(user_agency_id):
                return []
            query["_id"] = object_id_cls(user_agency_id)
            query.pop("agency_id", None)

        validate_scope_filters(current_user, group_id=group_id, brand_id=brand_id)
        if group_id:
            query["group_id"] = group_id
        if brand_id:
            query["brand_id"] = brand_id

        agencies = await list_agencies(db, query, limit=1000)
        brand_ids = list(set(a.get("brand_id") for a in agencies if a.get("brand_id")))
        brands = await list_brands_by_ids(db, brand_ids, limit=1000)
        brand_map = {str(b["_id"]): b["name"] for b in brands}

        result = []
        for a in agencies:
            agency = serialize_doc(a)
            city_source = a.get("city") or a.get("municipality")
            location = resolve_agency_location(city_source, a.get("address"))
            agency["city"] = city_source or location["city"]
            agency["postal_code"] = a.get("postal_code") or location["postal_code"]
            agency["brand_name"] = brand_map.get(a.get("brand_id"), "")
            result.append(agency)

        return result

    async def update_agency(agency_id: str, agency_data: Any, request: Request):
        current_user = await get_current_user(request)
        if current_user["role"] not in [app_admin_role, group_admin_role, brand_admin_role, agency_admin_role]:
            raise HTTPException(status_code=403, detail="Not authorized")

        previous = await find_agency_by_id(db, agency_id)
        if not previous:
            raise HTTPException(status_code=404, detail="Agency not found")
        ensure_doc_scope_access(current_user, previous, detail="No tienes acceso a esta agencia")
        if current_user["role"] == agency_admin_role and agency_data.brand_id != previous.get("brand_id"):
            raise HTTPException(status_code=403, detail="Agency admin cannot move agency to another brand")

        street = merge_optional_text(agency_data.street, previous.get("street"))
        exterior_number = merge_optional_text(agency_data.exterior_number, previous.get("exterior_number"))
        interior_number = merge_optional_text(agency_data.interior_number, previous.get("interior_number"))
        neighborhood = merge_optional_text(agency_data.neighborhood, previous.get("neighborhood"))
        municipality = merge_optional_text(agency_data.municipality, previous.get("municipality"))
        state = merge_optional_text(agency_data.state, previous.get("state"))
        country = merge_optional_text(agency_data.country, previous.get("country")) or "Mexico"

        explicit_city = merge_optional_text(agency_data.city, previous.get("city")) or municipality
        explicit_postal_code = merge_optional_text(agency_data.postal_code, previous.get("postal_code"))
        address = merge_optional_text(agency_data.address, previous.get("address")) or compose_structured_agency_address(
            street=street,
            exterior_number=exterior_number,
            interior_number=interior_number,
            neighborhood=neighborhood,
            city=explicit_city,
            state=state,
            postal_code=explicit_postal_code,
            country=country,
        )
        location = resolve_agency_location(explicit_city, address)
        final_city = explicit_city or location["city"]
        final_postal_code = explicit_postal_code or location["postal_code"]
        latitude = merge_optional_float(agency_data.latitude, previous.get("latitude"))
        longitude = merge_optional_float(agency_data.longitude, previous.get("longitude"))
        google_place_id = merge_optional_text(agency_data.google_place_id, previous.get("google_place_id"))

        await update_agency_by_id(
            db,
            agency_id,
            {
                "name": agency_data.name,
                "address": address,
                "city": final_city,
                "postal_code": final_postal_code,
                "street": street,
                "exterior_number": exterior_number,
                "interior_number": interior_number,
                "neighborhood": neighborhood,
                "municipality": municipality,
                "state": state,
                "country": country,
                "google_place_id": google_place_id,
                "latitude": latitude,
                "longitude": longitude,
            },
        )
        agency = await find_agency_by_id(db, agency_id)
        await log_audit_event(
            request=request,
            current_user=current_user,
            action="update_agency",
            entity_type="agency",
            entity_id=agency_id,
            group_id=agency.get("group_id") if agency else previous.get("group_id") if previous else None,
            brand_id=agency.get("brand_id") if agency else previous.get("brand_id") if previous else None,
            agency_id=agency_id,
            details={
                "before": {
                    "name": previous.get("name") if previous else None,
                    "city": previous.get("city") if previous else None,
                    "postal_code": previous.get("postal_code") if previous else None,
                    "address": previous.get("address") if previous else None,
                    "state": previous.get("state") if previous else None,
                    "municipality": previous.get("municipality") if previous else None,
                    "google_place_id": previous.get("google_place_id") if previous else None,
                    "latitude": previous.get("latitude") if previous else None,
                    "longitude": previous.get("longitude") if previous else None,
                },
                "after": {
                    "name": agency.get("name") if agency else None,
                    "city": agency.get("city") if agency else None,
                    "postal_code": agency.get("postal_code") if agency else None,
                    "address": agency.get("address") if agency else None,
                    "state": agency.get("state") if agency else None,
                    "municipality": agency.get("municipality") if agency else None,
                    "google_place_id": agency.get("google_place_id") if agency else None,
                    "latitude": agency.get("latitude") if agency else None,
                    "longitude": agency.get("longitude") if agency else None,
                },
            },
        )
        return serialize_doc(agency)

    return OrganizationCatalogHandlerBundle(
        create_group=create_group,
        get_groups=get_groups,
        get_group=get_group,
        update_group=update_group,
        delete_group=delete_group,
        create_brand=create_brand,
        get_brands=get_brands,
        update_brand=update_brand,
        delete_brand=delete_brand,
        create_agency=create_agency,
        get_agencies=get_agencies,
        update_agency=update_agency,
    )
