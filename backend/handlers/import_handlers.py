from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Sequence

from fastapi import File, HTTPException, Request, UploadFile


@dataclass(frozen=True)
class ImportHandlerBundle:
    import_organization: Callable[[Request, UploadFile], Awaitable[Any]]
    import_vehicles: Callable[[Request, UploadFile], Awaitable[Any]]
    import_sales: Callable[[Request, UploadFile], Awaitable[Any]]


def build_import_route_handlers(
    *,
    db: Any,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    import_organization_roles: Sequence[str],
    import_vehicle_sales_roles: Sequence[str],
    import_organization_from_excel: Callable[..., Awaitable[Dict[str, Any]]],
    import_vehicles_from_file: Callable[..., Awaitable[Dict[str, Any]]],
    import_sales_from_file: Callable[..., Awaitable[Dict[str, Any]]],
    resolve_agency_location: Callable[..., Dict[str, Any]],
    hash_password: Callable[[str], str],
    get_catalog_model_year: Callable[[], int],
    calculate_commission: Callable[..., Awaitable[float]],
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override: Callable[..., Dict[str, Any]],
    extract_active_aging_incentive_plan: Callable[..., Any],
    apply_aging_plan_to_effective_pricing: Callable[..., Any],
    to_non_negative_float: Callable[[Any, float], float],
    log_audit_event: Callable[..., Awaitable[None]],
) -> ImportHandlerBundle:
    async def import_organization(request: Request, file: UploadFile = File(...)):
        current_user = await get_current_user(request)
        if current_user["role"] not in import_organization_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        content = await file.read()
        response = await import_organization_from_excel(
            db,
            current_user=current_user,
            filename=file.filename,
            content=content,
            resolve_agency_location=resolve_agency_location,
            hash_password=hash_password,
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="import_organization",
            entity_type="organization_import",
            group_id=current_user.get("group_id"),
            details={
                "filename": file.filename,
                "summary": response.get("summary", {}),
                "errors_count": len(response.get("errors", [])),
            },
        )
        return response

    async def import_vehicles(request: Request, file: UploadFile = File(...)):
        current_user = await get_current_user(request)
        if current_user["role"] not in import_vehicle_sales_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        content = await file.read()
        response = await import_vehicles_from_file(
            db,
            filename=file.filename,
            content=content,
            allowed_model_year=get_catalog_model_year(),
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="import_vehicles",
            entity_type="vehicle_import",
            group_id=current_user.get("group_id"),
            details={
                "filename": file.filename,
                "imported": response.get("imported", 0),
                "total_rows": response.get("total_rows", 0),
                "errors_count": len(response.get("errors", [])),
            },
        )
        return response

    async def import_sales(request: Request, file: UploadFile = File(...)):
        current_user = await get_current_user(request)
        if current_user["role"] not in import_vehicle_sales_roles:
            raise HTTPException(status_code=403, detail="Not authorized")

        content = await file.read()
        response = await import_sales_from_file(
            db,
            filename=file.filename,
            content=content,
            calculate_commission=calculate_commission,
            resolve_effective_sale_pricing_for_model=resolve_effective_sale_pricing_for_model,
            apply_manual_sale_price_override=apply_manual_sale_price_override,
            extract_active_aging_incentive_plan=extract_active_aging_incentive_plan,
            apply_aging_plan_to_effective_pricing=apply_aging_plan_to_effective_pricing,
            to_non_negative_float=to_non_negative_float,
        )

        await log_audit_event(
            request=request,
            current_user=current_user,
            action="import_sales",
            entity_type="sales_import",
            group_id=current_user.get("group_id"),
            details={
                "filename": file.filename,
                "imported": response.get("imported", 0),
                "total_rows": response.get("total_rows", 0),
                "errors_count": len(response.get("errors", [])),
            },
        )
        return response

    return ImportHandlerBundle(
        import_organization=import_organization,
        import_vehicles=import_vehicles,
        import_sales=import_sales,
    )
