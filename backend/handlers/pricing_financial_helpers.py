from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import HTTPException


@dataclass(frozen=True)
class PricingFinancialHelperBundle:
    resolve_financial_rate_scope: Callable[..., Awaitable[Dict[str, Optional[str]]]]
    normalize_iso_date_string: Callable[..., Optional[str]]
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]]
    apply_manual_sale_price_override: Callable[..., Dict[str, Any]]
    resolve_effective_transaction_price_for_model: Callable[..., Awaitable[float]]
    monthly_to_annual: Callable[[float], float]
    extract_rate_components_from_doc: Callable[[Optional[Dict[str, Any]]], Dict[str, Optional[float]]]
    resolve_effective_rate_components_for_scope: Callable[..., Awaitable[Dict[str, float]]]
    resolve_effective_rate_components_for_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, float]]]
    build_default_financial_rate_name: Callable[..., Awaitable[str]]


def build_pricing_financial_helper_bundle(
    *,
    db: Any,
    object_id_cls: Any,
    validate_scope_filters: Callable[..., None],
    normalize_iso_date_string_service: Callable[..., Optional[str]],
    resolve_effective_sale_pricing_for_model_service: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override_service: Callable[..., Dict[str, Any]],
    to_non_negative_float: Callable[[Any, float], float],
    monthly_to_annual_service: Callable[[float], float],
    extract_rate_components_from_doc_service: Callable[[Optional[Dict[str, Any]]], Dict[str, Optional[float]]],
    resolve_effective_rate_components_service: Callable[..., Awaitable[Dict[str, float]]],
    find_latest_financial_rate: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    build_default_financial_rate_name_service: Callable[..., Awaitable[str]],
    find_group_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_brand_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
    find_agency_by_id: Callable[..., Awaitable[Optional[Dict[str, Any]]]],
) -> PricingFinancialHelperBundle:
    async def resolve_financial_rate_scope(
        current_user: dict,
        group_id: Optional[str],
        brand_id: Optional[str],
        agency_id: Optional[str],
    ) -> Dict[str, Optional[str]]:
        normalized_group_id = str(group_id or "").strip()
        normalized_brand_id = str(brand_id or "").strip() or None
        normalized_agency_id = str(agency_id or "").strip() or None

        if not normalized_group_id:
            raise HTTPException(status_code=400, detail="group_id is required")
        if not object_id_cls.is_valid(normalized_group_id):
            raise HTTPException(status_code=400, detail="Invalid group_id")

        group = await db.groups.find_one({"_id": object_id_cls(normalized_group_id)})
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

        validate_scope_filters(
            current_user,
            group_id=normalized_group_id,
            brand_id=normalized_brand_id,
            agency_id=normalized_agency_id,
        )

        if normalized_brand_id:
            if not object_id_cls.is_valid(normalized_brand_id):
                raise HTTPException(status_code=400, detail="Invalid brand_id")
            brand = await db.brands.find_one({"_id": object_id_cls(normalized_brand_id)})
            if not brand:
                raise HTTPException(status_code=404, detail="Brand not found")
            if str(brand.get("group_id") or "") != normalized_group_id:
                raise HTTPException(status_code=400, detail="Brand does not belong to selected group")

        if normalized_agency_id:
            if not object_id_cls.is_valid(normalized_agency_id):
                raise HTTPException(status_code=400, detail="Invalid agency_id")
            agency = await db.agencies.find_one({"_id": object_id_cls(normalized_agency_id)})
            if not agency:
                raise HTTPException(status_code=404, detail="Agency not found")
            if str(agency.get("group_id") or "") != normalized_group_id:
                raise HTTPException(status_code=400, detail="Agency does not belong to selected group")

            agency_brand_id = str(agency.get("brand_id") or "")
            if normalized_brand_id:
                if agency_brand_id and agency_brand_id != normalized_brand_id:
                    raise HTTPException(status_code=400, detail="Agency does not belong to selected brand")
            else:
                normalized_brand_id = agency_brand_id or None

        return {
            "group_id": normalized_group_id,
            "brand_id": normalized_brand_id,
            "agency_id": normalized_agency_id,
        }

    def normalize_iso_date_string(value: Optional[str], *, field_name: str, required: bool = False) -> Optional[str]:
        return normalize_iso_date_string_service(value, field_name=field_name, required=required)

    async def resolve_effective_sale_pricing_for_model(
        *,
        group_id: Optional[str],
        brand_id: Optional[str],
        agency_id: Optional[str],
        model: Optional[str],
        version: Optional[str] = None,
        reference_date_ymd: Optional[str] = None,
        fallback_msrp: Optional[float] = None,
    ) -> Dict[str, Any]:
        return await resolve_effective_sale_pricing_for_model_service(
            db,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            model=model,
            version=version,
            reference_date_ymd=reference_date_ymd,
            fallback_msrp=fallback_msrp,
            to_non_negative_float=to_non_negative_float,
        )

    def apply_manual_sale_price_override(
        pricing: Dict[str, Any],
        supplied_sale_price: Optional[float],
    ) -> Dict[str, Any]:
        return apply_manual_sale_price_override_service(
            pricing=pricing,
            supplied_sale_price=supplied_sale_price,
            to_non_negative_float=to_non_negative_float,
        )

    async def resolve_effective_transaction_price_for_model(
        *,
        group_id: Optional[str],
        brand_id: Optional[str],
        agency_id: Optional[str],
        model: Optional[str],
        version: Optional[str] = None,
        reference_date_ymd: Optional[str] = None,
        fallback_msrp: Optional[float] = None,
    ) -> float:
        pricing = await resolve_effective_sale_pricing_for_model_service(
            db,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            model=model,
            version=version,
            reference_date_ymd=reference_date_ymd,
            fallback_msrp=fallback_msrp,
            to_non_negative_float=to_non_negative_float,
        )
        return to_non_negative_float(pricing.get("transaction_price"), 0.0)

    def monthly_to_annual(rate_monthly_pct: float) -> float:
        return monthly_to_annual_service(rate_monthly_pct)

    def extract_rate_components_from_doc(rate_doc: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
        return extract_rate_components_from_doc_service(rate_doc)

    async def resolve_effective_rate_components_for_scope(
        *,
        group_id: Any,
        brand_id: Any,
        agency_id: Any,
    ) -> Dict[str, float]:
        return await resolve_effective_rate_components_service(
            db,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            find_latest_financial_rate=find_latest_financial_rate,
            extract_rate_components_from_doc=extract_rate_components_from_doc,
        )

    async def resolve_effective_rate_components_for_vehicle(vehicle: Dict[str, Any]) -> Dict[str, float]:
        return await resolve_effective_rate_components_for_scope(
            group_id=vehicle.get("group_id"),
            brand_id=vehicle.get("brand_id"),
            agency_id=vehicle.get("agency_id"),
        )

    async def build_default_financial_rate_name(
        group_id: Optional[str],
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
    ) -> str:
        return await build_default_financial_rate_name_service(
            db,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            find_group_by_id=find_group_by_id,
            find_brand_by_id=find_brand_by_id,
            find_agency_by_id=find_agency_by_id,
        )

    return PricingFinancialHelperBundle(
        resolve_financial_rate_scope=resolve_financial_rate_scope,
        normalize_iso_date_string=normalize_iso_date_string,
        resolve_effective_sale_pricing_for_model=resolve_effective_sale_pricing_for_model,
        apply_manual_sale_price_override=apply_manual_sale_price_override,
        resolve_effective_transaction_price_for_model=resolve_effective_transaction_price_for_model,
        monthly_to_annual=monthly_to_annual,
        extract_rate_components_from_doc=extract_rate_components_from_doc,
        resolve_effective_rate_components_for_scope=resolve_effective_rate_components_for_scope,
        resolve_effective_rate_components_for_vehicle=resolve_effective_rate_components_for_vehicle,
        build_default_financial_rate_name=build_default_financial_rate_name,
    )
