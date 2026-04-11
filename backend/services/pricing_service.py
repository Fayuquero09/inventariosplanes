from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from repositories.pricing_repository import list_price_bulletins_for_model


def is_price_bulletin_active(doc: Dict[str, Any], current_date_ymd: str) -> bool:
    effective_from = str(doc.get("effective_from") or "").strip()
    effective_to = str(doc.get("effective_to") or "").strip()
    if effective_from and current_date_ymd < effective_from:
        return False
    if effective_to and current_date_ymd > effective_to:
        return False
    return True


async def resolve_price_bulletin_for_model(
    db: Any,
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    model: Optional[str],
    version: Optional[str] = None,
    reference_date_ymd: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    normalized_group_id = str(group_id or "").strip()
    normalized_brand_id = str(brand_id or "").strip()
    normalized_agency_id = str(agency_id or "").strip()
    normalized_model = str(model or "").strip()
    normalized_version = str(version or "").strip() or None
    if not normalized_group_id or not normalized_brand_id or not normalized_model:
        return None

    query: Dict[str, Any] = {
        "group_id": normalized_group_id,
        "brand_id": normalized_brand_id,
        "model": normalized_model,
    }

    if normalized_version:
        query["version"] = {"$in": [normalized_version, None, ""]}
    else:
        query["version"] = {"$in": [None, ""]}

    if normalized_agency_id:
        query["$or"] = [
            {"agency_id": normalized_agency_id},
            {"agency_id": None},
        ]
    else:
        query["agency_id"] = None

    docs = await list_price_bulletins_for_model(db, query=query, limit=200)
    if not docs:
        return None

    target_date = reference_date_ymd or datetime.now(timezone.utc).date().isoformat()
    active_docs = [doc for doc in docs if is_price_bulletin_active(doc, target_date)]
    source_docs = active_docs if active_docs else docs

    def pick_by_version_preference(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None
        if normalized_version:
            for doc in candidates:
                if str(doc.get("version") or "").strip() == normalized_version:
                    return doc
            for doc in candidates:
                if not str(doc.get("version") or "").strip():
                    return doc
        return candidates[0]

    if normalized_agency_id:
        agency_docs = [doc for doc in source_docs if str(doc.get("agency_id") or "") == normalized_agency_id]
        agency_pick = pick_by_version_preference(agency_docs)
        if agency_pick:
            return agency_pick

        brand_default_docs = [doc for doc in source_docs if not str(doc.get("agency_id") or "").strip()]
        brand_default_pick = pick_by_version_preference(brand_default_docs)
        if brand_default_pick:
            return brand_default_pick

    return pick_by_version_preference(source_docs)


async def resolve_effective_sale_pricing_for_model(
    db: Any,
    *,
    group_id: Optional[str],
    brand_id: Optional[str],
    agency_id: Optional[str],
    model: Optional[str],
    version: Optional[str] = None,
    reference_date_ymd: Optional[str] = None,
    fallback_msrp: Optional[float] = None,
    to_non_negative_float: Callable[[Any, float], float],
) -> Dict[str, Any]:
    fallback_price = to_non_negative_float(fallback_msrp, 0.0)
    bulletin = await resolve_price_bulletin_for_model(
        db,
        group_id=group_id,
        brand_id=brand_id,
        agency_id=agency_id,
        model=model,
        version=version,
        reference_date_ymd=reference_date_ymd,
    )
    transaction_price = fallback_price
    brand_incentive_amount = 0.0
    dealer_incentive_amount = 0.0
    if bulletin:
        brand_incentive_amount = to_non_negative_float(bulletin.get("brand_bonus_amount"), 0.0)
        dealer_incentive_amount = to_non_negative_float(bulletin.get("dealer_bonus_amount"), 0.0)
        transaction_price_raw = bulletin.get("transaction_price")
        if transaction_price_raw is not None and to_non_negative_float(transaction_price_raw, 0.0) > 0:
            transaction_price = to_non_negative_float(transaction_price_raw, 0.0)
        else:
            msrp = bulletin.get("msrp")
            if msrp is not None and to_non_negative_float(msrp, 0.0) > 0:
                transaction_price = to_non_negative_float(msrp, 0.0)

    commission_base_price = transaction_price + brand_incentive_amount
    effective_revenue = transaction_price + brand_incentive_amount
    return {
        "configured_transaction_price": round(to_non_negative_float(transaction_price, 0.0), 2),
        "transaction_price": round(to_non_negative_float(transaction_price, 0.0), 2),
        "brand_incentive_amount": round(brand_incentive_amount, 2),
        "dealer_incentive_amount": round(dealer_incentive_amount, 2),
        "commission_base_price": round(to_non_negative_float(commission_base_price, 0.0), 2),
        "effective_revenue": round(to_non_negative_float(effective_revenue, 0.0), 2),
        "undocumented_dealer_incentive_amount": 0.0,
        "price_source": "price_bulletin",
    }


def apply_manual_sale_price_override(
    *,
    pricing: Dict[str, Any],
    supplied_sale_price: Optional[float],
    to_non_negative_float: Callable[[Any, float], float],
) -> Dict[str, Any]:
    configured_transaction_price = to_non_negative_float(
        pricing.get("configured_transaction_price", pricing.get("transaction_price")),
        0.0,
    )
    supplied_price = to_non_negative_float(supplied_sale_price, 0.0)
    brand_incentive_amount = to_non_negative_float(pricing.get("brand_incentive_amount"), 0.0)
    documented_dealer_incentive = to_non_negative_float(pricing.get("dealer_incentive_amount"), 0.0)

    if configured_transaction_price > 0 and supplied_price > 0 and supplied_price < configured_transaction_price:
        transaction_price = supplied_price
        undocumented_dealer_incentive = round(configured_transaction_price - supplied_price, 2)
        price_source = "dealer_undocumented_incentive"
    else:
        transaction_price = configured_transaction_price if configured_transaction_price > 0 else supplied_price
        undocumented_dealer_incentive = 0.0
        price_source = pricing.get("price_source") or "price_bulletin"

    transaction_price = round(to_non_negative_float(transaction_price, 0.0), 2)
    dealer_incentive_amount = round(documented_dealer_incentive + undocumented_dealer_incentive, 2)
    commission_base_price = round(transaction_price + brand_incentive_amount, 2)
    effective_revenue = round(transaction_price + brand_incentive_amount, 2)

    return {
        **pricing,
        "configured_transaction_price": round(configured_transaction_price, 2),
        "transaction_price": transaction_price,
        "brand_incentive_amount": round(brand_incentive_amount, 2),
        "dealer_incentive_amount": dealer_incentive_amount,
        "undocumented_dealer_incentive_amount": round(undocumented_dealer_incentive, 2),
        "commission_base_price": commission_base_price,
        "effective_revenue": effective_revenue,
        "price_source": price_source,
    }


def price_item_applies_to_sale(
    *,
    sale_model: Optional[str],
    sale_version: Optional[str],
    affected_exact_keys: set[str],
    affected_model_keys: set[str],
) -> bool:
    model_name = str(sale_model or "").strip()
    if not model_name:
        return False
    model_key = model_name.casefold()
    version_key = str(sale_version or "").strip().casefold()
    exact_key = f"{model_key}::{version_key}"
    if exact_key in affected_exact_keys:
        return True
    if model_key in affected_model_keys:
        return True
    return False
