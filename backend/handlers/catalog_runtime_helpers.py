from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional


@dataclass(frozen=True)
class CatalogRuntimeHelperBundle:
    normalize_catalog_text: Callable[[Any], Optional[str]]
    parse_catalog_year: Callable[[Any], Optional[int]]
    parse_catalog_price: Callable[[Any], Optional[float]]
    resolve_agency_location: Callable[[Optional[str], Optional[str]], Dict[str, Optional[str]]]
    compose_structured_agency_address: Callable[..., Optional[str]]
    merge_optional_text: Callable[[Optional[str], Optional[str]], Optional[str]]
    merge_optional_float: Callable[[Optional[float], Any], Optional[float]]
    backfill_agency_locations: Callable[[], Awaitable[Dict[str, int]]]
    build_catalog_tree_from_source: Callable[[bool], Dict[str, Any]]
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]]
    find_catalog_model: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]]
    ensure_allowed_model_year: Callable[[int], None]


def build_catalog_runtime_helper_bundle(
    *,
    db: Any,
    get_catalog_source_path: Callable[[], str],
    get_catalog_model_year: Callable[[], int],
    normalize_catalog_text_service: Callable[[Any], Optional[str]],
    parse_catalog_year_service: Callable[[Any], Optional[int]],
    parse_catalog_price_service: Callable[[Any], Optional[float]],
    resolve_agency_location_service: Callable[[Optional[str], Optional[str]], Dict[str, Optional[str]]],
    compose_structured_agency_address_service: Callable[..., Optional[str]],
    merge_optional_text_service: Callable[[Optional[str], Optional[str]], Optional[str]],
    merge_optional_float_service: Callable[[Optional[float], Any], Optional[float]],
    backfill_agency_locations_service: Callable[..., Awaitable[Dict[str, int]]],
    build_catalog_tree_from_source_service: Callable[..., Dict[str, Any]],
    find_catalog_make_service: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    find_catalog_model_service: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    ensure_allowed_model_year_service: Callable[..., None],
) -> CatalogRuntimeHelperBundle:
    normalize_catalog_text = normalize_catalog_text_service
    parse_catalog_year = parse_catalog_year_service
    parse_catalog_price = parse_catalog_price_service
    resolve_agency_location = resolve_agency_location_service
    compose_structured_agency_address = compose_structured_agency_address_service
    merge_optional_text = merge_optional_text_service
    merge_optional_float = merge_optional_float_service
    find_catalog_make = find_catalog_make_service
    find_catalog_model = find_catalog_model_service

    async def backfill_agency_locations() -> Dict[str, int]:
        return await backfill_agency_locations_service(db=db)

    def build_catalog_tree_from_source(all_years: bool = False) -> Dict[str, Any]:
        return build_catalog_tree_from_source_service(
            source_path=get_catalog_source_path(),
            model_year=get_catalog_model_year(),
            all_years=all_years,
        )

    def ensure_allowed_model_year(year: int) -> None:
        ensure_allowed_model_year_service(year=year, allowed_year=get_catalog_model_year())

    return CatalogRuntimeHelperBundle(
        normalize_catalog_text=normalize_catalog_text,
        parse_catalog_year=parse_catalog_year,
        parse_catalog_price=parse_catalog_price,
        resolve_agency_location=resolve_agency_location,
        compose_structured_agency_address=compose_structured_agency_address,
        merge_optional_text=merge_optional_text,
        merge_optional_float=merge_optional_float,
        backfill_agency_locations=backfill_agency_locations,
        build_catalog_tree_from_source=build_catalog_tree_from_source,
        find_catalog_make=find_catalog_make,
        find_catalog_model=find_catalog_model,
        ensure_allowed_model_year=ensure_allowed_model_year,
    )
