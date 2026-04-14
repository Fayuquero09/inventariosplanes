from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass(frozen=True)
class CommissionCatalogHelperBundle:
    normalize_commission_matrix_models: Callable[[Optional[List[Dict[str, Any]]]], List[Dict[str, Any]]]
    get_catalog_models_for_brand: Callable[[Optional[str]], List[Dict[str, Any]]]
    build_matrix_models_response: Callable[[List[Dict[str, Any]], List[Dict[str, Any]], float], List[Dict[str, Any]]]


def build_commission_catalog_helper_bundle(
    *,
    default_plant_share_pct: float,
    normalize_commission_matrix_models_service: Callable[..., List[Dict[str, Any]]],
    get_catalog_models_for_brand_service: Callable[..., List[Dict[str, Any]]],
    build_matrix_models_response_service: Callable[..., List[Dict[str, Any]]],
    build_catalog_tree_from_source: Callable[[bool], Dict[str, Any]],
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_catalog_price: Callable[[Any], Optional[float]],
) -> CommissionCatalogHelperBundle:
    def normalize_commission_matrix_models(models: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return normalize_commission_matrix_models_service(
            models,
            default_plant_share_pct=default_plant_share_pct,
        )

    def get_catalog_models_for_brand(brand_name: Optional[str]) -> List[Dict[str, Any]]:
        return get_catalog_models_for_brand_service(
            brand_name,
            build_catalog_tree_from_source=build_catalog_tree_from_source,
            find_catalog_make=find_catalog_make,
            parse_catalog_price=parse_catalog_price,
        )

    def build_matrix_models_response(
        catalog_models: List[Dict[str, Any]],
        overrides: List[Dict[str, Any]],
        default_percentage: float,
    ) -> List[Dict[str, Any]]:
        return build_matrix_models_response_service(
            catalog_models=catalog_models,
            overrides=overrides,
            default_percentage=default_percentage,
            default_plant_share_pct=default_plant_share_pct,
        )

    return CommissionCatalogHelperBundle(
        normalize_commission_matrix_models=normalize_commission_matrix_models,
        get_catalog_models_for_brand=get_catalog_models_for_brand,
        build_matrix_models_response=build_matrix_models_response,
    )
