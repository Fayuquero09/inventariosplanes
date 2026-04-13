from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import HTTPException, Request


@dataclass(frozen=True)
class CatalogRouteHandlers:
    get_catalog_makes: Callable[[Request], Awaitable[Dict[str, Any]]]
    get_catalog_models: Callable[..., Awaitable[Dict[str, Any]]]
    get_catalog_versions: Callable[..., Awaitable[Dict[str, Any]]]


def build_catalog_route_handlers(
    *,
    get_current_user: Callable[[Request], Awaitable[Dict[str, Any]]],
    build_catalog_tree_from_source: Callable[..., Dict[str, Any]],
    find_catalog_make: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    find_catalog_model: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    parse_catalog_price: Callable[[Any], Optional[float]],
    resolve_logo_url_for_brand: Callable[[str, Optional[Request]], Optional[str]],
) -> CatalogRouteHandlers:
    async def get_catalog_makes(request: Request) -> Dict[str, Any]:
        await get_current_user(request)
        catalog = build_catalog_tree_from_source()
        return {
            "model_year": catalog["model_year"],
            "source_last_modified": catalog["source_last_modified"],
            "items": [
                {
                    "name": make_entry["name"],
                    "models_count": len(make_entry.get("models", [])),
                    "logo_url": resolve_logo_url_for_brand(make_entry["name"], request),
                }
                for make_entry in catalog.get("makes", [])
            ],
        }

    async def get_catalog_models(
        *,
        request: Request,
        make: str,
        all_years: bool = False,
    ) -> Dict[str, Any]:
        await get_current_user(request)
        catalog = build_catalog_tree_from_source(all_years=all_years)
        make_entry = find_catalog_make(catalog, make)
        if not make_entry:
            if catalog.get("all_years"):
                raise HTTPException(status_code=404, detail="Make not found in catalog")
            raise HTTPException(
                status_code=404,
                detail=f"Make not found in catalog for model year {catalog['model_year']}",
            )

        return {
            "model_year": catalog["model_year"],
            "all_years": bool(catalog.get("all_years")),
            "available_years": catalog.get("available_years", []),
            "make": make_entry["name"],
            "items": [
                {
                    "name": model_entry["name"],
                    "versions_count": len(model_entry.get("versions", [])),
                    "min_msrp": parse_catalog_price(model_entry.get("min_msrp")),
                }
                for model_entry in make_entry.get("models", [])
            ],
        }

    async def get_catalog_versions(
        *,
        request: Request,
        make: str,
        model: str,
        all_years: bool = False,
    ) -> Dict[str, Any]:
        await get_current_user(request)
        catalog = build_catalog_tree_from_source(all_years=all_years)
        make_entry = find_catalog_make(catalog, make)
        if not make_entry:
            if catalog.get("all_years"):
                raise HTTPException(status_code=404, detail="Make not found in catalog")
            raise HTTPException(
                status_code=404,
                detail=f"Make not found in catalog for model year {catalog['model_year']}",
            )

        model_entry = find_catalog_model(make_entry, model)
        if not model_entry:
            raise HTTPException(status_code=404, detail=f"Model not found for make {make_entry['name']}")

        return {
            "model_year": catalog["model_year"],
            "all_years": bool(catalog.get("all_years")),
            "available_years": catalog.get("available_years", []),
            "make": make_entry["name"],
            "model": model_entry["name"],
            "items": model_entry.get("versions", []),
        }

    return CatalogRouteHandlers(
        get_catalog_makes=get_catalog_makes,
        get_catalog_models=get_catalog_models,
        get_catalog_versions=get_catalog_versions,
    )
