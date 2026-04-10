from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Query, Request


@dataclass(frozen=True)
class OrganizationCatalogRouteHandlers:
    GroupCreate: Any
    BrandCreate: Any
    AgencyCreate: Any
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
    get_catalog_makes: Callable[[Request], Awaitable[Any]]
    get_catalog_models: Callable[..., Awaitable[Any]]
    get_catalog_versions: Callable[..., Awaitable[Any]]


def register_organization_catalog_routes(router: APIRouter, handlers: OrganizationCatalogRouteHandlers) -> None:
    @router.post("/groups")
    async def create_group_route(group_data: handlers.GroupCreate, request: Request):
        return await handlers.create_group(group_data, request)

    @router.get("/groups")
    async def get_groups_route(request: Request):
        return await handlers.get_groups(request)

    @router.get("/groups/{group_id}")
    async def get_group_route(group_id: str, request: Request):
        return await handlers.get_group(group_id, request)

    @router.put("/groups/{group_id}")
    async def update_group_route(group_id: str, group_data: handlers.GroupCreate, request: Request):
        return await handlers.update_group(group_id, group_data, request)

    @router.delete("/groups/{group_id}")
    async def delete_group_route(group_id: str, request: Request, cascade: bool = Query(False)):
        return await handlers.delete_group(group_id, request, cascade)

    @router.post("/brands")
    async def create_brand_route(brand_data: handlers.BrandCreate, request: Request):
        return await handlers.create_brand(brand_data, request)

    @router.get("/brands")
    async def get_brands_route(request: Request, group_id: Optional[str] = None):
        return await handlers.get_brands(request=request, group_id=group_id)

    @router.put("/brands/{brand_id}")
    async def update_brand_route(brand_id: str, brand_data: handlers.BrandCreate, request: Request):
        return await handlers.update_brand(brand_id, brand_data, request)

    @router.delete("/brands/{brand_id}")
    async def delete_brand_route(brand_id: str, request: Request, cascade: bool = Query(False)):
        return await handlers.delete_brand(brand_id, request, cascade)

    @router.post("/agencies")
    async def create_agency_route(agency_data: handlers.AgencyCreate, request: Request):
        return await handlers.create_agency(agency_data, request)

    @router.get("/agencies")
    async def get_agencies_route(
        request: Request,
        brand_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        return await handlers.get_agencies(request=request, brand_id=brand_id, group_id=group_id)

    @router.put("/agencies/{agency_id}")
    async def update_agency_route(agency_id: str, agency_data: handlers.AgencyCreate, request: Request):
        return await handlers.update_agency(agency_id, agency_data, request)

    @router.get("/catalog/makes")
    async def get_catalog_makes_route(request: Request):
        return await handlers.get_catalog_makes(request)

    @router.get("/catalog/models")
    async def get_catalog_models_route(
        request: Request,
        make: str = Query(..., min_length=1),
        all_years: bool = Query(False),
    ):
        return await handlers.get_catalog_models(request=request, make=make, all_years=all_years)

    @router.get("/catalog/versions")
    async def get_catalog_versions_route(
        request: Request,
        make: str = Query(..., min_length=1),
        model: str = Query(..., min_length=1),
        all_years: bool = Query(False),
    ):
        return await handlers.get_catalog_versions(
            request=request,
            make=make,
            model=model,
            all_years=all_years,
        )

