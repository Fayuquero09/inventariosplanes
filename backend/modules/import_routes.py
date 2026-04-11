from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, File, Request, UploadFile


@dataclass(frozen=True)
class ImportRouteHandlers:
    import_organization: Callable[[Request, UploadFile], Awaitable[Any]]
    import_vehicles: Callable[[Request, UploadFile], Awaitable[Any]]
    import_sales: Callable[[Request, UploadFile], Awaitable[Any]]


def register_import_routes(router: APIRouter, handlers: ImportRouteHandlers) -> None:
    @router.post("/import/organization")
    async def import_organization_route(request: Request, file: UploadFile = File(...)):
        return await handlers.import_organization(request, file)

    @router.post("/import/vehicles")
    async def import_vehicles_route(request: Request, file: UploadFile = File(...)):
        return await handlers.import_vehicles(request, file)

    @router.post("/import/sales")
    async def import_sales_route(request: Request, file: UploadFile = File(...)):
        return await handlers.import_sales(request, file)
