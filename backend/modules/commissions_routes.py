from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Request


@dataclass(frozen=True)
class CommissionsRouteHandlers:
    CommissionMatrixUpsert: Any
    CommissionRuleCreate: Any
    CommissionApprovalAction: Any
    CommissionSimulatorInput: Any
    CommissionClosureCreate: Any
    CommissionClosureApprovalAction: Any
    get_commission_matrix: Callable[..., Awaitable[Any]]
    upsert_commission_matrix: Callable[[Any, Request], Awaitable[Any]]
    create_commission_rule: Callable[[Any, Request], Awaitable[Any]]
    get_commission_rules: Callable[..., Awaitable[Any]]
    update_commission_rule: Callable[[str, Any, Request], Awaitable[Any]]
    approve_commission_rule: Callable[[str, Any, Request], Awaitable[Any]]
    delete_commission_rule: Callable[[str, Request], Awaitable[Any]]
    commission_simulator: Callable[[Any, Request], Awaitable[Any]]
    create_commission_closure: Callable[[Any, Request], Awaitable[Any]]
    get_commission_closures: Callable[..., Awaitable[Any]]
    approve_commission_closure: Callable[[str, Any, Request], Awaitable[Any]]


def register_commissions_routes(router: APIRouter, handlers: CommissionsRouteHandlers) -> None:
    @router.get("/commission-matrix")
    async def get_commission_matrix_route(request: Request, agency_id: str):
        return await handlers.get_commission_matrix(request=request, agency_id=agency_id)

    @router.put("/commission-matrix")
    async def upsert_commission_matrix_route(
        payload: handlers.CommissionMatrixUpsert,
        request: Request,
    ):
        return await handlers.upsert_commission_matrix(payload, request)

    @router.post("/commission-rules")
    async def create_commission_rule_route(
        rule_data: handlers.CommissionRuleCreate,
        request: Request,
    ):
        return await handlers.create_commission_rule(rule_data, request)

    @router.get("/commission-rules")
    async def get_commission_rules_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
    ):
        return await handlers.get_commission_rules(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
        )

    @router.put("/commission-rules/{rule_id}")
    async def update_commission_rule_route(
        rule_id: str,
        rule_data: handlers.CommissionRuleCreate,
        request: Request,
    ):
        return await handlers.update_commission_rule(rule_id, rule_data, request)

    @router.post("/commission-rules/{rule_id}/approval")
    async def approve_commission_rule_route(
        rule_id: str,
        approval: handlers.CommissionApprovalAction,
        request: Request,
    ):
        return await handlers.approve_commission_rule(rule_id, approval, request)

    @router.delete("/commission-rules/{rule_id}")
    async def delete_commission_rule_route(rule_id: str, request: Request):
        return await handlers.delete_commission_rule(rule_id, request)

    @router.post("/commission-simulator")
    async def commission_simulator_route(
        payload: handlers.CommissionSimulatorInput,
        request: Request,
    ):
        return await handlers.commission_simulator(payload, request)

    @router.post("/commission-closures")
    async def create_commission_closure_route(
        payload: handlers.CommissionClosureCreate,
        request: Request,
    ):
        return await handlers.create_commission_closure(payload, request)

    @router.get("/commission-closures")
    async def get_commission_closures_route(
        request: Request,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        seller_id: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
    ):
        return await handlers.get_commission_closures(
            request=request,
            group_id=group_id,
            brand_id=brand_id,
            agency_id=agency_id,
            seller_id=seller_id,
            month=month,
            year=year,
        )

    @router.post("/commission-closures/{closure_id}/approval")
    async def approve_commission_closure_route(
        closure_id: str,
        approval: handlers.CommissionClosureApprovalAction,
        request: Request,
    ):
        return await handlers.approve_commission_closure(closure_id, approval, request)

