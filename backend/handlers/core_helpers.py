from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence


@dataclass(frozen=True)
class CoreHelperBundle:
    serialize_doc: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]]
    to_jsonable: Callable[[Any], Any]
    log_audit_event: Callable[..., Awaitable[None]]


def build_core_helper_bundle(
    *,
    db: Any,
    object_id_cls: Any,
    write_audit_roles: Sequence[str],
) -> CoreHelperBundle:
    def serialize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if doc is None:
            return None
        result: Dict[str, Any] = {}
        for k, v in doc.items():
            if k == "_id":
                result["id"] = str(v)
            elif isinstance(v, object_id_cls):
                result[k] = str(v)
            elif isinstance(v, datetime):
                result[k] = v.isoformat()
            else:
                result[k] = v
        return result

    def to_jsonable(value: Any) -> Any:
        if isinstance(value, object_id_cls):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): to_jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [to_jsonable(item) for item in value]
        return value

    async def log_audit_event(
        request: Any,
        current_user: Optional[dict],
        action: str,
        entity_type: str,
        entity_id: Optional[str] = None,
        group_id: Optional[str] = None,
        brand_id: Optional[str] = None,
        agency_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not current_user:
            return
        actor_role = current_user.get("role")
        if actor_role not in write_audit_roles:
            return

        client_ip = None
        if request and request.client:
            client_ip = request.client.host

        audit_doc = {
            "created_at": datetime.now(timezone.utc),
            "actor_id": current_user.get("id"),
            "actor_name": current_user.get("name"),
            "actor_email": current_user.get("email"),
            "actor_role": actor_role,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "group_id": group_id,
            "brand_id": brand_id,
            "agency_id": agency_id,
            "details": to_jsonable(details or {}),
            "path": request.url.path if request and request.url else None,
            "method": request.method if request else None,
            "ip": client_ip,
        }
        await db.audit_logs.insert_one(audit_doc)

    return CoreHelperBundle(
        serialize_doc=serialize_doc,
        to_jsonable=to_jsonable,
        log_audit_event=log_audit_event,
    )
