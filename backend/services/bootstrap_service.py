from datetime import datetime, timezone
from typing import Any, Callable


async def seed_admin_user(
    *,
    db: Any,
    admin_email: str,
    admin_password: str,
    app_admin_role: str,
    hash_password: Callable[[str], str],
    verify_password: Callable[[str, str], bool],
) -> str:
    existing = await db.users.find_one({"email": admin_email})
    if existing is None:
        hashed = hash_password(admin_password)
        await db.users.insert_one(
            {
                "email": admin_email,
                "password_hash": hashed,
                "name": "Admin",
                "role": app_admin_role,
                "group_id": None,
                "brand_id": None,
                "agency_id": None,
                "created_at": datetime.now(timezone.utc),
            }
        )
        return "created"

    if not verify_password(admin_password, existing.get("password_hash", "")):
        await db.users.update_one(
            {"email": admin_email},
            {"$set": {"password_hash": hash_password(admin_password)}},
        )
        return "password_updated"

    return "unchanged"


async def create_core_indexes(*, db: Any) -> None:
    await db.users.create_index("email", unique=True)
    await db.vehicles.create_index("vin")
    await db.vehicles.create_index("agency_id")
    await db.vehicles.create_index("status")
    await db.sales.create_index("seller_id")
    await db.sales.create_index("agency_id")
    await db.sales.create_index("sale_date")
    await db.audit_logs.create_index("created_at")
    await db.audit_logs.create_index("agency_id")
    await db.audit_logs.create_index("group_id")
    await db.audit_logs.create_index("actor_id")
