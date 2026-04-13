import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware


def create_app(*, title: str, session_secret: str) -> FastAPI:
    app = FastAPI(title=title)
    app.add_middleware(SessionMiddleware, secret_key=session_secret)
    return app


def mount_brand_logos(
    *,
    app: FastAPI,
    resolve_logo_directory: Callable[[], Optional[Path]],
    logger: Any,
    logo_directory_env: str,
) -> None:
    resolved_logo_dir = resolve_logo_directory()
    if resolved_logo_dir:
        app.mount("/logos", StaticFiles(directory=str(resolved_logo_dir)), name="brand-logos")
        logger.info("Brand logos directory mounted: %s", resolved_logo_dir)
    else:
        logger.warning(
            "Brand logos directory not found. Configure %s or place logos under cortex_frontend/public/logos.",
            logo_directory_env,
        )


def include_api_router(*, app: FastAPI, api_router: Any) -> None:
    app.include_router(api_router)


def configure_cors(
    *,
    app: FastAPI,
    frontend_url: Optional[str],
    build_allowed_origins: Callable[[Optional[str]], Any],
) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_credentials=True,
        allow_origins=build_allowed_origins(frontend_url),
        allow_methods=["*"],
        allow_headers=["*"],
    )


async def run_startup(
    *,
    db: Any,
    logger: Any,
    create_core_indexes: Callable[..., Awaitable[None]],
    backfill_agency_locations: Callable[[], Awaitable[Dict[str, int]]],
    seed_admin_user: Callable[..., Awaitable[str]],
    default_admin_email: str,
    default_admin_password: str,
    app_admin_role: str,
    hash_password: Callable[[str], str],
    verify_password: Callable[[str, str], bool],
) -> None:
    await create_core_indexes(db=db)
    backfill_summary = await backfill_agency_locations()
    logger.info(
        "Agency location backfill: checked=%s updated=%s city=%s postal_code=%s",
        backfill_summary["checked"],
        backfill_summary["updated"],
        backfill_summary["filled_city"],
        backfill_summary["filled_postal_code"],
    )
    admin_email = os.environ.get("ADMIN_EMAIL", default_admin_email)
    admin_password = os.environ.get("ADMIN_PASSWORD", default_admin_password)
    seed_status = await seed_admin_user(
        db=db,
        admin_email=admin_email,
        admin_password=admin_password,
        app_admin_role=app_admin_role,
        hash_password=hash_password,
        verify_password=verify_password,
    )
    if seed_status == "created":
        logger.info("Admin user created: %s", admin_email)
    elif seed_status == "password_updated":
        logger.info("Admin password updated: %s", admin_email)
    logger.info("AutoConnect API started")


async def run_shutdown(*, client: Any) -> None:
    client.close()
