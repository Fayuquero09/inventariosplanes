from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class AppConfigHelperBundle:
    get_jwt_secret: Callable[[], str]
    get_catalog_source_path: Callable[[], str]
    get_catalog_model_year: Callable[[], int]
    resolve_logo_url_for_brand: Callable[..., Optional[str]]
    resolve_logo_directory: Callable[[], Optional[Path]]


def build_app_config_helper_bundle(
    *,
    env: Dict[str, str],
    jwt_secret_default: str,
    catalog_default_source_path: str,
    catalog_default_model_year: int,
    logo_directory_env: str,
    cortex_root_default_path: str,
    get_catalog_source_path_service: Callable[..., str],
    get_catalog_model_year_service: Callable[..., int],
    resolve_logo_url_for_brand_service: Callable[..., Optional[str]],
    resolve_logo_directory_service: Callable[..., Optional[Path]],
) -> AppConfigHelperBundle:
    def get_jwt_secret() -> str:
        return env.get("JWT_SECRET", jwt_secret_default)

    def get_catalog_source_path() -> str:
        return get_catalog_source_path_service(default_source_path=catalog_default_source_path)

    def get_catalog_model_year() -> int:
        return get_catalog_model_year_service(default_model_year=catalog_default_model_year)

    def resolve_logo_url_for_brand(brand_name: str, request: Optional[Any] = None) -> Optional[str]:
        return resolve_logo_url_for_brand_service(
            brand_name,
            request=request,
            logo_directory_env=logo_directory_env,
            cortex_root_default_path=cortex_root_default_path,
        )

    def resolve_logo_directory() -> Optional[Path]:
        return resolve_logo_directory_service(
            logo_directory_env=logo_directory_env,
            cortex_root_default_path=cortex_root_default_path,
        )

    return AppConfigHelperBundle(
        get_jwt_secret=get_jwt_secret,
        get_catalog_source_path=get_catalog_source_path,
        get_catalog_model_year=get_catalog_model_year,
        resolve_logo_url_for_brand=resolve_logo_url_for_brand,
        resolve_logo_directory=resolve_logo_directory,
    )
