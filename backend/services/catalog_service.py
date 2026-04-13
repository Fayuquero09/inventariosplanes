import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import HTTPException

from services.catalog_utils_service import normalize_catalog_text, parse_catalog_price, parse_catalog_year


_catalog_cache: Dict[str, Any] = {
    "source_path": None,
    "model_year": None,
    "all_years": False,
    "mtime": None,
    "payload": None,
}


def get_catalog_source_path(*, default_source_path: str) -> str:
    import os

    return os.environ.get("STRAPI_JATO_CATALOG_PATH", default_source_path)


def get_catalog_model_year(*, default_model_year: int) -> int:
    import os

    raw_value = os.environ.get("CATALOG_MODEL_YEAR", str(default_model_year))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return default_model_year


def build_catalog_tree_from_source(
    *,
    source_path: str,
    model_year: int,
    all_years: bool = False,
) -> Dict[str, Any]:
    target_model_year: Optional[int] = None if all_years else model_year
    source_file = Path(source_path)
    if not source_file.exists():
        raise HTTPException(status_code=500, detail=f"Catalog source file not found: {source_path}")

    source_mtime = source_file.stat().st_mtime
    if (
        _catalog_cache.get("payload") is not None
        and _catalog_cache.get("source_path") == source_path
        and _catalog_cache.get("model_year") == target_model_year
        and _catalog_cache.get("all_years") == bool(all_years)
        and _catalog_cache.get("mtime") == source_mtime
    ):
        return _catalog_cache["payload"]

    try:
        with source_file.open("r", encoding="utf-8") as f:
            source_data = json.load(f)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error reading catalog source file: {str(exc)}")

    if isinstance(source_data, dict):
        vehicles = source_data.get("vehicles", [])
    elif isinstance(source_data, list):
        vehicles = source_data
    else:
        vehicles = []

    if not isinstance(vehicles, list):
        raise HTTPException(status_code=500, detail="Invalid catalog format: vehicles list is required")

    make_map: Dict[str, Dict[str, Any]] = {}
    available_years: set[int] = set()
    matched_rows = 0

    for item in vehicles:
        if not isinstance(item, dict):
            continue

        version_data = item.get("version")
        version_data = version_data if isinstance(version_data, dict) else {}
        row_year = parse_catalog_year(version_data.get("year"))
        if row_year is not None:
            available_years.add(row_year)
        if target_model_year is not None and row_year != target_model_year:
            continue

        make_data = item.get("make")
        model_data = item.get("model")
        make_name = normalize_catalog_text(make_data.get("name") if isinstance(make_data, dict) else make_data)
        model_name = normalize_catalog_text(model_data.get("name") if isinstance(model_data, dict) else model_data)
        version_name = normalize_catalog_text(version_data.get("name"))
        pricing_data = item.get("pricing")
        pricing_data = pricing_data if isinstance(pricing_data, dict) else {}
        msrp_value = parse_catalog_price(pricing_data.get("msrp"))

        if not make_name or not model_name or not version_name:
            continue

        matched_rows += 1
        make_key = make_name.casefold()
        model_key = model_name.casefold()
        version_key = version_name.casefold()

        make_entry = make_map.setdefault(make_key, {"name": make_name, "models": {}})
        model_entry = make_entry["models"].setdefault(
            model_key,
            {"name": model_name, "versions": {}, "min_msrp": None},
        )

        existing_version = model_entry["versions"].get(version_key)
        if not existing_version:
            model_entry["versions"][version_key] = {"name": version_name, "msrp": msrp_value}
        elif msrp_value is not None:
            current_msrp = parse_catalog_price(existing_version.get("msrp"))
            if current_msrp is None or msrp_value < current_msrp:
                existing_version["msrp"] = msrp_value

        if msrp_value is not None:
            current_min_msrp = parse_catalog_price(model_entry.get("min_msrp"))
            if current_min_msrp is None or msrp_value < current_min_msrp:
                model_entry["min_msrp"] = msrp_value

    makes = []
    total_models = 0
    total_versions = 0

    for make_entry in sorted(make_map.values(), key=lambda x: x["name"].casefold()):
        models = []
        for model_entry in sorted(make_entry["models"].values(), key=lambda x: x["name"].casefold()):
            versions = [
                {
                    "name": version_entry["name"],
                    "msrp": parse_catalog_price(version_entry.get("msrp")),
                }
                for version_entry in sorted(model_entry["versions"].values(), key=lambda x: x["name"].casefold())
            ]
            total_models += 1
            total_versions += len(versions)
            models.append(
                {
                    "name": model_entry["name"],
                    "min_msrp": parse_catalog_price(model_entry.get("min_msrp")),
                    "versions": versions,
                }
            )
        makes.append({"name": make_entry["name"], "models": models})

    payload = {
        "model_year": target_model_year,
        "all_years": bool(all_years),
        "available_years": sorted(list(available_years)),
        "source_path": source_path,
        "source_last_modified": datetime.fromtimestamp(source_mtime, tz=timezone.utc).isoformat(),
        "rows_matched": matched_rows,
        "counts": {
            "makes": len(makes),
            "models": total_models,
            "versions": total_versions,
        },
        "makes": makes,
    }

    _catalog_cache["source_path"] = source_path
    _catalog_cache["model_year"] = target_model_year
    _catalog_cache["all_years"] = bool(all_years)
    _catalog_cache["mtime"] = source_mtime
    _catalog_cache["payload"] = payload
    return payload


def ensure_allowed_model_year(*, year: int, allowed_year: int) -> None:
    if year != allowed_year:
        raise HTTPException(status_code=400, detail=f"Solo se permite año modelo {allowed_year}")
