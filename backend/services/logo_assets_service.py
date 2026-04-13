import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGO_FILE_EXTENSIONS = {".png", ".svg", ".webp", ".jpg", ".jpeg"}
LOGO_SLUG_ALIASES: Dict[str, List[str]] = {
    "changan": ["changang"],
    "changang": ["changan"],
    "gac-motor": ["gac"],
    "gac": ["gac-motor"],
}

_logo_assets_cache: Dict[str, Any] = {
    "directory": None,
    "files": None,
}


def _slugify_asset_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized


def _build_logo_directory_candidates(
    *,
    logo_directory_env: str,
    cortex_root_default_path: str,
) -> List[Path]:
    candidates: List[Path] = []
    direct_logo_dir = os.environ.get(logo_directory_env)
    if direct_logo_dir:
        candidates.append(Path(direct_logo_dir))

    roots = [
        os.environ.get("CORTEX_AUTOMOTRIZ_ROOT"),
        cortex_root_default_path,
    ]
    for root in roots:
        if not root:
            continue
        base = Path(root)
        candidates.extend(
            [
                base / "cortex_frontend" / "public" / "logos",
                base / "dataframe_base_backup_20251006" / "cortex_frontend" / "public" / "logos",
                base / "dataframe_base_backup_only_db_20251006" / "cortex_frontend" / "public" / "logos",
                base / "strapi" / "cortex_frontend" / "public" / "logos",
                base / "Strapi" / "cortex_frontend" / "public" / "logos",
            ]
        )

    deduped: List[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _count_logo_files(directory: Path) -> int:
    count = 0
    try:
        for child in directory.iterdir():
            if child.is_file() and child.suffix.lower() in LOGO_FILE_EXTENSIONS:
                count += 1
    except Exception:
        return 0
    return count


def resolve_logo_directory(
    *,
    logo_directory_env: str,
    cortex_root_default_path: str,
) -> Optional[Path]:
    best_dir: Optional[Path] = None
    best_count = 0

    for candidate in _build_logo_directory_candidates(
        logo_directory_env=logo_directory_env,
        cortex_root_default_path=cortex_root_default_path,
    ):
        try:
            if not candidate.exists() or not candidate.is_dir():
                continue
        except Exception:
            continue
        candidate_count = _count_logo_files(candidate)
        if candidate_count > best_count:
            best_count = candidate_count
            best_dir = candidate

    return best_dir


def _load_logo_filename_index(
    *,
    logo_directory_env: str,
    cortex_root_default_path: str,
) -> Dict[str, str]:
    directory = resolve_logo_directory(
        logo_directory_env=logo_directory_env,
        cortex_root_default_path=cortex_root_default_path,
    )
    cached_directory = _logo_assets_cache.get("directory")
    cached_files = _logo_assets_cache.get("files")
    directory_key = str(directory) if directory else None

    if cached_directory == directory_key and isinstance(cached_files, dict):
        return cached_files

    files: Dict[str, str] = {}
    if directory:
        try:
            for child in directory.iterdir():
                if not child.is_file():
                    continue
                if child.suffix.lower() not in LOGO_FILE_EXTENSIONS:
                    continue
                files[child.name.lower()] = child.name
        except Exception:
            files = {}

    _logo_assets_cache["directory"] = directory_key
    _logo_assets_cache["files"] = files
    return files


def _resolve_logo_filename_for_brand(
    brand_name: str,
    *,
    logo_directory_env: str,
    cortex_root_default_path: str,
) -> Optional[str]:
    files = _load_logo_filename_index(
        logo_directory_env=logo_directory_env,
        cortex_root_default_path=cortex_root_default_path,
    )
    if not files:
        return None

    slug = _slugify_asset_name(brand_name)
    if not slug:
        return None

    candidate_slugs = [slug]
    candidate_slugs.extend(LOGO_SLUG_ALIASES.get(slug, []))

    for slug_candidate in candidate_slugs:
        filename_candidates = [
            f"{slug_candidate}-logo.png",
            f"{slug_candidate}.png",
            f"{slug_candidate}-logo.svg",
            f"{slug_candidate}.svg",
            f"{slug_candidate}-logo.webp",
            f"{slug_candidate}.webp",
            f"{slug_candidate}-logo.jpg",
            f"{slug_candidate}.jpg",
            f"{slug_candidate}-logo.jpeg",
            f"{slug_candidate}.jpeg",
        ]
        for filename in filename_candidates:
            resolved = files.get(filename.lower())
            if resolved:
                return resolved
    return None


def resolve_logo_url_for_brand(
    brand_name: str,
    request: Optional[Any] = None,
    *,
    logo_directory_env: str,
    cortex_root_default_path: str,
) -> Optional[str]:
    filename = _resolve_logo_filename_for_brand(
        brand_name,
        logo_directory_env=logo_directory_env,
        cortex_root_default_path=cortex_root_default_path,
    )
    if not filename:
        return None
    relative_path = f"/logos/{filename}"
    if not request:
        return relative_path
    return f"{str(request.base_url).rstrip('/')}{relative_path}"
