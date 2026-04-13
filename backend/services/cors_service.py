from typing import List, Optional


def normalize_origin(origin: Optional[str]) -> Optional[str]:
    if not origin:
        return None
    value = origin.strip().rstrip("/")
    return value or None


def build_allowed_origins(frontend_url: Optional[str]) -> List[str]:
    origins = {
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    }
    frontend_origin = normalize_origin(frontend_url)
    if frontend_origin:
        origins.add(frontend_origin)
    return sorted(origins)
