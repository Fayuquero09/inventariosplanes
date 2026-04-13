from typing import Any, Dict, Optional


def normalize_catalog_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def parse_catalog_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 4:
        try:
            return int(digits[:4])
        except ValueError:
            return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_catalog_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def find_catalog_make(catalog: Dict[str, Any], make_name: str) -> Optional[Dict[str, Any]]:
    key = normalize_catalog_text(make_name)
    if not key:
        return None
    lookup = key.casefold()
    for make_entry in catalog.get("makes", []):
        if str(make_entry.get("name", "")).casefold() == lookup:
            return make_entry
    return None


def find_catalog_model(make_entry: Dict[str, Any], model_name: str) -> Optional[Dict[str, Any]]:
    key = normalize_catalog_text(model_name)
    if not key:
        return None
    lookup = key.casefold()
    for model_entry in make_entry.get("models", []):
        if str(model_entry.get("name", "")).casefold() == lookup:
            return model_entry
    return None
