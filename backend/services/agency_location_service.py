import re
from typing import Any, Dict, List, Optional


MEX_STATE_SUFFIX_PATTERN = (
    r"(?:Aguascalientes|Ags\.?|Baja California(?: Sur)?|B\.?C\.?S?\.?|Campeche|Camp\.?|Chiapas|Chis\.?|"
    r"Chihuahua|Chih\.?|Coahuila|Coah\.?|Colima|Col\.?|Durango|Dgo\.?|Guanajuato|Gto\.?|Guerrero|Gro\.?|"
    r"Hidalgo|Hgo\.?|Jalisco|Jal\.?|México|Estado de México|Edo\.? Mex\.?|Edomex|Michoacán|Michoacan|Mich\.?|"
    r"Morelos|Mor\.?|Nayarit|Nay\.?|Nuevo León|Nuevo Leon|N\.?L\.?|Oaxaca|Oax\.?|Puebla|Pue\.?|Querétaro|Queretaro|Qro\.?|"
    r"Quintana Roo|Q\.? Roo|San Luis Potosí|San Luis Potosi|S\.?L\.?P\.?|Sinaloa|Sin\.?|Sonora|Son\.?|"
    r"Tabasco|Tab\.?|Tamaulipas|Tamps\.?|Tlaxcala|Tlax\.?|Veracruz|Ver\.?|Yucatán|Yucatan|Yuc\.?|Zacatecas|Zac\.?|"
    r"CDMX|Ciudad de México|Ciudad de Mexico)"
)

CITY_CONNECTORS = {"de", "del", "la", "las", "los", "y", "el"}
CITY_STOPWORDS = {
    "av",
    "av.",
    "avenida",
    "blvd",
    "blvd.",
    "boulevard",
    "calz",
    "calz.",
    "calzada",
    "carretera",
    "km",
    "no",
    "no.",
    "num",
    "numero",
    "número",
    "col",
    "col.",
    "colonia",
    "fracc",
    "fracc.",
    "fraccionamiento",
    "cp",
    "c.p.",
    "pte",
    "pte.",
    "oriente",
    "poniente",
    "sur",
    "norte",
    "sn",
    "s/n",
}
CITY_AREA_MARKERS = {"col", "col.", "colonia", "fracc", "fracc.", "fraccionamiento", "cp", "c.p.", "c", "p"}


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def extract_postal_code(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    text = " ".join(str(address).split())
    if not text:
        return None
    match = re.search(r"\b(?:C\.?\s*P\.?\s*:?\s*)?(\d{5})\b", text, flags=re.IGNORECASE)
    return match.group(1) if match else None


def extract_city_from_fragment(fragment: Optional[str]) -> Optional[str]:
    if not fragment:
        return None

    cleaned = " ".join(str(fragment).split())
    if not cleaned:
        return None

    raw_tokens = re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ.'-]+|\d+", cleaned)
    if not raw_tokens:
        return None

    city_tokens: List[str] = []
    reversed_tokens = list(reversed(raw_tokens))
    for index, raw in enumerate(reversed_tokens):
        token = raw.strip(" .,#;:-")
        if not token:
            continue
        lower = token.lower()
        next_token = (
            reversed_tokens[index + 1].strip(" .,#;:-").lower() if index + 1 < len(reversed_tokens) else None
        )

        if token.isdigit():
            if city_tokens:
                break
            continue

        if len(token) == 1:
            if city_tokens:
                break
            continue

        if lower in CITY_CONNECTORS and city_tokens:
            city_tokens.append(token)
            continue

        if next_token in CITY_AREA_MARKERS and city_tokens:
            break

        if lower in CITY_STOPWORDS:
            if city_tokens:
                break
            continue

        city_tokens.append(token)
        if len(city_tokens) >= 5:
            break

    if not city_tokens:
        return None

    city = " ".join(reversed(city_tokens)).strip()
    city = re.sub(r"\s{2,}", " ", city)
    return city if len(city) >= 2 else None


def infer_city_from_address(address: Optional[str]) -> Optional[str]:
    if not address:
        return None

    text = " ".join(str(address).split())
    if not text or re.search(r"\bPROXIMAMENTE\b", text, flags=re.IGNORECASE):
        return None

    if re.search(r"\bCDMX\b", text, flags=re.IGNORECASE):
        return "CDMX"

    without_cp = re.sub(r"\bC\.?\s*P\.?\s*:?\s*\d{5}\b", " ", text, flags=re.IGNORECASE)
    without_cp = re.sub(r"\bCP\s*:?\s*\d{5}\b", " ", without_cp, flags=re.IGNORECASE)
    without_cp = re.sub(r"\b\d{5}\b", " ", without_cp)
    without_cp = " ".join(without_cp.split())
    if not without_cp:
        return None

    by_comma_and_state = re.search(
        rf"([^,]+?),\s*{MEX_STATE_SUFFIX_PATTERN}\.?\s*$",
        without_cp,
        flags=re.IGNORECASE,
    )
    if by_comma_and_state:
        city = extract_city_from_fragment(by_comma_and_state.group(1))
        if city:
            return city

    by_space_and_state = re.search(
        rf"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9\s\.,#'/-]+?)\s+{MEX_STATE_SUFFIX_PATTERN}\.?\s*$",
        without_cp,
        flags=re.IGNORECASE,
    )
    if by_space_and_state:
        city = extract_city_from_fragment(by_space_and_state.group(1))
        if city:
            return city

    segments = [segment.strip(" .") for segment in without_cp.split(",") if segment.strip(" .")]
    if len(segments) >= 2:
        city = extract_city_from_fragment(segments[-2])
        if city:
            return city
    if segments:
        city = extract_city_from_fragment(segments[-1])
        if city:
            return city

    return extract_city_from_fragment(without_cp)


def resolve_agency_location(city: Optional[str], address: Optional[str]) -> Dict[str, Optional[str]]:
    resolved_city = normalize_text(city) or infer_city_from_address(address)
    resolved_postal_code = extract_postal_code(address)
    return {
        "city": resolved_city,
        "postal_code": resolved_postal_code,
    }


def compose_structured_agency_address(
    street: Optional[str],
    exterior_number: Optional[str],
    interior_number: Optional[str],
    neighborhood: Optional[str],
    city: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
    country: Optional[str],
) -> Optional[str]:
    line1_tokens = [token for token in [street, exterior_number] if token]
    line1 = " ".join(line1_tokens).strip()
    if interior_number:
        line1 = f"{line1}, Int {interior_number}" if line1 else f"Int {interior_number}"

    line2 = neighborhood or None

    city_line_tokens = [token for token in [city, state] if token]
    city_line = ", ".join(city_line_tokens)
    if postal_code:
        city_line = f"{city_line} {postal_code}".strip() if city_line else postal_code

    line4 = country or None
    parts = [part.strip() for part in [line1, line2, city_line, line4] if part and part.strip()]
    return ", ".join(parts) if parts else None


def merge_optional_text(incoming: Optional[str], previous: Optional[str]) -> Optional[str]:
    if incoming is None:
        return normalize_text(previous)
    return normalize_text(incoming)


def merge_optional_float(incoming: Optional[float], previous: Any) -> Optional[float]:
    if incoming is None:
        try:
            return float(previous) if previous is not None else None
        except (TypeError, ValueError):
            return None
    return incoming


async def backfill_agency_locations(*, db: Any) -> Dict[str, int]:
    agencies = await db.agencies.find({}, {"city": 1, "address": 1, "postal_code": 1}).to_list(20000)
    updated = 0
    filled_city = 0
    filled_postal_code = 0

    for agency in agencies:
        current_city = agency.get("city")
        current_postal_code = agency.get("postal_code")
        location = resolve_agency_location(current_city, agency.get("address"))

        update_data: Dict[str, Any] = {}
        if not current_city and location.get("city"):
            update_data["city"] = location["city"]
            filled_city += 1

        if not current_postal_code and location.get("postal_code"):
            update_data["postal_code"] = location["postal_code"]
            filled_postal_code += 1

        if update_data:
            await db.agencies.update_one({"_id": agency["_id"]}, {"$set": update_data})
            updated += 1

    return {
        "checked": len(agencies),
        "updated": updated,
        "filled_city": filled_city,
        "filled_postal_code": filled_postal_code,
    }
