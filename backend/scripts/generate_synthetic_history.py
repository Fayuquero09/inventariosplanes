#!/usr/bin/env python3
"""
Generate synthetic historical sales/vehicles data for AutoConnect.

Goals:
- Use real MSRP prices from Strapi/JATO catalog JSON.
- Create a 24-month synthetic history for trends/KPIs.
- Use sales files from ~/Downloads as demand/seasonality hints when available.
- Keep data traceable via synthetic_source + synthetic_seed_id markers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import bcrypt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient


CATALOG_DEFAULT_SOURCE_PATH = (
    "/Users/Fernando.Molina/cortex-automotriz/strapi/data/jato/latest-jato.es-mx.all-2026-2024.json"
)
CATALOG_DEFAULT_MODEL_YEAR = 2026
SYNTHETIC_SOURCE = "strapi_prices_historical_24m"
SAN_JUAN_GROUP_NAME = "Grupo San Juan"
SAN_JUAN_BRAND_AGENCY_PLAN: List[Tuple[str, int]] = [
    ("Ford", 3),
    ("GAC", 2),
    ("Nissan", 5),
    ("Hyundai", 5),
]

MONTH_ALIASES = {
    "ENE": 1,
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "ABR": 4,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DIC": 12,
    "DEC": 12,
}

BRAND_ALIASES = {
    "buickgmc": "gmc",
    "bmwmotorrad": "bmw",
    "omodajaecoo": "omoda",
    "gacmotor": "gacmotor",
    "gac": "gacmotor",
    "stellantis": "peugeot",
    "nissan": "nissan",
}

# VIN generation profiles per make key (normalized).
# NOTE: These WMIs are representative for synthetic testing and can be refined
# with OEM/dealer-specific mappings later.
BRAND_VIN_PROFILES: Dict[str, Dict[str, str]] = {
    "ford": {"wmi": "3FA", "plant": "A"},
    "nissan": {"wmi": "3N1", "plant": "M"},
    "hyundai": {"wmi": "KMH", "plant": "U"},
    "kia": {"wmi": "KNA", "plant": "5"},
    "gacmotor": {"wmi": "LMG", "plant": "G"},
    "gac": {"wmi": "LMG", "plant": "G"},
    "gmc": {"wmi": "1GT", "plant": "Z"},
    "buickgmc": {"wmi": "1G4", "plant": "Z"},
    "chevrolet": {"wmi": "3G1", "plant": "R"},
    "mazda": {"wmi": "JM1", "plant": "L"},
    "bmw": {"wmi": "WBA", "plant": "F"},
    "changan": {"wmi": "LS5", "plant": "C"},
    "isuzu": {"wmi": "JAL", "plant": "P"},
    "peugeot": {"wmi": "VR3", "plant": "D"},
    "lincoln": {"wmi": "5LM", "plant": "A"},
    "harleydavidson": {"wmi": "1HD", "plant": "Y"},
    "infiniti": {"wmi": "JN1", "plant": "N"},
    "foton": {"wmi": "LVA", "plant": "B"},
    "ktm": {"wmi": "VBK", "plant": "K"},
}
DEFAULT_VIN_PROFILE = {"wmi": "3FA", "plant": "A"}

VIN_TRANSLITERATION: Dict[str, int] = {
    **{str(n): n for n in range(10)},
    "A": 1,
    "B": 2,
    "C": 3,
    "D": 4,
    "E": 5,
    "F": 6,
    "G": 7,
    "H": 8,
    "J": 1,
    "K": 2,
    "L": 3,
    "M": 4,
    "N": 5,
    "P": 7,
    "R": 9,
    "S": 2,
    "T": 3,
    "U": 4,
    "V": 5,
    "W": 6,
    "X": 7,
    "Y": 8,
    "Z": 9,
}
VIN_CHECK_DIGIT_WEIGHTS = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]
VIN_YEAR_CODES = "ABCDEFGHJKLMNPRSTVWXY123456789"
VIN_ALLOWED_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"

SELLER_FIRST_NAMES = [
    "Luis",
    "Ana",
    "Carlos",
    "Fernanda",
    "Jorge",
    "Sofia",
    "Ricardo",
    "Mariana",
    "Diego",
    "Paola",
]

SELLER_LAST_NAMES = [
    "Martinez",
    "Hernandez",
    "Lopez",
    "Garcia",
    "Rivera",
    "Vargas",
    "Ramos",
    "Castillo",
    "Ortega",
    "Navarro",
]

VEHICLE_COLORS = [
    "Blanco",
    "Negro",
    "Gris",
    "Plata",
    "Rojo",
    "Azul",
]


@dataclass
class DemandSignals:
    make_avg_units: Dict[str, float]
    seasonality_by_month: Dict[int, float]
    source_files: List[str]
    fallback_avg_units: float


def normalize_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]", "", text)


def parse_month_col(column_name: str) -> Optional[Tuple[int, int]]:
    match = re.match(r"^([A-Za-z]{3})_(\d{2})$", str(column_name).strip())
    if not match:
        return None
    month_token = match.group(1).upper()
    month = MONTH_ALIASES.get(month_token)
    if not month:
        return None
    year = 2000 + int(match.group(2))
    return year, month


def month_start(year: int, month: int) -> datetime:
    return datetime(year, month, 1, tzinfo=timezone.utc)


def target_months_window(months: int) -> List[Tuple[int, int]]:
    now = datetime.now(timezone.utc)
    current = month_start(now.year, now.month)
    out: List[Tuple[int, int]] = []
    cursor = current
    for _ in range(months):
        out.append((cursor.year, cursor.month))
        if cursor.month == 1:
            cursor = month_start(cursor.year - 1, 12)
        else:
            cursor = month_start(cursor.year, cursor.month - 1)
    out.reverse()
    return out


def load_catalog_variants(catalog_path: Path, model_year: int) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog_path}")

    with catalog_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    vehicles = data.get("vehicles", []) if isinstance(data, dict) else data
    if not isinstance(vehicles, list):
        raise ValueError("Catalog JSON does not contain a valid vehicles list")

    by_make: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    all_variants: List[Dict[str, Any]] = []

    for row in vehicles:
        if not isinstance(row, dict):
            continue

        make_data = row.get("make", {})
        model_data = row.get("model", {})
        version_data = row.get("version", {})
        pricing = row.get("pricing", {})

        make_name = (make_data.get("name") if isinstance(make_data, dict) else make_data) or ""
        model_name = (model_data.get("name") if isinstance(model_data, dict) else model_data) or ""
        version_name = (version_data.get("name") if isinstance(version_data, dict) else version_data) or "Base"
        row_year = version_data.get("year") if isinstance(version_data, dict) else None
        msrp = pricing.get("msrp") if isinstance(pricing, dict) else None

        if not make_name or not model_name or not msrp:
            continue
        try:
            price_value = float(msrp)
        except (TypeError, ValueError):
            continue
        if price_value <= 0:
            continue

        if row_year is not None:
            try:
                if int(row_year) != int(model_year):
                    continue
            except (TypeError, ValueError):
                continue

        variant = {
            "make_name": str(make_name).strip(),
            "model": str(model_name).strip(),
            "version": str(version_name).strip() or "Base",
            "year": int(model_year),
            "msrp": price_value,
        }
        make_key = normalize_key(variant["make_name"])
        by_make[make_key].append(variant)
        all_variants.append(variant)

    if not all_variants:
        raise ValueError("No valid 2026-priced variants found in catalog")

    return dict(by_make), all_variants


def detect_reference_sales_files(downloads_dir: Path) -> List[Path]:
    candidates: List[Path] = []

    preferred = [
        downloads_dir / "ventas_mensuales_2324_enriquecido.csv",
        downloads_dir / "ventas_mensuales_2025_actualizado.xlsx",
    ]
    for path in preferred:
        if path.exists():
            candidates.append(path)

    if not candidates:
        for pattern in ["ventas_mensuales_*2324*.csv", "ventas_mensuales_2025*.xlsx"]:
            matches = sorted(downloads_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
            if matches:
                candidates.append(matches[0])

    return candidates


def read_sales_reference(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file type: {path}")


def build_demand_signals(downloads_dir: Path) -> DemandSignals:
    files = detect_reference_sales_files(downloads_dir)
    make_month_units: Dict[Tuple[str, int, int], float] = defaultdict(float)
    month_totals: Dict[Tuple[int, int], float] = defaultdict(float)
    source_files: List[str] = []

    for file_path in files:
        try:
            df = read_sales_reference(file_path)
        except Exception:
            continue

        make_col = None
        for candidate in ["Make", "make", "Marca", "marca"]:
            if candidate in df.columns:
                make_col = candidate
                break
        if not make_col:
            continue

        month_columns: Dict[str, Tuple[int, int]] = {}
        for column in df.columns:
            parsed = parse_month_col(str(column))
            if parsed:
                month_columns[str(column)] = parsed
        if not month_columns:
            continue

        source_files.append(str(file_path))

        for _, row in df.iterrows():
            make_key = normalize_key(row.get(make_col))
            if not make_key:
                continue
            for col, (year, month) in month_columns.items():
                value = pd.to_numeric(row.get(col), errors="coerce")
                if pd.isna(value):
                    continue
                units = max(float(value), 0.0)
                make_month_units[(make_key, year, month)] += units
                month_totals[(year, month)] += units

    make_to_values: Dict[str, List[float]] = defaultdict(list)
    for (make_key, _, _), units in make_month_units.items():
        make_to_values[make_key].append(units)

    make_avg_units = {
        make_key: (sum(values) / len(values) if values else 0.0)
        for make_key, values in make_to_values.items()
    }

    by_month_number: Dict[int, List[float]] = defaultdict(list)
    for (_, month), total in month_totals.items():
        by_month_number[month].append(total)
    month_avg = {
        month: (sum(values) / len(values) if values else 0.0)
        for month, values in by_month_number.items()
    }

    positive_month_values = [value for value in month_avg.values() if value > 0]
    global_month_avg = sum(positive_month_values) / len(positive_month_values) if positive_month_values else 1.0
    seasonality_by_month = {
        month: (month_avg.get(month, global_month_avg) / global_month_avg if global_month_avg else 1.0)
        for month in range(1, 13)
    }

    positive_make_values = sorted(value for value in make_avg_units.values() if value > 0)
    if positive_make_values:
        median_idx = len(positive_make_values) // 2
        fallback_avg_units = positive_make_values[median_idx]
    else:
        fallback_avg_units = 80.0

    return DemandSignals(
        make_avg_units=make_avg_units,
        seasonality_by_month=seasonality_by_month,
        source_files=source_files,
        fallback_avg_units=fallback_avg_units,
    )


def resolve_make_key(brand_name: str, catalog_make_keys: Sequence[str]) -> Optional[str]:
    if not brand_name:
        return None
    normalized = normalize_key(brand_name)
    catalog_set = set(catalog_make_keys)

    if normalized in catalog_set:
        return normalized

    alias = BRAND_ALIASES.get(normalized)
    if alias:
        alias_key = normalize_key(alias)
        if alias_key in catalog_set:
            return alias_key

    # Basic containment fallback.
    for make_key in catalog_make_keys:
        if normalized and (normalized in make_key or make_key in normalized):
            return make_key
    return None


def make_seed_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def vin_year_code(year: int) -> str:
    # VIN year cycle starts in 1980 and repeats every 30 years.
    if year < 1980:
        year = 1980
    index = (year - 1980) % len(VIN_YEAR_CODES)
    return VIN_YEAR_CODES[index]


def calculate_vin_check_digit(vin: str) -> str:
    if len(vin) != 17:
        raise ValueError("VIN must have 17 characters to calculate check digit")
    total = 0
    for index, char in enumerate(vin):
        value = VIN_TRANSLITERATION.get(char)
        if value is None:
            raise ValueError(f"Invalid VIN character for checksum: {char}")
        total += value * VIN_CHECK_DIGIT_WEIGHTS[index]
    remainder = total % 11
    return "X" if remainder == 10 else str(remainder)


def _vin_profile_for_make(make_key: Optional[str]) -> Dict[str, str]:
    if not make_key:
        return DEFAULT_VIN_PROFILE
    normalized = normalize_key(make_key)
    return BRAND_VIN_PROFILES.get(normalized, DEFAULT_VIN_PROFILE)


def _build_vin_vds(make_key: Optional[str], seed_id: str, counter: int) -> str:
    digest = hashlib.sha1(f"{normalize_key(make_key)}|{seed_id}|{counter}".encode("utf-8")).digest()
    chars: List[str] = []
    for byte in digest:
        chars.append(VIN_ALLOWED_CHARS[byte % len(VIN_ALLOWED_CHARS)])
        if len(chars) == 5:
            break
    return "".join(chars)


def build_vin(seed_id: str, counter: int, make_key: Optional[str], year: int) -> str:
    profile = _vin_profile_for_make(make_key)
    wmi = profile["wmi"].upper()
    plant = profile["plant"].upper()[:1] or "A"
    vds = _build_vin_vds(make_key, seed_id, counter)
    year_code = vin_year_code(int(year))
    serial = f"{counter % 1_000_000:06d}"

    # Positions:
    # 1-3 WMI, 4-8 VDS, 9 check digit, 10 year, 11 plant, 12-17 serial.
    vin_without_check = f"{wmi}{vds}0{year_code}{plant}{serial}"
    check_digit = calculate_vin_check_digit(vin_without_check)
    vin = f"{vin_without_check[:8]}{check_digit}{vin_without_check[9:]}"
    return vin


def create_seller_docs_for_agency(agency: Dict[str, Any], seed_id: str, start_index: int, count: int) -> List[Dict[str, Any]]:
    docs = []
    for i in range(count):
        idx = start_index + i
        first = SELLER_FIRST_NAMES[idx % len(SELLER_FIRST_NAMES)]
        last = SELLER_LAST_NAMES[(idx // len(SELLER_FIRST_NAMES)) % len(SELLER_LAST_NAMES)]
        full_name = f"{first} {last}"
        local_agency = normalize_key(agency.get("name", "agency"))[:10] or "agency"
        email = f"seller.{seed_id}.{local_agency}.{idx}@autoconnect.local"
        docs.append(
            {
                "email": email,
                "password_hash": bcrypt.hashpw(b"Seller123!", bcrypt.gensalt()).decode("utf-8"),
                "name": full_name,
                "position": "Vendedor",
                "role": "seller",
                "group_id": agency["group_id"],
                "brand_id": agency["brand_id"],
                "agency_id": agency["id"],
                "created_at": datetime.now(timezone.utc),
                "synthetic_source": SYNTHETIC_SOURCE,
                "synthetic_seed_id": seed_id,
            }
        )
    return docs


def build_seller_model_focus_map(
    seller_ids: Sequence[str],
    model_pool: Sequence[str],
    rng: np.random.Generator,
) -> Dict[str, set[str]]:
    models = sorted({str(model).strip() for model in model_pool if str(model).strip()})
    focus_map: Dict[str, set[str]] = {}
    if not seller_ids:
        return focus_map
    if not models:
        for seller_id in seller_ids:
            focus_map[str(seller_id)] = set()
        return focus_map

    for idx, seller_id in enumerate(seller_ids):
        seller_id_str = str(seller_id)
        if len(models) <= 2:
            focus_map[seller_id_str] = set(models)
            continue

        # 2-3 modelos foco por vendedor para inducir especialización.
        focus_size = 2 if (idx % 3) else 3
        focus_size = min(focus_size, len(models))
        chosen = rng.choice(models, size=focus_size, replace=False).tolist()
        focus_map[seller_id_str] = set(chosen)

    return focus_map


def parse_brand_plan(raw_value: str) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for token in str(raw_value or "").split(","):
        item = token.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"Invalid brand plan token '{item}'. Expected format Brand:Count")
        brand, count_text = item.split(":", 1)
        brand = brand.strip()
        try:
            count = int(count_text.strip())
        except ValueError as exc:
            raise ValueError(f"Invalid agency count for brand '{brand}': {count_text}") from exc
        if not brand or count <= 0:
            raise ValueError(f"Invalid brand plan token '{item}'")
        out.append((brand, count))
    if not out:
        raise ValueError("Brand plan cannot be empty")
    return out


def ensure_group_and_structure(
    db: Any,
    group_name: str,
    brand_agency_plan: Sequence[Tuple[str, int]],
    seed_id: str,
    dry_run: bool,
    replace_structure: bool,
) -> Dict[str, Any]:
    group = db.groups.find_one({"name": group_name})
    if not group and not dry_run:
        group_id = db.groups.insert_one(
            {
                "name": group_name,
                "description": "Grupo ficticio para base sintética",
                "created_at": datetime.now(timezone.utc),
                "synthetic_source": SYNTHETIC_SOURCE,
                "synthetic_seed_id": seed_id,
            }
        ).inserted_id
        group = db.groups.find_one({"_id": group_id})

    if not group and dry_run:
        # Simulated id for dry-run logs.
        group_id_str = "dry_run_group_san_juan"
    else:
        group_id_str = str(group["_id"])

    if replace_structure and not dry_run:
        existing_brands = list(db.brands.find({"group_id": group_id_str}, {"_id": 1}))
        existing_brand_ids = [str(row["_id"]) for row in existing_brands]
        agency_query: Dict[str, Any] = {"group_id": group_id_str}
        if existing_brand_ids:
            agency_query = {
                "$or": [
                    {"group_id": group_id_str},
                    {"brand_id": {"$in": existing_brand_ids}},
                ]
            }
        existing_agencies = list(db.agencies.find(agency_query, {"_id": 1}))
        existing_agency_ids = [str(row["_id"]) for row in existing_agencies]

        if existing_agency_ids:
            db.sales.delete_many({"agency_id": {"$in": existing_agency_ids}})
            db.vehicles.delete_many({"agency_id": {"$in": existing_agency_ids}})
            db.users.delete_many({"role": "seller", "agency_id": {"$in": existing_agency_ids}})
        db.agencies.delete_many({"group_id": group_id_str})
        db.brands.delete_many({"group_id": group_id_str})

    created_brand_ids: Dict[str, str] = {}
    agency_created_count = 0

    for brand_name, agency_count in brand_agency_plan:
        brand = None
        if not dry_run:
            brand = db.brands.find_one(
                {
                    "group_id": group_id_str,
                    "name": {"$regex": f"^{re.escape(brand_name)}$", "$options": "i"},
                }
            )
        if not brand and not dry_run:
            brand_id = db.brands.insert_one(
                {
                    "name": brand_name,
                    "group_id": group_id_str,
                    "logo_url": "",
                    "created_at": datetime.now(timezone.utc),
                    "synthetic_source": SYNTHETIC_SOURCE,
                    "synthetic_seed_id": seed_id,
                }
            ).inserted_id
            brand = db.brands.find_one({"_id": brand_id})
        brand_id_str = str(brand["_id"]) if brand else f"dry_{normalize_key(brand_name)}"
        created_brand_ids[brand_name] = brand_id_str

        existing_agencies = []
        if not dry_run:
            existing_agencies = list(
                db.agencies.find(
                    {"group_id": group_id_str, "brand_id": brand_id_str},
                    {"_id": 1, "name": 1},
                )
            )

        for idx in range(len(existing_agencies) + 1, agency_count + 1):
            agency_created_count += 1
            if dry_run:
                continue
            agency_name = f"{brand_name.upper()} SAN JUAN {idx:02d}"
            postal_code = f"{78000 + agency_created_count:05d}"
            db.agencies.insert_one(
                {
                    "name": agency_name,
                    "brand_id": brand_id_str,
                    "group_id": group_id_str,
                    "address": f"Av. Principal {100 + agency_created_count}, Parque San Juan, Mexico",
                    "city": "San Juan",
                    "street": "Av. Principal",
                    "exterior_number": str(100 + agency_created_count),
                    "interior_number": None,
                    "neighborhood": "Centro",
                    "municipality": "San Juan",
                    "state": "Estado de Mexico",
                    "postal_code": postal_code,
                    "country": "Mexico",
                    "google_place_id": None,
                    "latitude": None,
                    "longitude": None,
                    "created_at": datetime.now(timezone.utc),
                    "synthetic_source": SYNTHETIC_SOURCE,
                    "synthetic_seed_id": seed_id,
                }
            )

    return {
        "group_name": group_name,
        "group_id": group_id_str,
        "brand_count": len(brand_agency_plan),
        "agency_target": sum(count for _, count in brand_agency_plan),
    }


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Generate synthetic AutoConnect historical data")
    parser.add_argument("--mongo-url", default=os.environ.get("MONGO_URL", "mongodb://127.0.0.1:27017"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME", "inventariosplanes"))
    parser.add_argument(
        "--catalog-path",
        default=os.environ.get("STRAPI_JATO_CATALOG_PATH", CATALOG_DEFAULT_SOURCE_PATH),
    )
    parser.add_argument("--catalog-model-year", type=int, default=int(os.environ.get("CATALOG_MODEL_YEAR", CATALOG_DEFAULT_MODEL_YEAR)))
    parser.add_argument("--downloads-dir", default=str(Path.home() / "Downloads"))
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--seed", type=int, default=20260408)
    parser.add_argument("--seed-id", default="")
    parser.add_argument("--market-share-factor", type=float, default=0.015)
    parser.add_argument("--max-units-per-month", type=int, default=35)
    parser.add_argument("--stock-min", type=int, default=2)
    parser.add_argument("--stock-max", type=int, default=12)
    parser.add_argument("--reset-synthetic", action="store_true")
    parser.add_argument("--include-test-data", action="store_true")
    parser.add_argument("--group-name", default="")
    parser.add_argument("--brand-plan", default="")
    parser.add_argument("--replace-group-structure", action="store_true")
    parser.add_argument("--san-juan-scenario", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    seed_id = args.seed_id or make_seed_id()
    rng = np.random.default_rng(args.seed)

    client = MongoClient(args.mongo_url)
    db = client[args.db_name]

    if args.reset_synthetic and not args.dry_run:
        db.sales.delete_many({"synthetic_source": SYNTHETIC_SOURCE})
        db.vehicles.delete_many({"synthetic_source": SYNTHETIC_SOURCE})
        db.users.delete_many({"synthetic_source": SYNTHETIC_SOURCE, "role": "seller"})

    catalog_by_make, all_variants = load_catalog_variants(Path(args.catalog_path), args.catalog_model_year)
    demand = build_demand_signals(Path(args.downloads_dir))

    scenario_group_name = args.group_name.strip() if args.group_name else ""
    scenario_brand_plan: List[Tuple[str, int]] = []
    if args.san_juan_scenario:
        scenario_group_name = SAN_JUAN_GROUP_NAME
        scenario_brand_plan = SAN_JUAN_BRAND_AGENCY_PLAN
    elif args.brand_plan:
        scenario_brand_plan = parse_brand_plan(args.brand_plan)

    if scenario_group_name:
        if not scenario_brand_plan:
            raise ValueError("When --group-name is provided, --brand-plan is required.")
        structure_info = ensure_group_and_structure(
            db=db,
            group_name=scenario_group_name,
            brand_agency_plan=scenario_brand_plan,
            seed_id=seed_id,
            dry_run=args.dry_run,
            replace_structure=args.replace_group_structure or args.reset_synthetic or args.san_juan_scenario,
        )
        print(
            f"Scenario structure ready: {structure_info['group_name']} "
            f"(brands={structure_info['brand_count']}, agencies={structure_info['agency_target']})"
        )

    groups = {str(doc["_id"]): doc for doc in db.groups.find({})}
    brands = {str(doc["_id"]): doc for doc in db.brands.find({})}
    agencies_raw = [doc for doc in db.agencies.find({})]
    users_raw = [doc for doc in db.users.find({"role": "seller"})]

    agencies: List[Dict[str, Any]] = []
    for doc in agencies_raw:
        agency = {
            "id": str(doc["_id"]),
            "name": doc.get("name", ""),
            "group_id": doc.get("group_id"),
            "brand_id": doc.get("brand_id"),
            "city": doc.get("city"),
        }
        group_name = str(groups.get(agency["group_id"], {}).get("name", "") or "")
        brand_name = str(brands.get(agency["brand_id"], {}).get("name", "") or "")
        if not args.include_test_data and ("test" in group_name.lower() or "test" in brand_name.lower()):
            continue
        if scenario_group_name and group_name != scenario_group_name:
            continue
        agency["group_name"] = group_name
        agency["brand_name"] = brand_name
        agencies.append(agency)

    if not agencies:
        print("No agencies found to seed.")
        return 1

    sellers_by_agency: Dict[str, List[str]] = defaultdict(list)
    for user_doc in users_raw:
        agency_id = user_doc.get("agency_id")
        if agency_id:
            sellers_by_agency[str(agency_id)].append(str(user_doc["_id"]))

    make_key_by_brand_id: Dict[str, Optional[str]] = {}
    catalog_make_keys = list(catalog_by_make.keys())
    for brand_id, brand_doc in brands.items():
        make_key_by_brand_id[brand_id] = resolve_make_key(str(brand_doc.get("name", "")), catalog_make_keys)

    agencies_by_make: Dict[str, int] = defaultdict(int)
    for agency in agencies:
        make_key = make_key_by_brand_id.get(agency["brand_id"])
        if make_key:
            agencies_by_make[make_key] += 1

    target_months = target_months_window(args.months)
    generation_now_utc = datetime.now(timezone.utc)
    sold_vehicle_docs: List[Dict[str, Any]] = []
    sold_meta: List[Dict[str, Any]] = []
    stock_vehicle_docs: List[Dict[str, Any]] = []
    new_seller_docs: List[Dict[str, Any]] = []

    seller_counter = 1
    vin_counter = 1

    for agency in agencies:
        agency_id = agency["id"]
        make_key = make_key_by_brand_id.get(agency["brand_id"])
        variants = catalog_by_make.get(make_key or "", all_variants)

        existing_seller_count = len(sellers_by_agency.get(agency_id) or [])
        if existing_seller_count < 3:
            create_count = max(0, 3 - existing_seller_count)
            seller_docs = create_seller_docs_for_agency(agency, seed_id, seller_counter, create_count)
            seller_counter += create_count
            new_seller_docs.extend(seller_docs)
            if not sellers_by_agency.get(agency_id):
                sellers_by_agency[agency_id] = []

        avg_units_for_make = demand.make_avg_units.get(make_key or "", demand.fallback_avg_units)
        agency_count_for_make = max(agencies_by_make.get(make_key or "", 1), 1)
        base_lambda = max(
            0.2,
            (avg_units_for_make / agency_count_for_make) * args.market_share_factor,
        )
        base_lambda = min(base_lambda, float(args.max_units_per_month))
        agency_bias = float(rng.uniform(0.7, 1.35))

        for month_idx, (year, month) in enumerate(target_months):
            seasonality = demand.seasonality_by_month.get(month, 1.0)
            trend_factor = 0.92 + (0.18 * (month_idx / max(len(target_months) - 1, 1)))
            expected_units = min(
                float(args.max_units_per_month),
                max(0.0, base_lambda * agency_bias * seasonality * trend_factor),
            )
            units = int(rng.poisson(expected_units))
            if expected_units > 1.2 and units == 0 and rng.random() < 0.14:
                units = 1

            for _ in range(units):
                variant = variants[int(rng.integers(0, len(variants)))]
                msrp = float(variant["msrp"])
                sale_price = round(msrp * float(rng.uniform(0.94, 1.07)), 2)
                purchase_price = round(msrp * 0.88, 2)
                fi_revenue = round(sale_price * float(rng.uniform(0.0, 0.035)), 2)
                commission = round(
                    (sale_price * float(rng.uniform(0.008, 0.018))) +
                    (fi_revenue * float(rng.uniform(0.08, 0.15))),
                    2,
                )

                month_start_date = datetime(year, month, 1, 9, 0, tzinfo=timezone.utc)
                if month == 12:
                    month_end_date = datetime(year + 1, 1, 1, 0, 0, tzinfo=timezone.utc) - timedelta(minutes=1)
                else:
                    month_end_date = datetime(year, month + 1, 1, 0, 0, tzinfo=timezone.utc) - timedelta(minutes=1)

                if year == generation_now_utc.year and month == generation_now_utc.month:
                    sale_window_end = min(month_end_date, generation_now_utc)
                else:
                    sale_window_end = month_end_date

                if sale_window_end <= month_start_date:
                    sale_date = month_start_date
                else:
                    random_seconds = int(rng.integers(0, int((sale_window_end - month_start_date).total_seconds()) + 1))
                    sale_date = month_start_date + timedelta(seconds=random_seconds)
                in_stock_days = int(rng.integers(7, 120))
                entry_date = sale_date - timedelta(days=in_stock_days)

                sold_vehicle_docs.append(
                    {
                        "vin": build_vin(
                            seed_id=seed_id,
                            counter=vin_counter,
                            make_key=make_key,
                            year=int(variant["year"]),
                        ),
                        "model": variant["model"],
                        "year": int(variant["year"]),
                        "trim": variant["version"] or "Base",
                        "color": VEHICLE_COLORS[int(rng.integers(0, len(VEHICLE_COLORS)))],
                        "vehicle_type": "new",
                        "synthetic_msrp": msrp,
                        "synthetic_wholesale_discount_pct": 12.0,
                        "purchase_price": purchase_price,
                        "agency_id": agency_id,
                        "brand_id": agency["brand_id"],
                        "group_id": agency["group_id"],
                        "entry_date": entry_date,
                        "exit_date": sale_date,
                        "status": "sold",
                        "created_at": entry_date,
                        "synthetic_source": SYNTHETIC_SOURCE,
                        "synthetic_seed_id": seed_id,
                    }
                )
                sold_meta.append(
                    {
                        "agency_id": agency_id,
                        "brand_id": agency["brand_id"],
                        "group_id": agency["group_id"],
                        "model": variant["model"],
                        "sale_price": sale_price,
                        "sale_date": sale_date,
                        "fi_revenue": fi_revenue,
                        "commission": commission,
                    }
                )
                vin_counter += 1

        stock_units = int(
            max(
                args.stock_min,
                min(args.stock_max, round(base_lambda * agency_bias * float(rng.uniform(0.8, 1.6)))),
            )
        )
        now_utc = generation_now_utc
        for _ in range(stock_units):
            variant = variants[int(rng.integers(0, len(variants)))]
            msrp = float(variant["msrp"])
            purchase_price = round(msrp * 0.88, 2)
            age_days = int(rng.integers(5, 220))
            entry_date = now_utc - timedelta(days=age_days)
            stock_vehicle_docs.append(
                {
                    "vin": build_vin(
                        seed_id=seed_id,
                        counter=vin_counter,
                        make_key=make_key,
                        year=int(variant["year"]),
                    ),
                    "model": variant["model"],
                    "year": int(variant["year"]),
                    "trim": variant["version"] or "Base",
                    "color": VEHICLE_COLORS[int(rng.integers(0, len(VEHICLE_COLORS)))],
                    "vehicle_type": "new",
                    "synthetic_msrp": msrp,
                    "synthetic_wholesale_discount_pct": 12.0,
                    "purchase_price": purchase_price,
                    "agency_id": agency_id,
                    "brand_id": agency["brand_id"],
                    "group_id": agency["group_id"],
                    "entry_date": entry_date,
                    "exit_date": None,
                    "status": "in_stock",
                    "created_at": entry_date,
                    "synthetic_source": SYNTHETIC_SOURCE,
                    "synthetic_seed_id": seed_id,
                }
            )
            vin_counter += 1

    if args.dry_run:
        print("Dry run complete.")
        print(f"Seed ID: {seed_id}")
        print(f"Agencies processed: {len(agencies)}")
        print(f"New synthetic sellers: {len(new_seller_docs)}")
        print(f"Synthetic sold vehicles: {len(sold_vehicle_docs)}")
        print(f"Synthetic sales: {len(sold_meta)}")
        print(f"Synthetic in-stock vehicles: {len(stock_vehicle_docs)}")
        print(f"Demand files used: {demand.source_files}")
        return 0

    inserted_seller_ids: Dict[str, List[str]] = defaultdict(list)
    if new_seller_docs:
        result = db.users.insert_many(new_seller_docs, ordered=False)
        for doc, inserted_id in zip(new_seller_docs, result.inserted_ids):
            inserted_seller_ids[doc["agency_id"]].append(str(inserted_id))

    # Merge inserted sellers with existing ones.
    for agency in agencies:
        agency_id = agency["id"]
        sellers_by_agency[agency_id].extend(inserted_seller_ids.get(agency_id, []))

    model_pool_by_agency: Dict[str, set[str]] = defaultdict(set)
    for meta in sold_meta:
        model_name = str(meta.get("model") or "").strip()
        if model_name:
            model_pool_by_agency[str(meta["agency_id"])].add(model_name)

    seller_focus_by_agency: Dict[str, Dict[str, set[str]]] = {}
    for agency in agencies:
        agency_id = agency["id"]
        seller_focus_by_agency[agency_id] = build_seller_model_focus_map(
            seller_ids=sellers_by_agency.get(agency_id, []),
            model_pool=sorted(model_pool_by_agency.get(agency_id, set())),
            rng=rng,
        )

    # Insert sold vehicles and matching sales.
    sales_docs: List[Dict[str, Any]] = []
    chunk_size = 2000
    for start in range(0, len(sold_vehicle_docs), chunk_size):
        end = min(start + chunk_size, len(sold_vehicle_docs))
        vehicle_chunk = sold_vehicle_docs[start:end]
        meta_chunk = sold_meta[start:end]
        inserted = db.vehicles.insert_many(vehicle_chunk, ordered=False)
        for vehicle_id, meta in zip(inserted.inserted_ids, meta_chunk):
            seller_candidates = sellers_by_agency.get(meta["agency_id"]) or []
            if not seller_candidates:
                continue
            seller_focus_map = seller_focus_by_agency.get(meta["agency_id"], {})
            model_name = str(meta.get("model") or "").strip()
            weights: List[float] = []
            for candidate in seller_candidates:
                focus_models = seller_focus_map.get(str(candidate), set())
                if model_name and model_name in focus_models:
                    weights.append(3.2)
                elif focus_models:
                    weights.append(0.9)
                else:
                    weights.append(1.0)

            total_weight = float(sum(weights))
            if total_weight > 0:
                probs = [w / total_weight for w in weights]
                seller_id = str(rng.choice(np.array(seller_candidates, dtype=object), p=probs))
            else:
                seller_id = str(seller_candidates[int(rng.integers(0, len(seller_candidates)))])
            sales_docs.append(
                {
                    "vehicle_id": str(vehicle_id),
                    "seller_id": seller_id,
                    "agency_id": meta["agency_id"],
                    "brand_id": meta["brand_id"],
                    "group_id": meta["group_id"],
                    "model": meta.get("model"),
                    "sale_price": meta["sale_price"],
                    "sale_date": meta["sale_date"],
                    "fi_revenue": meta["fi_revenue"],
                    "commission": meta["commission"],
                    "created_at": meta["sale_date"],
                    "synthetic_source": SYNTHETIC_SOURCE,
                    "synthetic_seed_id": seed_id,
                }
            )

    if sales_docs:
        for start in range(0, len(sales_docs), chunk_size):
            end = min(start + chunk_size, len(sales_docs))
            db.sales.insert_many(sales_docs[start:end], ordered=False)

    if stock_vehicle_docs:
        for start in range(0, len(stock_vehicle_docs), chunk_size):
            end = min(start + chunk_size, len(stock_vehicle_docs))
            db.vehicles.insert_many(stock_vehicle_docs[start:end], ordered=False)

    print("Synthetic seed completed.")
    print(f"Seed ID: {seed_id}")
    print(f"Agencies processed: {len(agencies)}")
    print(f"New synthetic sellers: {len(new_seller_docs)}")
    print(f"Inserted sold vehicles: {len(sold_vehicle_docs)}")
    print(f"Inserted sales: {len(sales_docs)}")
    print(f"Inserted in-stock vehicles: {len(stock_vehicle_docs)}")
    print(f"Catalog path: {args.catalog_path}")
    if demand.source_files:
        print("Demand files used:")
        for path in demand.source_files:
            print(f"  - {path}")
    else:
        print("Demand files used: none (fallback demand profile)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
