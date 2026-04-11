from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException

from repositories.import_repository import (
    find_agency_by_id,
    find_vehicle_by_id,
    insert_agency,
    insert_brand,
    insert_group,
    insert_user,
    insert_vehicle,
    list_agencies,
    list_brands,
    list_groups,
    list_users_email_role,
    update_agency_fields,
    update_brand_fields,
    update_group_fields,
    update_user_fields,
)
from services.sales_service import create_sale_record


IMPORT_VALID_USER_ROLES = {
    "app_admin",
    "app_user",
    "group_admin",
    "group_finance_manager",
    "brand_admin",
    "agency_admin",
    "agency_sales_manager",
    "agency_general_manager",
    "agency_commercial_manager",
    "group_user",
    "brand_user",
    "agency_user",
    "seller",
}


def _normalize_cell(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_key(value: Any) -> Optional[str]:
    text = _normalize_cell(value)
    return text.lower() if text else None


def _load_excel_sheets(content: bytes) -> Dict[str, pd.DataFrame]:
    try:
        sheets_raw = pd.read_excel(BytesIO(content), sheet_name=None)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(exc)}")
    return {str(name).strip().lower(): df for name, df in sheets_raw.items()}


def _load_tabular(content: bytes, filename: Optional[str]) -> pd.DataFrame:
    name = str(filename or "").lower()
    try:
        if name.endswith(".csv"):
            return pd.read_csv(BytesIO(content))
        if name.endswith((".xlsx", ".xls")):
            return pd.read_excel(BytesIO(content))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(exc)}")
    raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")


async def import_organization_from_excel(
    db: Any,
    *,
    current_user: Dict[str, Any],
    filename: Optional[str],
    content: bytes,
    resolve_agency_location: Callable[[Optional[str], Optional[str]], Dict[str, Optional[str]]],
    hash_password: Callable[[str], str],
) -> Dict[str, Any]:
    name = str(filename or "").lower()
    if not name.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Unsupported file format. Use Excel (.xlsx/.xls).")

    sheets = _load_excel_sheets(content)
    if not any(sheet_name in sheets for sheet_name in ["groups", "brands", "agencies", "sellers"]):
        raise HTTPException(
            status_code=400,
            detail="Excel must contain at least one sheet named: groups, brands, agencies, or sellers.",
        )

    user_role = str(current_user.get("role") or "")
    user_group_id = current_user.get("group_id")
    if user_role == "group_admin" and not user_group_id:
        raise HTTPException(status_code=403, detail="Group admin has no assigned group")

    groups = await list_groups(db, limit=10000)
    groups_by_id = {str(group["_id"]): group for group in groups}
    groups_by_name = {
        _normalize_key(group.get("name")): group
        for group in groups
        if _normalize_key(group.get("name"))
    }

    brands = await list_brands(db, limit=10000)
    brands_by_id = {str(brand["_id"]): brand for brand in brands}
    brands_by_group_and_name: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for brand in brands:
        key_name = _normalize_key(brand.get("name"))
        group_id = brand.get("group_id")
        if key_name and group_id:
            brands_by_group_and_name[(str(group_id), key_name)] = brand

    agencies = await list_agencies(db, limit=10000)
    agencies_by_id = {str(agency["_id"]): agency for agency in agencies}
    agencies_by_brand_and_name: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for agency in agencies:
        key_name = _normalize_key(agency.get("name"))
        brand_id = agency.get("brand_id")
        if key_name and brand_id:
            agencies_by_brand_and_name[(str(brand_id), key_name)] = agency

    users = await list_users_email_role(db, limit=10000)
    users_by_email = {
        _normalize_key(user.get("email")): user
        for user in users
        if _normalize_key(user.get("email"))
    }

    summary: Dict[str, Dict[str, int]] = {
        "groups": {"created": 0, "updated": 0, "skipped": 0},
        "brands": {"created": 0, "updated": 0, "skipped": 0},
        "agencies": {"created": 0, "updated": 0, "skipped": 0},
        "sellers": {"created": 0, "updated": 0, "skipped": 0},
    }
    errors: List[str] = []

    def resolve_group_id(row: pd.Series, default_group_id: Optional[str] = None) -> Optional[str]:
        group_id_raw = _normalize_cell(row.get("group_id"))
        if group_id_raw and group_id_raw in groups_by_id:
            return group_id_raw
        group_name_key = _normalize_key(row.get("group_name"))
        if group_name_key and group_name_key in groups_by_name:
            return str(groups_by_name[group_name_key]["_id"])
        return default_group_id

    if "groups" in sheets:
        groups_df = sheets["groups"]
        if user_role == "group_admin":
            summary["groups"]["skipped"] += len(groups_df)
            errors.append("Sheet groups skipped: group_admin cannot create groups.")
        else:
            for index, row in groups_df.iterrows():
                row_number = index + 2
                group_name = _normalize_cell(row.get("name"))
                description = _normalize_cell(row.get("description"))
                if not group_name:
                    summary["groups"]["skipped"] += 1
                    errors.append(f"groups row {row_number}: name is required")
                    continue

                group_key = group_name.lower()
                existing = groups_by_name.get(group_key)
                if existing:
                    update_data: Dict[str, Any] = {}
                    if description is not None and description != existing.get("description"):
                        update_data["description"] = description
                    if update_data:
                        await update_group_fields(db, existing.get("_id"), update_data)
                        existing.update(update_data)
                        summary["groups"]["updated"] += 1
                    else:
                        summary["groups"]["skipped"] += 1
                else:
                    group_doc = {
                        "name": group_name,
                        "description": description,
                        "created_at": datetime.now(timezone.utc),
                    }
                    inserted_id = await insert_group(db, group_doc)
                    group_doc["_id"] = inserted_id
                    groups_by_id[inserted_id] = group_doc
                    groups_by_name[group_key] = group_doc
                    summary["groups"]["created"] += 1

    if "brands" in sheets:
        brands_df = sheets["brands"]
        for index, row in brands_df.iterrows():
            row_number = index + 2
            brand_name = _normalize_cell(row.get("name"))
            if not brand_name:
                summary["brands"]["skipped"] += 1
                errors.append(f"brands row {row_number}: name is required")
                continue

            resolved_group_id = resolve_group_id(
                row,
                default_group_id=user_group_id if user_role == "group_admin" else None,
            )
            if not resolved_group_id:
                summary["brands"]["skipped"] += 1
                errors.append(f"brands row {row_number}: group_id/group_name is required")
                continue

            if user_role == "group_admin" and resolved_group_id != user_group_id:
                summary["brands"]["skipped"] += 1
                errors.append(f"brands row {row_number}: cannot import outside your group")
                continue

            logo_url = _normalize_cell(row.get("logo_url"))
            brand_key = (resolved_group_id, brand_name.lower())
            existing = brands_by_group_and_name.get(brand_key)
            if existing:
                update_data: Dict[str, Any] = {}
                if logo_url is not None and logo_url != existing.get("logo_url"):
                    update_data["logo_url"] = logo_url
                if update_data:
                    await update_brand_fields(db, existing.get("_id"), update_data)
                    existing.update(update_data)
                    summary["brands"]["updated"] += 1
                else:
                    summary["brands"]["skipped"] += 1
            else:
                brand_doc = {
                    "name": brand_name,
                    "group_id": resolved_group_id,
                    "logo_url": logo_url,
                    "created_at": datetime.now(timezone.utc),
                }
                inserted_id = await insert_brand(db, brand_doc)
                brand_doc["_id"] = inserted_id
                brands_by_id[inserted_id] = brand_doc
                brands_by_group_and_name[brand_key] = brand_doc
                summary["brands"]["created"] += 1

    if "agencies" in sheets:
        agencies_df = sheets["agencies"]
        for index, row in agencies_df.iterrows():
            row_number = index + 2
            agency_name = _normalize_cell(row.get("name"))
            if not agency_name:
                summary["agencies"]["skipped"] += 1
                errors.append(f"agencies row {row_number}: name is required")
                continue

            resolved_group_id = resolve_group_id(
                row,
                default_group_id=user_group_id if user_role == "group_admin" else None,
            )

            brand_id_raw = _normalize_cell(row.get("brand_id"))
            brand = brands_by_id.get(brand_id_raw) if brand_id_raw else None
            if not brand:
                brand_name_key = _normalize_key(row.get("brand_name"))
                if brand_name_key:
                    if resolved_group_id:
                        brand = brands_by_group_and_name.get((resolved_group_id, brand_name_key))
                    else:
                        candidates = [
                            item
                            for (group_id, brand_name), item in brands_by_group_and_name.items()
                            if brand_name == brand_name_key
                        ]
                        if len(candidates) == 1:
                            brand = candidates[0]

            if not brand:
                summary["agencies"]["skipped"] += 1
                errors.append(f"agencies row {row_number}: brand_id/brand_name not found or ambiguous")
                continue

            resolved_group_id = str(brand.get("group_id"))
            resolved_brand_id = str(brand["_id"])
            if user_role == "group_admin" and resolved_group_id != user_group_id:
                summary["agencies"]["skipped"] += 1
                errors.append(f"agencies row {row_number}: cannot import outside your group")
                continue

            city = _normalize_cell(row.get("city"))
            address = _normalize_cell(row.get("address"))
            agency_key = (resolved_brand_id, agency_name.lower())
            existing = agencies_by_brand_and_name.get(agency_key)

            if existing:
                update_data: Dict[str, Any] = {}
                if address is not None and address != existing.get("address"):
                    update_data["address"] = address
                next_city_source = city if city is not None else existing.get("city")
                next_address_source = address if address is not None else existing.get("address")
                location = resolve_agency_location(next_city_source, next_address_source)
                if location.get("city") is not None and location.get("city") != existing.get("city"):
                    update_data["city"] = location["city"]
                if location.get("postal_code") is not None and location.get("postal_code") != existing.get("postal_code"):
                    update_data["postal_code"] = location["postal_code"]
                if update_data:
                    await update_agency_fields(db, existing.get("_id"), update_data)
                    existing.update(update_data)
                    summary["agencies"]["updated"] += 1
                else:
                    summary["agencies"]["skipped"] += 1
            else:
                location = resolve_agency_location(city, address)
                agency_doc = {
                    "name": agency_name,
                    "brand_id": resolved_brand_id,
                    "group_id": resolved_group_id,
                    "city": location["city"],
                    "postal_code": location["postal_code"],
                    "address": address,
                    "created_at": datetime.now(timezone.utc),
                }
                inserted_id = await insert_agency(db, agency_doc)
                agency_doc["_id"] = inserted_id
                agencies_by_id[inserted_id] = agency_doc
                agencies_by_brand_and_name[agency_key] = agency_doc
                summary["agencies"]["created"] += 1

    if "sellers" in sheets:
        sellers_df = sheets["sellers"]
        for index, row in sellers_df.iterrows():
            row_number = index + 2
            email = _normalize_key(row.get("email"))
            if not email:
                summary["sellers"]["skipped"] += 1
                errors.append(f"sellers row {row_number}: email is required")
                continue

            resolved_group_id = resolve_group_id(
                row,
                default_group_id=user_group_id if user_role == "group_admin" else None,
            )

            agency_id_raw = _normalize_cell(row.get("agency_id"))
            agency = agencies_by_id.get(agency_id_raw) if agency_id_raw else None
            if not agency:
                agency_name_key = _normalize_key(row.get("agency_name"))
                if agency_name_key:
                    brand = None
                    brand_id_raw = _normalize_cell(row.get("brand_id"))
                    if brand_id_raw:
                        brand = brands_by_id.get(brand_id_raw)
                    if not brand:
                        brand_name_key = _normalize_key(row.get("brand_name"))
                        if brand_name_key:
                            if resolved_group_id:
                                brand = brands_by_group_and_name.get((resolved_group_id, brand_name_key))
                            else:
                                candidates = [
                                    item
                                    for (group_id, brand_name), item in brands_by_group_and_name.items()
                                    if brand_name == brand_name_key
                                ]
                                if len(candidates) == 1:
                                    brand = candidates[0]
                    if brand:
                        agency = agencies_by_brand_and_name.get((str(brand["_id"]), agency_name_key))
                    if not agency:
                        candidates = [
                            item
                            for (_, agency_name), item in agencies_by_brand_and_name.items()
                            if agency_name == agency_name_key
                        ]
                        if len(candidates) == 1:
                            agency = candidates[0]

            if not agency:
                summary["sellers"]["skipped"] += 1
                errors.append(f"sellers row {row_number}: agency_id/agency_name not found or ambiguous")
                continue

            resolved_group_id = str(agency.get("group_id"))
            resolved_brand_id = str(agency.get("brand_id"))
            resolved_agency_id = str(agency.get("_id"))
            if user_role == "group_admin" and resolved_group_id != user_group_id:
                summary["sellers"]["skipped"] += 1
                errors.append(f"sellers row {row_number}: cannot import outside your group")
                continue

            name = _normalize_cell(row.get("name")) or email.split("@")[0]
            role = _normalize_cell(row.get("role")) or "seller"
            password = _normalize_cell(row.get("password")) or "TempPass123!"
            if role not in IMPORT_VALID_USER_ROLES:
                summary["sellers"]["skipped"] += 1
                errors.append(f"sellers row {row_number}: invalid role '{role}'")
                continue
            if user_role == "group_admin" and role in {"app_admin", "app_user"}:
                summary["sellers"]["skipped"] += 1
                errors.append(f"sellers row {row_number}: group_admin cannot create app-level roles")
                continue

            existing_user = users_by_email.get(email)
            if existing_user:
                update_data = {
                    "name": name,
                    "role": role,
                    "group_id": resolved_group_id,
                    "brand_id": resolved_brand_id,
                    "agency_id": resolved_agency_id,
                }
                if _normalize_cell(row.get("password")):
                    update_data["password_hash"] = hash_password(password)
                await update_user_fields(db, existing_user.get("_id"), update_data)
                existing_user.update(update_data)
                summary["sellers"]["updated"] += 1
            else:
                user_doc = {
                    "email": email,
                    "password_hash": hash_password(password),
                    "name": name,
                    "role": role,
                    "group_id": resolved_group_id,
                    "brand_id": resolved_brand_id,
                    "agency_id": resolved_agency_id,
                    "created_at": datetime.now(timezone.utc),
                }
                inserted_id = await insert_user(db, user_doc)
                user_doc["_id"] = inserted_id
                users_by_email[email] = user_doc
                summary["sellers"]["created"] += 1

    return {
        "message": "Organization import finished",
        "summary": summary,
        "errors": errors[:200],
    }


async def import_vehicles_from_file(
    db: Any,
    *,
    filename: Optional[str],
    content: bytes,
    allowed_model_year: int,
) -> Dict[str, Any]:
    df = _load_tabular(content, filename)
    required_columns = ["vin", "model", "year", "trim", "color", "vehicle_type", "purchase_price", "agency_id"]
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")

    imported = 0
    errors: List[str] = []

    for index, row in df.iterrows():
        row_number = index + 2
        try:
            agency = await find_agency_by_id(db, row.get("agency_id"))
            if not agency:
                errors.append(f"Row {row_number}: Agency not found")
                continue

            entry_date = row.get("entry_date")
            if pd.isna(entry_date):
                entry_date = datetime.now(timezone.utc)
            elif isinstance(entry_date, str):
                entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))

            row_year = int(row["year"])
            if row_year != int(allowed_model_year):
                errors.append(f"Row {row_number}: only model year {allowed_model_year} is allowed")
                continue

            vehicle_doc = {
                "vin": str(row["vin"]),
                "model": str(row["model"]),
                "year": row_year,
                "trim": str(row["trim"]),
                "color": str(row["color"]),
                "vehicle_type": str(row["vehicle_type"]),
                "purchase_price": float(row["purchase_price"]),
                "agency_id": str(row["agency_id"]),
                "brand_id": agency.get("brand_id"),
                "group_id": agency.get("group_id"),
                "entry_date": entry_date,
                "exit_date": None,
                "status": "in_stock",
                "created_at": datetime.now(timezone.utc),
            }
            await insert_vehicle(db, vehicle_doc)
            imported += 1
        except Exception as exc:
            errors.append(f"Row {row_number}: {str(exc)}")

    return {
        "imported": imported,
        "errors": errors,
        "total_rows": len(df),
    }


async def import_sales_from_file(
    db: Any,
    *,
    filename: Optional[str],
    content: bytes,
    calculate_commission: Callable[..., Awaitable[float]],
    resolve_effective_sale_pricing_for_model: Callable[..., Awaitable[Dict[str, Any]]],
    apply_manual_sale_price_override: Callable[[Dict[str, Any], Any], Dict[str, Any]],
    extract_active_aging_incentive_plan: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    apply_aging_plan_to_effective_pricing: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Tuple[Dict[str, Any], Dict[str, float]]],
    to_non_negative_float: Callable[[Any, float], float],
) -> Dict[str, Any]:
    df = _load_tabular(content, filename)
    required_columns = ["vehicle_id", "seller_id", "sale_price", "sale_date"]
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")

    imported = 0
    errors: List[str] = []

    for index, row in df.iterrows():
        row_number = index + 2
        try:
            vehicle = await find_vehicle_by_id(db, row.get("vehicle_id"))
            if not vehicle:
                errors.append(f"Row {row_number}: Vehicle not found")
                continue

            sale_date = row.get("sale_date")
            if isinstance(sale_date, str):
                sale_date = datetime.fromisoformat(sale_date.replace("Z", "+00:00"))

            fi_revenue = float(row.get("fi_revenue", 0)) if not pd.isna(row.get("fi_revenue")) else 0
            plant_incentive = float(row.get("plant_incentive", 0)) if not pd.isna(row.get("plant_incentive")) else 0

            await create_sale_record(
                db,
                sale_data={
                    "vehicle_id": str(row["vehicle_id"]),
                    "seller_id": str(row["seller_id"]),
                    "sale_price": row.get("sale_price"),
                    "sale_date": sale_date,
                    "fi_revenue": fi_revenue,
                    "plant_incentive": plant_incentive,
                },
                vehicle=vehicle,
                calculate_commission=calculate_commission,
                resolve_effective_sale_pricing_for_model=resolve_effective_sale_pricing_for_model,
                apply_manual_sale_price_override=apply_manual_sale_price_override,
                extract_active_aging_incentive_plan=extract_active_aging_incentive_plan,
                apply_aging_plan_to_effective_pricing=apply_aging_plan_to_effective_pricing,
                to_non_negative_float=to_non_negative_float,
            )

            imported += 1
        except Exception as exc:
            errors.append(f"Row {row_number}: {str(exc)}")

    return {
        "imported": imported,
        "errors": errors,
        "total_rows": len(df),
    }
