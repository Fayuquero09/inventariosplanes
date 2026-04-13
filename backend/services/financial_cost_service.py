from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Optional


def coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    parsed: Optional[datetime] = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def calculate_vehicle_financial_cost_in_period(
    vehicle: Dict[str, Any],
    period_start: datetime,
    period_end: datetime,
    *,
    resolve_effective_rate_components_for_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    days_per_month_for_rate: int,
) -> float:
    entry_date = coerce_utc_datetime(vehicle.get("entry_date"))
    if not entry_date:
        return 0.0

    start_at = coerce_utc_datetime(period_start) or period_start
    end_at = coerce_utc_datetime(period_end) or period_end
    if start_at.tzinfo is None:
        start_at = start_at.replace(tzinfo=timezone.utc)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=timezone.utc)
    if end_at <= start_at:
        return 0.0

    effective_rate = await resolve_effective_rate_components_for_vehicle(vehicle)
    total_rate_monthly = effective_rate.get("total_rate_monthly")
    if total_rate_monthly is None:
        return 0.0

    grace_days = int(effective_rate.get("grace_days", 0) or 0)
    grace_end = entry_date + timedelta(days=grace_days)
    vehicle_end = coerce_utc_datetime(vehicle.get("exit_date")) or datetime.now(timezone.utc)

    charge_start = max(start_at, grace_end)
    charge_end = min(end_at, vehicle_end)
    if charge_end <= charge_start:
        return 0.0

    charge_days = (charge_end - charge_start).days
    if charge_days <= 0:
        return 0.0

    daily_rate = total_rate_monthly / days_per_month_for_rate / 100
    financial_cost = float(vehicle.get("purchase_price", 0) or 0) * daily_rate * charge_days
    return round(financial_cost, 2)


async def calculate_vehicle_financial_cost(
    vehicle: Dict[str, Any],
    *,
    resolve_effective_rate_components_for_vehicle: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    days_per_month_for_rate: int,
) -> float:
    entry_date = coerce_utc_datetime(vehicle.get("entry_date"))
    if not entry_date:
        return 0.0
    end_date = coerce_utc_datetime(vehicle.get("exit_date")) or datetime.now(timezone.utc)
    return await calculate_vehicle_financial_cost_in_period(
        vehicle,
        entry_date,
        end_date,
        resolve_effective_rate_components_for_vehicle=resolve_effective_rate_components_for_vehicle,
        days_per_month_for_rate=days_per_month_for_rate,
    )
