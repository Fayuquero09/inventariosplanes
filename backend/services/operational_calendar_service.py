from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> int:
    """weekday uses datetime.weekday() convention: Monday=0 ... Sunday=6."""
    first_day_weekday = datetime(year, month, 1, tzinfo=timezone.utc).weekday()
    return 1 + ((weekday - first_day_weekday + 7) % 7) + ((occurrence - 1) * 7)


def add_months_ym(year: int, month: int, offset: int) -> Tuple[int, int]:
    normalized = (int(year) * 12) + (int(month) - 1) + int(offset)
    new_year = normalized // 12
    new_month = (normalized % 12) + 1
    return new_year, new_month


def decrement_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def mexico_lft_holidays_by_month(year: int) -> Dict[int, List[int]]:
    holidays: Dict[int, set[int]] = {month: set() for month in range(1, 13)}

    # LFT Art. 74 (federales, referencia operativa).
    holidays[1].add(1)  # 1 de enero
    holidays[2].add(nth_weekday_of_month(year, 2, 0, 1))  # 1er lunes de febrero
    holidays[3].add(nth_weekday_of_month(year, 3, 0, 3))  # 3er lunes de marzo
    holidays[5].add(1)  # 1 de mayo
    holidays[9].add(16)  # 16 de septiembre
    holidays[11].add(nth_weekday_of_month(year, 11, 0, 3))  # 3er lunes de noviembre
    holidays[12].add(25)  # 25 de diciembre

    # Transmisión del Poder Ejecutivo Federal: 1 de octubre cada 6 años (2024, 2030, ...).
    if year >= 2024 and ((year - 2024) % 6 == 0):
        holidays[10].add(1)

    return {month: sorted(days) for month, days in holidays.items()}


def resolve_effective_objective_units(
    configured_units: float,
    previous_year_units_observed: int,
    days_in_month: int,
    elapsed_days: Optional[int] = None,
) -> Tuple[float, str]:
    configured = float(configured_units or 0)
    if configured > 0:
        return round(configured, 2), "configured"

    previous = int(previous_year_units_observed or 0)
    if previous <= 0:
        return 0.0, "none"

    if elapsed_days and elapsed_days > 0:
        projected = (previous / elapsed_days) * days_in_month
        return round(projected, 2), "benchmark_last_year"

    return float(previous), "benchmark_last_year"


def compute_operational_day_profile(year: int, month: int) -> Dict[str, Any]:
    days_in_month = monthrange(year, month)[1] or 1
    holiday_days = set(mexico_lft_holidays_by_month(year).get(month, []))
    day_weights: Dict[int, float] = {}
    total_weight = 0.0

    for day in range(1, days_in_month + 1):
        weekday = datetime(year, month, day, tzinfo=timezone.utc).weekday()  # Monday=0 ... Sunday=6
        if day in holiday_days or weekday == 6:  # Domingo o feriado
            weight = 0.0
        elif weekday == 5:  # Sábado = medio día hábil
            weight = 0.5
        else:
            weight = 1.0
        day_weights[day] = weight
        total_weight += weight

    cumulative = 0.0
    cumulative_shares: Dict[int, float] = {}
    for day in range(1, days_in_month + 1):
        cumulative += day_weights.get(day, 0.0)
        if total_weight > 0:
            cumulative_shares[day] = round(min(1.0, cumulative / total_weight), 6)
        else:
            cumulative_shares[day] = round(day / days_in_month, 6)

    cumulative_shares[days_in_month] = 1.0
    return {
        "days_in_month": days_in_month,
        "day_weights": day_weights,
        "cumulative_shares": cumulative_shares,
        "total_weight": total_weight,
    }

