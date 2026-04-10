from typing import Any, Dict, List


def calculate_commission_from_rules(
    rules: List[Dict[str, Any]],
    *,
    units: int,
    average_ticket: float,
    average_fi_revenue: float,
) -> float:
    total_commission = 0.0
    for rule in rules:
        rule_type = rule.get("rule_type")
        value = float(rule.get("value") or 0)
        if rule_type == "per_unit":
            total_commission += value * units
        elif rule_type == "percentage":
            total_commission += (average_ticket * units) * (value / 100)
        elif rule_type == "volume_bonus":
            min_units = rule.get("min_units")
            max_units = rule.get("max_units")
            if min_units and units >= int(min_units):
                if not max_units or units <= int(max_units):
                    total_commission += value
        elif rule_type == "fi_bonus":
            total_commission += (average_fi_revenue * units) * (value / 100)
    return round(total_commission, 2)

