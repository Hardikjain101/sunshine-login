from __future__ import annotations

from datetime import date, datetime, time
from typing import Optional


def parse_iso_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def parse_hhmm(value: str) -> Optional[time]:
    try:
        return datetime.strptime(str(value), "%H:%M").time().replace(second=0, microsecond=0)
    except (TypeError, ValueError):
        return None


def clamp_date(value: date, min_value: date, max_value: date) -> date:
    if value < min_value:
        return min_value
    if value > max_value:
        return max_value
    return value
