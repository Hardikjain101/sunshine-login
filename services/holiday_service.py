from __future__ import annotations

from datetime import date


def _legacy():
    import Final

    return Final


def get_holidays(year: int) -> dict[date, str]:
    return _legacy().get_company_holidays(year)


def get_effective_holidays(
    year: int,
    annotation_items: tuple[tuple[str, str, str], ...] | None = None,
) -> dict[date, str]:
    return _legacy().get_effective_holiday_map(year, annotation_items)
