from __future__ import annotations

from typing import Optional

import pandas as pd


def _legacy():
    import Final

    return Final


def render_work_pattern_calendar_html(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    kpi_data: Optional[dict[str, float]] = None,
    annotation_items: Optional[tuple[tuple[str, str, str], ...]] = None,
) -> str:
    return _legacy().get_work_pattern_calendar_cached(
        daily_df=daily_df,
        employee_name=employee_name,
        year=year,
        month=month,
        kpi_data=kpi_data,
        special_day_items=annotation_items,
    )
