from __future__ import annotations

import pandas as pd


def _legacy():
    import Final

    return Final


def calculate_lunch_break_risk(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    employees: list[str] | None = None,
    high_work_hours: float = 8.0,
    short_lunch_minutes: int = 30,
    avg_lunch_warning_minutes: int = 30,
    long_continuous_hours: float = 6.0,
) -> pd.DataFrame:
    return _legacy().calculate_lunch_break_risk(
        daily_df=daily_df,
        year=year,
        month=month,
        employees=employees,
        high_work_hours=high_work_hours,
        short_lunch_minutes=short_lunch_minutes,
        avg_lunch_warning_minutes=avg_lunch_warning_minutes,
        long_continuous_hours=long_continuous_hours,
    )
