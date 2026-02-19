from __future__ import annotations

from datetime import date

import pandas as pd


def _legacy():
    import Final

    return Final


def calculate_weekly_monthly_overtime(
    daily_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _legacy().FeatureEngineer.calculate_overtime_metrics(daily_df)


def calculate_fifteen_day_overtime(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    return _legacy().calculate_15_day_overtime(
        daily_df=daily_df,
        year=year,
        month=month,
        start_date=start_date,
        end_date=end_date,
    )
