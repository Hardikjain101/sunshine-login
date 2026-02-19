from __future__ import annotations

import pandas as pd


def _legacy():
    import Final

    return Final


def plot_overtime_chart(overtime_df: pd.DataFrame, time_period: str, top_n: int = 15):
    return _legacy().plot_overtime_charts(overtime_df, time_period, top_n=top_n)


def plot_lunch_risk_bar(risk_df: pd.DataFrame, top_n: int = 15):
    return _legacy().plot_lunch_risk_bar_chart(risk_df, top_n=top_n)


def plot_lunch_risk_scatter(risk_df: pd.DataFrame, avg_lunch_warning_minutes: int = 30):
    return _legacy().plot_lunch_risk_scatter(risk_df, avg_lunch_warning_minutes=avg_lunch_warning_minutes)
