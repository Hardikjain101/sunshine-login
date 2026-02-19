from __future__ import annotations

from typing import Tuple

import pandas as pd


def _legacy():
    import Final

    return Final


def run_dashboard() -> None:
    """
    Compatibility runner that keeps validated dashboard behavior unchanged.
    """
    _legacy().main()


def load_processed_data(
    data_source_path: str,
    source_signature: str,
    cache_version: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return _legacy().load_and_process_data(data_source_path, source_signature, cache_version)


def validate_processed_frames(
    raw_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    emp_metrics_df: pd.DataFrame,
    weekly_overtime_df: pd.DataFrame,
    monthly_overtime_df: pd.DataFrame,
) -> None:
    _legacy()._validate_processed_frames(
        raw_df,
        daily_df,
        emp_metrics_df,
        weekly_overtime_df,
        monthly_overtime_df,
    )
