from __future__ import annotations

import pandas as pd


def _legacy():
    import Final

    return Final


def add_compliance_metrics(daily_df: pd.DataFrame) -> pd.DataFrame:
    return _legacy().FeatureEngineer.add_compliance_metrics(daily_df)


def apply_annotation_overrides(
    daily_df: pd.DataFrame,
    annotation_items: tuple[tuple[str, str, str], ...] | None = None,
) -> pd.DataFrame:
    return _legacy().apply_annotation_overrides(daily_df, annotation_items)
