from __future__ import annotations


def _legacy():
    import Final

    return Final


def render_metric_card(label: str, value, help_text: str = "") -> None:
    _legacy().create_metric_card(label, value, help_text=help_text)
