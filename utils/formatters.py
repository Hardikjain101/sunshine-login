from __future__ import annotations

from datetime import time
from typing import Optional, Tuple

from utils.date_utils import parse_hhmm


def parse_special_hours_reason(reason: str) -> Tuple[Optional[time], Optional[time], str]:
    """
    Parse "HH:MM-HH:MM | reason".
    """
    raw = str(reason or "").strip()
    if not raw:
        return None, None, ""

    if "|" in raw:
        window, remaining = raw.split("|", 1)
        remaining = remaining.strip()
    else:
        window = raw
        remaining = ""

    if "-" not in window:
        return None, None, raw

    start_text, end_text = [part.strip() for part in window.split("-", 1)]
    start_time = parse_hhmm(start_text)
    end_time = parse_hhmm(end_text)
    if start_time is None or end_time is None:
        return None, None, raw
    return start_time, end_time, remaining


def format_special_hours_reason(open_time: time, close_time: time, reason: str = "") -> str:
    base = f"{open_time.strftime('%H:%M')}-{close_time.strftime('%H:%M')}"
    reason_text = str(reason or "").strip()
    if reason_text:
        return f"{base} | {reason_text}"
    return base
