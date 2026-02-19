from __future__ import annotations

from typing import Iterable, TypeVar

T = TypeVar("T")


def first_or_default(values: Iterable[T], default: T) -> T:
    for value in values:
        return value
    return default


def safe_str(value: object) -> str:
    return str(value).strip() if value is not None else ""
