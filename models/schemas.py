from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class AnnotationRecord:
    date: date
    annotation_type: str
    reason: str = ""
    created_at: datetime | None = None

    def as_item(self) -> tuple[str, str, str]:
        return (self.date.strftime("%Y-%m-%d"), self.annotation_type, self.reason)
