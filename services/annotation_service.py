from __future__ import annotations

from datetime import date

from database.repository import AnnotationRepository
from models.schemas import AnnotationRecord


class AnnotationService:
    """
    Application-facing annotation API.
    """

    _type_alias = {"Closed": "Full Off"}

    def __init__(self, repository: AnnotationRepository | None = None) -> None:
        self.repository = repository or AnnotationRepository()

    def ensure_ready(self) -> None:
        self.repository.ensure_table()

    def list_records(self) -> list[AnnotationRecord]:
        return self.repository.list_annotations()

    def list_items(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(record.as_item() for record in self.list_records())

    def upsert(self, day: date, annotation_type: str, reason: str = "") -> None:
        normalized = self._type_alias.get(annotation_type, annotation_type)
        self.repository.upsert_annotation(day, normalized, reason)

    def delete(self, day: date) -> None:
        self.repository.delete_annotation(day)
