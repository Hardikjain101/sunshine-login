from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

from database.connection import DBConnectionPool
from database import queries
from models.schemas import AnnotationRecord


class AnnotationRepository:
    """
    Repository for annotation persistence.
    """

    allowed_types = {"Open Late", "Early Close", "Special Hours", "Full Off"}

    def ensure_table(self) -> None:
        with DBConnectionPool.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queries.CREATE_ANNOTATION_TABLE)

    def list_annotations(self) -> list[AnnotationRecord]:
        with DBConnectionPool.connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(queries.SELECT_ANNOTATIONS)
                rows = cursor.fetchall() or []
        records: list[AnnotationRecord] = []
        for row in rows:
            raw_date = row.get("date")
            if isinstance(raw_date, datetime):
                dval = raw_date.date()
            else:
                dval = raw_date
            if not isinstance(dval, date):
                continue
            ann_type = str(row.get("annotation_type") or "").strip()
            if ann_type not in self.allowed_types:
                continue
            records.append(
                AnnotationRecord(
                    date=dval,
                    annotation_type=ann_type,
                    reason=str(row.get("reason") or "").strip(),
                    created_at=row.get("created_at"),
                )
            )
        return records

    def upsert_annotation(self, dval: date, annotation_type: str, reason: str = "") -> None:
        if annotation_type not in self.allowed_types:
            raise ValueError(f"Unsupported annotation type: {annotation_type}")
        with DBConnectionPool.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    queries.UPSERT_ANNOTATION,
                    (dval, annotation_type, str(reason or "").strip()),
                )

    def delete_annotation(self, dval: date) -> None:
        with DBConnectionPool.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queries.DELETE_ANNOTATION_BY_DATE, (dval,))

    def upsert_many(self, rows: Iterable[AnnotationRecord]) -> None:
        payload = [
            (row.date, row.annotation_type, row.reason)
            for row in rows
            if row.annotation_type in self.allowed_types
        ]
        if not payload:
            return
        with DBConnectionPool.transaction() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(queries.UPSERT_ANNOTATION, payload)
