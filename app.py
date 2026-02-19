"""
Production entry point for the HR dashboard.

This module keeps startup minimal and delegates business/UI behavior to
modular services while preserving legacy behavior.
"""

from __future__ import annotations

import streamlit as st
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback
    def load_dotenv(*args, **kwargs):
        return False

from config.database import load_database_config
from config.settings import load_app_settings
from database.connection import DBConnectionPool
from services.annotation_service import AnnotationService
from services.attendance_service import run_dashboard
from components.filters_component import inject_global_styles


def initialize_infrastructure() -> None:
    """
    Initialize environment, DB pool, and required tables.
    """
    load_dotenv(override=False)
    settings = load_app_settings()

    try:
        db_config = load_database_config()
        DBConnectionPool.initialize(db_config, pool_size=settings.db_pool_size)
        AnnotationService().ensure_ready()
    except Exception as exc:
        # Keep app startup resilient; dashboard can still run with degraded DB features.
        st.warning(f"Database bootstrap warning: {exc}")

    inject_global_styles(settings.static_css_path)


def main() -> None:
    initialize_infrastructure()
    run_dashboard()


if __name__ == "__main__":
    main()
