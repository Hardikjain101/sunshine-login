from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os


@dataclass(frozen=True)
class AppSettings:
    app_name: str
    cache_version: str
    db_pool_size: int
    static_css_path: str


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


@lru_cache(maxsize=1)
def load_app_settings() -> AppSettings:
    """
    Load global app settings.
    Defaults align with the validated legacy dashboard behavior.
    """
    legacy_cache_version = "2026-02-19"
    try:
        from Final import Config as LegacyConfig  # Lazy import to avoid heavy startup coupling

        legacy_cache_version = str(getattr(LegacyConfig, "CACHE_VERSION", legacy_cache_version))
    except Exception:
        pass

    return AppSettings(
        app_name=os.getenv("APP_NAME", "HR Attendance Analytics Dashboard"),
        cache_version=os.getenv("CACHE_VERSION", legacy_cache_version),
        db_pool_size=_env_int("DB_POOL_SIZE", 5),
        static_css_path=os.getenv("STATIC_CSS_PATH", "static/styles.css"),
    )
