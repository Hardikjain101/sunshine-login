from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    def as_connector_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }


def _safe_port(value: str | None, default: int = 3306) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def load_database_config() -> DatabaseConfig:
    """
    Load DB config from environment variables only.
    """
    return DatabaseConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=_safe_port(os.getenv("MYSQL_PORT"), 3306),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "hr_dashboard"),
    )
