from __future__ import annotations

from contextlib import contextmanager
from threading import Lock

import mysql.connector
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection

from config.database import DatabaseConfig


class DBConnectionPool:
    """
    Process-local MySQL connection pool.
    """

    _pool: pooling.MySQLConnectionPool | None = None
    _lock = Lock()
    _pool_name = "hr_dashboard_pool"

    @classmethod
    def initialize(cls, config: DatabaseConfig, pool_size: int = 5) -> None:
        with cls._lock:
            if cls._pool is not None:
                return
            cls._pool = pooling.MySQLConnectionPool(
                pool_name=cls._pool_name,
                pool_size=max(1, int(pool_size)),
                **config.as_connector_kwargs(),
            )

    @classmethod
    def get_connection(cls) -> MySQLConnection:
        if cls._pool is None:
            raise RuntimeError("DB pool not initialized. Call DBConnectionPool.initialize() first.")
        return cls._pool.get_connection()

    @classmethod
    @contextmanager
    def connection(cls):
        conn = cls.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    @classmethod
    @contextmanager
    def transaction(cls):
        conn = cls.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def ping_database() -> bool:
    """
    Lightweight connectivity check.
    """
    try:
        with DBConnectionPool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return True
    except Exception:
        return False
