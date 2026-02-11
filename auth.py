"""
Authentication utilities for the Streamlit HR dashboard.
Uses MySQL for user storage and bcrypt for password hashing.
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

import bcrypt
import mysql.connector
from mysql.connector import Error

USER_TABLE = "users"


def _is_bcrypt_hash(value: str) -> bool:
    if not value:
        return False
    return value.startswith(("$2a$", "$2b$", "$2y$")) and len(value) >= 60


def _load_db_config() -> dict:
    """
    Load MySQL connection settings.
    Priority: Streamlit secrets (mysql.*) then environment variables.
    """
    secrets = {}
    try:
        import streamlit as st

        if "mysql" in st.secrets:
            secrets = dict(st.secrets["mysql"])
    except Exception:
        secrets = {}

    def pick(key: str, default: Optional[str] = None) -> Optional[str]:
        return (
            secrets.get(key)
            or secrets.get(key.upper())
            or os.getenv(key.upper())
            or os.getenv(key)
            or default
        )

    port_raw = pick("port", "3306")
    try:
        port = int(port_raw) if port_raw is not None else 3306
    except (TypeError, ValueError):
        port = 3306

    return {
        "host": pick("host", "localhost"),
        "port": port,
        "user": pick("user", "root"),
        "password": pick("password", ""),
        "database": pick("database", "hr_dashboard"),
    }


def get_connection() -> mysql.connector.MySQLConnection:
    """Create a new MySQL connection using configured settings."""
    return mysql.connector.connect(**_load_db_config())


def ensure_user_table() -> None:
    """Create the users table if it does not exist."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {USER_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(150) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                role ENUM('admin', 'employee') NOT NULL DEFAULT 'employee',
                is_active TINYINT(1) NOT NULL DEFAULT 1
            )
            """
        )
        conn.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    if not password:
        raise ValueError("Password cannot be empty.")
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def login_user(username: str, password: str) -> Tuple[bool, str]:
    """
    Validate user credentials against the database.
    Returns (success, message).
    """
    username = (username or "").strip()
    if not username or not password:
        return False, "Username and password are required."

    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"SELECT password_hash, is_active FROM {USER_TABLE} WHERE username = %s",
            (username,),
        )
        row = cursor.fetchone()
        if not row or not row.get("is_active"):
            return False, "Invalid credentials or inactive user."

        stored_hash = row.get("password_hash", "")
        if not stored_hash:
            return False, "Invalid credentials or inactive user."

        # Primary path: bcrypt hashed password
        if _is_bcrypt_hash(stored_hash):
            try:
                if bcrypt.checkpw(
                    password.encode("utf-8"), stored_hash.encode("utf-8")
                ):
                    return True, ""
                return False, "Invalid credentials or inactive user."
            except ValueError:
                # Stored hash is not valid bcrypt; fall through to legacy handling.
                pass

        # Legacy fallback: plaintext stored password.
        # If it matches, upgrade it to bcrypt immediately.
        if password == stored_hash:
            new_hash = hash_password(password)
            cursor.execute(
                f"UPDATE {USER_TABLE} SET password_hash = %s WHERE username = %s",
                (new_hash, username),
            )
            conn.commit()
            return True, ""

        return False, "Invalid credentials or inactive user."
    except Error:
        return False, "Database error while authenticating."
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def check_role(username: str) -> Optional[str]:
    """Fetch the user's role from the database."""
    username = (username or "").strip()
    if not username:
        return None

    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"SELECT role FROM {USER_TABLE} WHERE username = %s",
            (username,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return row.get("role")
    except Error:
        return None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


__all__ = [
    "USER_TABLE",
    "get_connection",
    "ensure_user_table",
    "hash_password",
    "login_user",
    "check_role",
]
