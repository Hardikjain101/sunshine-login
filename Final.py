"""
================================================================================
HR ATTENDANCE ANALYTICS DASHBOARD
================================================================================
A complete end-to-end data pipeline for HR attendance analysis with
interactive Streamlit dashboard for management decision-making.
Author: HR Analytics Team
Version: 1.0
Date: 2025
================================================================================
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, time, timedelta, date
from typing import Tuple, Dict, List, Optional
import calendar
import warnings
import os
import shutil
import re
from functools import lru_cache
import bcrypt
import mysql.connector
from mysql.connector import Error
warnings.filterwarnings('ignore')

# ===============================
# ACCESS CONTROL SECTION START
# ===============================

# Employee access directory (admin assigns access from this list)
EMPLOYEE_DIRECTORY = [
    "Brenda F Bernard",
    "Stacey Savoy",
    "Dasha S Gary",
    "Megan Blevins",
    "Heather E Broussard",
    "Brittany Domingue",
    "Mhykeisha George",
    "Breanne Roesch",
    "Natalie F Lanclos",
    "Brianna Alfred",
    "Candice Allo",
    "Courtney Jenkins",
    "Kenyelle Hayes",
    "Shelbie Clark",
    "Roshon Tezeno",
    "Jasmine Chavis",
    "Jaime S Woodring",
    "Alexandra Daigle",
    "Susan Broussard",
    "Allison Wilson",
    "Bethany Green",
    "Jazmine Parfait",
    "Keyra Adams",
    "Jasmine Heath",
]

USER_TABLE = "users"
ACCESS_TABLE = "user_employee_access"
ANNOTATION_TABLE = "attendance_annotations"
ANNOTATION_TYPES = ("Open Late", "Early Close", "Special Hours", "Full Off")


def _load_db_config() -> dict:
    """
    Load MySQL connection settings.
    Priority: Streamlit secrets (mysql.*) then environment variables.
    """
    secrets = {}
    try:
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


def _get_connection() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(**_load_db_config())


def _is_bcrypt_hash(value: str) -> bool:
    return bool(value) and value.startswith(("$2a$", "$2b$", "$2y$")) and len(value) >= 60


def _hash_password(password: str) -> str:
    if not password:
        raise ValueError("Password cannot be empty.")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _ensure_access_table() -> None:
    """Create access table if it does not exist."""
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ACCESS_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                employee_name VARCHAR(100) NOT NULL,
                FOREIGN KEY (user_id) REFERENCES {USER_TABLE}(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _normalize_annotation_type(value: str) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lower() == "closed":
        return "Full Off"
    for allowed in ANNOTATION_TYPES:
        if raw.lower() == allowed.lower():
            return allowed
    return None


def _ensure_annotation_table() -> None:
    """
    Create annotation table if it does not exist.
    One annotation is stored per calendar date (UNIQUE date).
    """
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ANNOTATION_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                `date` DATE NOT NULL,
                annotation_type ENUM('Open Late', 'Early Close', 'Special Hours', 'Full Off') NOT NULL,
                reason TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_annotation_date (`date`)
            )
            """
        )
        conn.commit()
    except Exception:
        # Annotation persistence must remain optional-safe if DB access is limited.
        pass
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@st.cache_data(show_spinner=False)
def load_annotations_from_db() -> Tuple[Tuple[str, str, str], ...]:
    """
    Fetch all persisted annotations.
    Returns tuple rows: (YYYY-MM-DD, annotation_type, reason)
    """
    conn = None
    cursor = None
    rows: List[Tuple[str, str, str]] = []
    try:
        _ensure_annotation_table()
        conn = _get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT `date`, annotation_type, reason
            FROM {ANNOTATION_TABLE}
            ORDER BY `date`
            """
        )
        for row in (cursor.fetchall() or []):
            raw_date = row.get("date")
            if pd.isna(raw_date):
                continue
            if isinstance(raw_date, datetime):
                date_str = raw_date.date().strftime("%Y-%m-%d")
            elif isinstance(raw_date, date):
                date_str = raw_date.strftime("%Y-%m-%d")
            else:
                date_str = str(raw_date)
            ann_type = _normalize_annotation_type(row.get("annotation_type")) or "Special Hours"
            reason = str(row.get("reason") or "").strip()
            rows.append((date_str, ann_type, reason))
    except Exception:
        return tuple()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    return tuple(rows)


def upsert_annotation(annotation_date: date, annotation_type: str, reason: str = "") -> bool:
    """
    Insert or update a date annotation (one row per date).
    """
    normalized_type = _normalize_annotation_type(annotation_type)
    if annotation_date is None or not normalized_type:
        return False

    conn = None
    cursor = None
    try:
        _ensure_annotation_table()
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO {ANNOTATION_TABLE} (`date`, annotation_type, reason)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                annotation_type = VALUES(annotation_type),
                reason = VALUES(reason),
                created_at = CURRENT_TIMESTAMP
            """,
            (annotation_date, normalized_type, str(reason or "").strip()),
        )
        conn.commit()
        load_annotations_from_db.clear()
        return True
    except Exception:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def delete_annotation(annotation_date: date) -> bool:
    """
    Delete an annotation by date.
    """
    if annotation_date is None:
        return False
    conn = None
    cursor = None
    try:
        _ensure_annotation_table()
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM {ANNOTATION_TABLE} WHERE `date` = %s",
            (annotation_date,),
        )
        conn.commit()
        load_annotations_from_db.clear()
        return True
    except Exception:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def get_annotation_items_from_db() -> Tuple[Tuple[str, str, str], ...]:
    """
    Public helper to load persisted annotations.
    """
    return load_annotations_from_db()


def build_special_day_map(
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]]
) -> Dict[date, Dict[str, str]]:
    """
    Convert persisted/UI annotation tuple into a date-keyed mapping.
    """
    special_day_map: Dict[date, Dict[str, str]] = {}
    if not special_day_items:
        return special_day_map

    for date_str, day_type, reason in special_day_items:
        try:
            day_date = datetime.strptime(str(date_str), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        normalized_type = _normalize_annotation_type(day_type)
        if not normalized_type:
            continue
        special_day_map[day_date] = {
            'type': normalized_type,
            'reason': str(reason or '').strip()
        }
    return special_day_map


def _parse_hhmm(text: str) -> Optional[time]:
    text = str(text or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%H:%M")
        return parsed.time().replace(second=0, microsecond=0)
    except ValueError:
        return None


def _add_minutes_to_time(base: time, minutes: int) -> time:
    anchor = datetime.combine(date.today(), base)
    shifted = anchor + timedelta(minutes=int(minutes))
    return shifted.time().replace(second=0, microsecond=0)


def parse_special_hours_reason(reason: str) -> Tuple[Optional[time], Optional[time], str]:
    """
    Parse a reason payload that may start with "HH:MM-HH:MM".
    Returns: (open_time, close_time, display_reason_without_prefix).
    """
    raw = str(reason or "").strip()
    if not raw:
        return None, None, ""

    match = re.match(
        r"^\s*(\d{1,2}:\d{2})\s*(?:-|to)\s*(\d{1,2}:\d{2})(?:\s*[\|\-]\s*(.*))?\s*$",
        raw,
        flags=re.IGNORECASE
    )
    if not match:
        return None, None, raw

    open_time = _parse_hhmm(match.group(1))
    close_time = _parse_hhmm(match.group(2))
    remaining = (match.group(3) or "").strip()
    if open_time is None or close_time is None:
        return None, None, raw
    return open_time, close_time, remaining


def format_special_hours_reason(open_time: time, close_time: time, reason: str = "") -> str:
    """
    Persist special-hours as "HH:MM-HH:MM | reason" in reason text.
    """
    base = f"{open_time.strftime('%H:%M')}-{close_time.strftime('%H:%M')}"
    reason_text = str(reason or "").strip()
    if reason_text:
        return f"{base} | {reason_text}"
    return base


def authenticate_user(username: str, password: str) -> Tuple[bool, str, Optional[Dict[str, object]]]:
    """
    Validate credentials and return user info.
    Returns (success, message, user_dict).
    """
    username = (username or "").strip()
    if not username or not password:
        return False, "Username and password are required.", None

    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT id, username, password_hash, role, is_active
            FROM {USER_TABLE}
            WHERE username = %s
            """,
            (username,),
        )
        row = cursor.fetchone()
        if not row:
            return False, "Invalid credentials or inactive user.", None

        is_active = row.get("is_active", 1)
        if is_active in (0, False, "0"):
            return False, "Invalid credentials or inactive user.", None

        stored_hash = row.get("password_hash", "")
        if _is_bcrypt_hash(stored_hash):
            try:
                if bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
                    return True, "", row
                return False, "Invalid credentials or inactive user.", None
            except ValueError:
                pass

        # Legacy fallback: plaintext stored password. If it matches, upgrade.
        if password == stored_hash:
            new_hash = _hash_password(password)
            cursor.execute(
                f"UPDATE {USER_TABLE} SET password_hash = %s WHERE username = %s",
                (new_hash, username),
            )
            conn.commit()
            row["password_hash"] = new_hash
            return True, "", row

        return False, "Invalid credentials or inactive user.", None
    except Error:
        return False, "Database error while authenticating.", None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _get_current_user_record() -> Optional[Dict[str, object]]:
    """Fetch the active database record for the signed-in user."""
    if not st.session_state.get("auth_authenticated"):
        return None

    auth_user_id = st.session_state.get("auth_user_id")
    auth_user = (st.session_state.get("auth_user") or "").strip()
    if not auth_user_id or not auth_user:
        return None

    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT id, username, role, is_active
            FROM {USER_TABLE}
            WHERE id = %s AND username = %s
            """,
            (auth_user_id, auth_user),
        )
        row = cursor.fetchone()
        if not row:
            return None

        row["role"] = (row.get("role") or "employee").strip().lower()
        row["is_active"] = int(row.get("is_active", 0) or 0)
        if row["is_active"] != 1:
            return None
        return row
    except Exception:
        return None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _current_user_is_admin() -> bool:
    """Require both session role and database role to be admin."""
    session_role = (st.session_state.get("auth_role") or "").strip().lower()
    if session_role != "admin":
        return False

    current_user = _get_current_user_record()
    if not current_user:
        return False
    return current_user.get("role") == "admin"


def get_allowed_employees(user_id: int) -> List[str]:
    """Fetch allowed employee names for a user."""
    conn = None
    cursor = None
    try:
        _ensure_access_table()
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT employee_name FROM {ACCESS_TABLE} WHERE user_id = %s",
            (user_id,),
        )
        rows = cursor.fetchall() or []
        return [row[0] for row in rows if row and row[0]]
    except Error:
        return []
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def delete_user_access(user_id: int) -> None:
    """Remove all employee access rows for a user."""
    conn = None
    cursor = None
    try:
        _ensure_access_table()
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM {ACCESS_TABLE} WHERE user_id = %s",
            (user_id,),
        )
        conn.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def save_employee_access(user_id: int, employee_list: List[str]) -> None:
    """Replace a user's employee access list."""
    employee_list = [e for e in (employee_list or []) if e]
    delete_user_access(user_id)
    if not employee_list:
        return

    conn = None
    cursor = None
    try:
        _ensure_access_table()
        conn = _get_connection()
        cursor = conn.cursor()
        payload = [(user_id, name) for name in employee_list]
        cursor.executemany(
            f"INSERT INTO {ACCESS_TABLE} (user_id, employee_name) VALUES (%s, %s)",
            payload,
        )
        conn.commit()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _fetch_all_users(exclude_user_id: Optional[int] = None) -> List[Dict[str, object]]:
    """Fetch all users, optionally excluding a user id."""
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor(dictionary=True)
        if exclude_user_id:
            cursor.execute(
                f"""
                SELECT id, username, role, is_active
                FROM {USER_TABLE}
                WHERE id <> %s
                ORDER BY username
                """,
                (exclude_user_id,),
            )
        else:
            cursor.execute(
                f"""
                SELECT id, username, role, is_active
                FROM {USER_TABLE}
                ORDER BY username
                """
            )
        rows = cursor.fetchall() or []
        normalized = []
        for row in rows:
            if not row:
                continue
            row["is_active"] = int(row.get("is_active", 1) or 0)
            normalized.append(row)
        return normalized
    except Exception:
        return []
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _set_user_active(user_id: int, is_active: int) -> bool:
    """Activate or deactivate a user by id."""
    conn = None
    cursor = None
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE {USER_TABLE} SET is_active = %s WHERE id = %s",
            (int(is_active), user_id),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _update_user_password(user_id: int, new_password: str) -> bool:
    """Reset a user's password using bcrypt."""
    conn = None
    cursor = None
    try:
        pw_hash = _hash_password(new_password)
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE {USER_TABLE} SET password_hash = %s WHERE id = %s",
            (pw_hash, user_id),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _change_current_user_password(
    current_password: str,
    new_password: str,
    confirm_password: str,
) -> Tuple[bool, str]:
    """Allow a signed-in user to change only their own password."""
    current_user = _get_current_user_record()
    if not current_user:
        return False, "Unable to identify the current user."

    if not current_password or not new_password or not confirm_password:
        return False, "Current password, new password, and confirmation are required."
    if new_password != confirm_password:
        return False, "New password and confirmation do not match."

    auth_ok, _, auth_row = authenticate_user(current_user["username"], current_password)
    if not auth_ok or not auth_row:
        return False, "Current password is incorrect."

    try:
        authenticated_user_id = int(auth_row.get("id"))
        current_user_id = int(current_user.get("id"))
    except (TypeError, ValueError):
        return False, "Unable to verify the current user."

    if authenticated_user_id != current_user_id:
        return False, "Unable to verify the current user."
    if not _update_user_password(current_user_id, new_password):
        return False, "Database error while updating password."
    return True, "Password updated successfully."


def _filter_df_by_employees(df: pd.DataFrame, allowed: List[str]) -> pd.DataFrame:
    if df is None:
        return df
    if allowed == "ALL":
        return df
    if not allowed:
        return df.iloc[0:0]
    allowed_set = set(allowed)
    for col in ("Employee Full Name", "Employee", "Employee Name"):
        if col in df.columns:
            return df[df[col].isin(allowed_set)].copy()
    return df


def _build_employee_filter_clause(allowed: List[str]) -> Tuple[str, Tuple]:
    """
    Build a safe SQL filter clause for employee_name.
    Example output: ("WHERE employee_name IN (%s, %s)", ("A", "B"))
    """
    if not allowed:
        return "WHERE 1=0", tuple()
    placeholders = ", ".join(["%s"] * len(allowed))
    return f"WHERE employee_name IN ({placeholders})", tuple(allowed)


def render_account_security_panel() -> None:
    """Self-service account security controls for the signed-in user."""
    st.caption("Change your own password.")
    with st.form("change_my_password_form", clear_on_submit=True):
        current_password = st.text_input("Current Password", type="password")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")
        submitted = st.form_submit_button("Change Password")

    if submitted:
        ok, message = _change_current_user_password(
            current_password,
            new_password,
            confirm_password,
        )
        if ok:
            st.success(message)
        else:
            st.error(message)


def render_admin_panel() -> None:
    """Admin UI to create users and assign employee access."""
    if not _current_user_is_admin():
        st.info("Admin access required.")
        return

    st.subheader("Admin Panel")
    st.markdown("Create users and assign employee access.")

    with st.form("create_user_form", clear_on_submit=True):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        role = st.selectbox("Role", ["employee", "admin"])

        selected_employees: List[str] = []
        if role == "employee":
            selected_employees = st.multiselect(
                "Assign Employees",
                EMPLOYEE_DIRECTORY,
            )

        submitted = st.form_submit_button("Create User")

    if submitted:
        if not username or not password:
            st.error("Username and password are required.")
            return
        if role == "employee" and not selected_employees:
            st.error("Select at least one employee for this user.")
            return

        conn = None
        cursor = None
        try:
            conn = _get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT id FROM {USER_TABLE} WHERE username = %s",
                (username,),
            )
            if cursor.fetchone():
                st.error("User already exists.")
                return

            pw_hash = _hash_password(password)
            cursor.execute(
                f"""
                INSERT INTO {USER_TABLE} (username, password_hash, role, is_active)
                VALUES (%s, %s, %s, %s)
                """,
                (username, pw_hash, role, 1),
            )
            conn.commit()
            user_id = cursor.lastrowid

            if role == "employee":
                save_employee_access(user_id, selected_employees)

            st.success("User created.")
        except Error:
            st.error("Database error while creating user.")
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    # =====================================
    # ADMIN USER MANAGEMENT SECTION START
    # =====================================
    st.markdown("---")
    st.subheader("Manage Existing Users")
    st.caption("Update status, reset passwords, and modify employee access.")

    current_admin_id = st.session_state.get("auth_user_id")
    users = _fetch_all_users(exclude_user_id=current_admin_id)

    if not users:
        st.info("No other users found.")
    else:
        user_by_id = {user.get("id"): user for user in users if user.get("id") is not None}
        if not user_by_id:
            st.info("No other users found.")
        else:
            selected_user_id = st.selectbox(
                "Select User",
                options=list(user_by_id.keys()),
                format_func=lambda uid: (
                    f"{user_by_id[uid].get('username', 'Unknown')} "
                    f"({user_by_id[uid].get('role', 'employee')})"
                ),
                key="admin_manage_user_select",
            )

            selected_user = user_by_id.get(selected_user_id, {})
            username_value = selected_user.get("username", "")
            role_value = selected_user.get("role", "")
            is_active_value = int(selected_user.get("is_active", 1) or 0)

            st.text_input(
                "Username",
                value=username_value,
                disabled=True,
                key=f"manage_username_{selected_user_id}",
            )
            st.text_input(
                "Role",
                value=role_value,
                disabled=True,
                key=f"manage_role_{selected_user_id}",
            )
            st.text_input(
                "Status",
                value="Active" if is_active_value == 1 else "Inactive",
                disabled=True,
                key=f"manage_status_{selected_user_id}",
            )

            toggle_label = "Deactivate User" if is_active_value == 1 else "Reactivate User"
            if st.button(toggle_label, key=f"toggle_user_{selected_user_id}"):
                target_status = 0 if is_active_value == 1 else 1
                if _set_user_active(selected_user_id, target_status):
                    st.success("User status updated.")
                    if hasattr(st, "rerun"):
                        st.rerun()
                    else:
                        st.experimental_rerun()
                else:
                    st.error("Database error while updating user status.")

            st.markdown("---")
            st.markdown("**Reset Password**")
            new_password = st.text_input(
                "New Password",
                type="password",
                key=f"reset_pw_{selected_user_id}",
            )
            if st.button("Reset Password", key=f"reset_pw_btn_{selected_user_id}"):
                if not new_password:
                    st.error("New password is required.")
                elif _update_user_password(selected_user_id, new_password):
                    st.success("Password reset successfully.")
                    st.session_state.pop(f"reset_pw_{selected_user_id}", None)
                    if hasattr(st, "rerun"):
                        st.rerun()
                    else:
                        st.experimental_rerun()
                else:
                    st.error("Database error while resetting password.")

            st.markdown("---")
            st.markdown("**Assigned Employees**")
            current_access = get_allowed_employees(selected_user_id)
            if current_access:
                st.caption("Currently assigned: " + ", ".join(current_access))
            else:
                st.caption("Currently assigned: None")

            available_employees = EMPLOYEE_DIRECTORY
            default_access = [e for e in current_access if e in available_employees]
            updated_access = st.multiselect(
                "Modify Employee Access",
                available_employees,
                default=default_access,
                key=f"access_select_{selected_user_id}",
            )
            if st.button("Update Access", key=f"update_access_{selected_user_id}"):
                try:
                    save_employee_access(selected_user_id, updated_access)
                    st.success("Employee access updated.")
                except Exception:
                    st.error("Database error while updating employee access.")

    # =====================================
    # ADMIN USER MANAGEMENT SECTION END
    # =====================================


# ===============================
# ACCESS CONTROL SECTION END
# ===============================

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

class Config:
    """Centralized configuration for business rules and thresholds"""
    
    # Persistent Storage
    DATA_FILE_PATH = "attendance_data.xlsx"
    CACHE_VERSION = "2026-02-19"
    
    # Department Auto-Mapping
    DEPARTMENT_MAPPING = {
        'Keyra': 'ex employees',
        'Brianna': 'ex employees',
        'Candice': 'Counselors',
        'Brenda': 'Mid Office',
        'Megan': 'Front Desk',
        'Heather': 'Mid Office',
        'Shelbie': 'ex employees',
        'Brittany': 'Mid Office',
        'Dasha': 'Front Desk',
        'Mhykeisha': 'Nurse practitioner',
        'Alexandra': 'Front Desk',
        'Bethany': 'Mid Office',
        'Kenyelle': 'Mid Office',
        'Jasmine': 'Mid Office',
        'Courtney': 'ex employees',
        'Jazmine': 'ex employees',
        'Breanne': 'Nurse practitioner',
        'Stacey': 'Nurse practitioner',
        'Allison': 'Nurse practitioner',
        'Jaime': 'Nurse practitioner',
        'Natalie': 'Front Desk',
        'Roshon': 'Mid Office',
        'Susan': 'Nurse practitioner'
    }
    
    # Business hours configuration
    STANDARD_START_TIME = time(8, 0)      # 8:00 AM
    LATE_GRACE_PERIOD_END = time(8, 8)    # 8:08 AM
    VERY_LATE_THRESHOLD = time(8, 15)     # 8:15 AM
    STANDARD_END_TIME = time(17, 0)       # 5:00 PM
    
    # Early Departure Times
    EARLY_DEPARTURE_TIME_MON_THU = time(16, 30) # 4:30 PM
    EARLY_DEPARTURE_TIME_FRI = time(12, 15)   # 12:15 PM
    
    # Friday Full Day Threshold
    FRIDAY_FULL_DAY_PUNCH_OUT = time(12, 15) # 12:15 PM
    
    # Working hours thresholds
    MIN_WORK_HOURS = 4.0                   # Minimum daily hours
    MAX_WORK_HOURS = 10.0                  # Maximum reasonable hours
    HALF_DAY_THRESHOLD = 5.0               # Hours for half-day
    MON_THU_FULL_DAY_THRESHOLD = 6.5       # Mon-Thu only full-day threshold
    FULL_DAY_THRESHOLD = 8.0               # Hours for full-day

    # Meal-risk visual thresholds (calendar cues only)
    MEAL_RISK_LONG_DAY_HOURS = 8.0         # Long day threshold for risk cues
    MEAL_RISK_WARNING_MINUTES = 30         # Warning if meal is shorter than this
    MEAL_RISK_CRITICAL_MINUTES = 1         # Critical if meal is essentially zero
    
    # Overtime thresholds
    WEEKLY_STANDARD_HOURS = 40.0           # Weekly standard hours
    MONTHLY_STANDARD_HOURS = 160.0         # Monthly standard hours
    
    # Anomaly detection
    DUPLICATE_THRESHOLD_MINUTES = 5        # Minutes to detect duplicates
    EXCESSIVE_SHIFT_HOURS = 10.0           # Flag excessive shifts
    SHORT_SHIFT_HOURS = 4.0                # Flag short shifts
    
    # Consistency scoring
    EXPECTED_WORKING_DAYS = 22             # Expected days per month

    # Full-name department overrides (more specific than first-name mapping)
    FULL_NAME_DEPARTMENT_MAPPING = {
        'Alexandra Daigle': 'Front Desk',
        'Bethany Green': 'Mid Office',
        'Jazmine Parfait': 'ex employees',
        'Brianna Alfred': 'ex employees'
    }

    # Punch cleaning / meal sanity guards
    NEAR_DUP_SECONDS = 60                  # Collapse noisy repeats within this window
    MEAL_SANITY_HOURS = 4.0                # Upper bound for believable meal gaps
    MEAL_REALISTIC_MAX_HOURS = 3.0         # Preferred upper bound when choosing alternates
    ENABLE_PUNCH_CLEANING = True           # Toggle to revert to legacy per-day punch cleaning
    ENABLE_PUNCH_SANITY_GUARD = True       # Toggle to revert to legacy pairing if needed
    REGRESSION_CHANGE_LIMIT = 0.01         # 1% max allowed day-level changes in debug check

# ============================================================================
# HOLIDAY UTILITIES
# ============================================================================

def _get_easter_date(year: int) -> date:
    """Compute Easter Sunday date for the given year (Gregorian calendar)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)

@lru_cache(maxsize=None)
def get_company_holidays(year: int) -> Dict[date, str]:
    """
    Return a mapping of holiday dates to names for the given year.
    Holidays are based on actual calendar dates (no observed shifts).
    """
    easter = _get_easter_date(year)

    # Fixed-date holidays
    holidays = {
        date(year, 1, 1): "New Year's Day",
        date(year, 7, 4): "Independence Day",
        date(year, 12, 24): "Christmas Eve",
        date(year, 12, 25): "Christmas Day"
    }

    # Movable feasts based on Easter
    holidays[easter - timedelta(days=47)] = "Mardi Gras"
    holidays[easter - timedelta(days=2)] = "Good Friday"

    # Memorial Day: last Monday in May
    memorial = date(year, 5, 31)
    while memorial.weekday() != 0:
        memorial -= timedelta(days=1)
    holidays[memorial] = "Memorial Day"

    # Labor Day: first Monday in September
    labor = date(year, 9, 1)
    while labor.weekday() != 0:
        labor += timedelta(days=1)
    holidays[labor] = "Labor Day"

    # Thanksgiving Day: fourth Thursday in November
    thanksgiving = date(year, 11, 1)
    while thanksgiving.weekday() != 3:
        thanksgiving += timedelta(days=1)
    thanksgiving += timedelta(weeks=3)
    holidays[thanksgiving] = "Thanksgiving Day"

    return holidays

def get_holiday_map(year: int) -> Dict[date, str]:
    """Deterministic holiday lookup for a given year (cache-safe wrapper)."""
    return get_company_holidays(year)


def get_effective_holiday_map(
    year: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> Dict[date, str]:
    """
    Holiday map extended with persisted Full Off annotations.
    """
    holiday_map = get_holiday_map(year).copy()
    special_day_map = build_special_day_map(special_day_items)
    for day_date, meta in special_day_map.items():
        if day_date.year == year and meta.get('type') == 'Full Off':
            holiday_map[day_date] = "Full Off"
    return holiday_map

def get_company_holiday_set(start_date: date, end_date: date) -> set:
    """Return a set of holiday dates across a date range (inclusive)."""
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    holiday_set = set()
    for year in range(start_date.year, end_date.year + 1):
        holiday_set.update(get_company_holidays(year).keys())
    return holiday_set

# ============================================================================
# PHASE 1: DATA CLEANING & STANDARDIZATION
# ============================================================================

class DataCleaner:
    """Handles all data cleaning and standardization operations"""
    
    @staticmethod
    def fill_missing_departments(df: pd.DataFrame) -> pd.DataFrame:
        """Auto-fill missing departments based on first name mapping"""
        if 'Department' not in df.columns:
            df['Department'] = np.nan
            
        def get_dept(row):
            # Specific full-name overrides take precedence over imported values.
            full_name = str(row.get('Employee Full Name', '')).strip().title()
            if full_name:
                mapped_full = Config.FULL_NAME_DEPARTMENT_MAPPING.get(full_name)
                if mapped_full:
                    return mapped_full
            curr = row.get('Department')
            if pd.notna(curr) and str(curr).strip() not in ['', 'nan', 'None', 'Unknown']:
                return curr
            fname = str(row.get('Employee First Name', '')).strip().title()
            return Config.DEPARTMENT_MAPPING.get(fname, 'Unknown')
            
        df['Department'] = df.apply(get_dept, axis=1)
        return df

    @staticmethod
    def standardize_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine and standardize employee names
        Creates: Employee Full Name with proper Title Case
        """
        # Handle missing middle names
        df['Employee Middle Name'] = df['Employee Middle Name'].fillna('')
        
        # Combine names with proper spacing
        def combine_name(row):
            first = str(row.get('Employee First Name', '')).strip()
            middle = str(row.get('Employee Middle Name', '')).strip()
            last = str(row.get('Employee Last Name', '')).strip()
            
            # Build full name
            parts = [first, middle, last] if middle else [first, last]
            full_name = ' '.join(filter(None, parts))
            
            # Apply Title Case and clean extra spaces
            return ' '.join(full_name.split()).title()
        
        df['Employee Full Name'] = df.apply(combine_name, axis=1)
        
        return df
    
    @staticmethod
    def clean_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert and standardize all datetime columns
        Creates authoritative timestamp and derived date/time fields
        """
        # Define datetime columns
        datetime_cols = {
            'Actual Date Time': 'actual_dt',
            'Punch Date Time': 'punch_dt',
            'Created Date Time (UTC)': 'created_dt'
        }
        
        # Convert to datetime
        for col, alias in datetime_cols.items():
            if col in df.columns:
                df[alias] = pd.to_datetime(df[col], errors='coerce')
        
        # Select authoritative timestamp (prefer Actual, fallback to Punch)
        df['Timestamp'] = df['actual_dt'].fillna(df['punch_dt'])
        
        # Create derived fields
        df['Punch Date'] = df['Timestamp'].dt.date
        df['Punch Time'] = df['Timestamp'].dt.time
        df['Day of Week'] = df['Timestamp'].dt.day_name()
        df['Week Number'] = df['Timestamp'].dt.isocalendar().week
        df['Month'] = df['Timestamp'].dt.month
        df['Month Name'] = df['Timestamp'].dt.strftime('%B')
        df['Year'] = df['Timestamp'].dt.year
        df['Hour'] = df['Timestamp'].dt.hour
        df['Date'] = pd.to_datetime(df['Punch Date'])
        
        return df
    
    @staticmethod
    def detect_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Identify duplicate punches within threshold timeframe
        Creates: Duplicate Punch flag
        """
        df = df.sort_values(['Employee Number', 'Timestamp'])
        df['Time_Diff_Minutes'] = df.groupby('Employee Number')['Timestamp'].diff().dt.total_seconds() / 60
        
        # Flag duplicates (within threshold)
        df['Duplicate Punch'] = (
            (df['Time_Diff_Minutes'] > 0) & 
            (df['Time_Diff_Minutes'] <= Config.DUPLICATE_THRESHOLD_MINUTES)
        )
        
        return df
    
    @staticmethod
    def create_data_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create flags for missing and invalid data
        """
        # Missing punch type
        if 'Type' in df.columns:
            df['Missing Punch Type'] = df['Type'].isna() | (df['Type'] == '')
        else:
            df['Missing Punch Type'] = False
        
        # Missing timestamp
        df['Missing Timestamp'] = df['Timestamp'].isna()
        
        # Missing employee info
        df['Incomplete Employee Record'] = (
            df['Employee Full Name'].isna() | 
            (df['Employee Full Name'] == '') |
            df['Employee Number'].isna()
        )
        
        return df
    
    @staticmethod
    def clean_system_metadata(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize system-related fields
        """
        # Normalize punch sources
        if 'Source' in df.columns:
            df['Source Normalized'] = df['Source'].fillna('Unknown').str.strip().str.title()
        
        # Clean IP addresses
        if 'IP Address' in df.columns:
            df['IP Address Clean'] = df['IP Address'].fillna('Unknown').str.strip()
        
        # Handle location and door
        if 'Location' in df.columns:
            df['Location'] = df['Location'].fillna('Not Specified')
        
        if 'Door' in df.columns:
            df['Door'] = df['Door'].fillna('Not Specified')
        
        return df

# ============================================================================
# PHASE 2: FEATURE ENGINEERING (BUSINESS LOGIC)
# ============================================================================

class FeatureEngineer:
    """Creates derived business metrics and features"""

    @staticmethod
    def _clean_punch_times(
        punch_group: pd.DataFrame,
        near_dup_seconds: int = Config.NEAR_DUP_SECONDS
    ) -> Tuple[List[pd.Timestamp], Dict[str, object]]:
        """
        Collapse exact and near-duplicate punches for a single employee-day.
        Preserves the very first and very last punch while removing obvious noise.
        """
        times = punch_group['Timestamp'].dropna().sort_values().tolist()
        if not times:
            return [], {
                'raw_count': 0,
                'clean_count': 0,
                'removed_exact': 0,
                'removed_near': 0,
                'first_raw': None,
                'last_raw': None
            }

        first_raw, last_raw = times[0], times[-1]
        cleaned: List[pd.Timestamp] = []
        removed_exact = 0
        removed_near = 0

        for idx, ts in enumerate(times):
            if not cleaned:
                cleaned.append(ts)
                continue

            diff_seconds = (ts - cleaned[-1]).total_seconds()
            is_last_raw = (ts == last_raw)

            # Exact duplicate -> drop, keep earlier metadata by default
            if diff_seconds == 0:
                removed_exact += 1
                continue

            # Near-duplicate noise (same direction punches) -> drop unless it's the final punch
            if diff_seconds <= near_dup_seconds and not is_last_raw:
                removed_near += 1
                continue

            cleaned.append(ts)

        # Protect first-in and last-out explicitly
        if cleaned[0] != first_raw:
            cleaned.insert(0, first_raw)
        if last_raw not in cleaned:
            cleaned.append(last_raw)

        debug_info = {
            'raw_count': len(times),
            'clean_count': len(cleaned),
            'removed_exact': removed_exact,
            'removed_near': removed_near,
            'first_raw': first_raw,
            'last_raw': last_raw
        }
        return cleaned, debug_info

    @staticmethod
    def _compute_work_and_break(punch_times: List[pd.Timestamp]) -> Dict[str, object]:
        """
        Pair punches sequentially (0->1, 2->3...) and compute work/break metrics.
        """
        work_seconds = 0.0
        break_seconds = 0.0
        valid_pairs = 0
        invalid_pairs = 0
        break_issues = 0
        break_count = 0
        first_valid_in = None
        last_valid_out = None
        max_break_gap_seconds = 0.0

        for idx in range(0, len(punch_times) - 1, 2):
            start = punch_times[idx]
            end = punch_times[idx + 1]
            if end > start:
                if first_valid_in is None:
                    first_valid_in = start
                last_valid_out = end
                valid_pairs += 1
                work_seconds += (end - start).total_seconds()

                next_idx = idx + 2
                if next_idx < len(punch_times):
                    next_in = punch_times[next_idx]
                    if next_in > end:
                        gap_seconds = (next_in - end).total_seconds()
                        break_seconds += gap_seconds
                        max_break_gap_seconds = max(max_break_gap_seconds, gap_seconds)
                        break_count += 1
                    else:
                        break_issues += 1
            else:
                invalid_pairs += 1

        meal_hours = max(0.0, break_seconds / 3600)
        work_hours = max(0.0, work_seconds / 3600)

        return {
            'work_seconds': work_seconds,
            'break_seconds': break_seconds,
            'meal_hours': meal_hours,
            'work_hours': work_hours,
            'valid_pairs': valid_pairs,
            'invalid_pairs': invalid_pairs,
            'break_issues': break_issues,
            'break_count': break_count,
            'first_valid_in': first_valid_in,
            'last_valid_out': last_valid_out,
            'max_break_gap_seconds': max_break_gap_seconds
        }

    @staticmethod
    def _try_alternate_pairing(punch_times: List[pd.Timestamp]) -> Tuple[Optional[List[pd.Timestamp]], Optional[Dict[str, object]], str]:
        """
        Attempt a minimal alternate pairing by dropping one interior punch that causes
        unrealistic meal gaps while preserving first-in and last-out.
        Returns (candidate_times, metrics, reason) or (None, None, reason) if no better option.
        """
        if len(punch_times) < 3:
            return None, None, "too_few_punches"

        candidates = []
        for drop_idx in range(1, len(punch_times) - 1):  # never drop first/last
            candidate = [ts for i, ts in enumerate(punch_times) if i != drop_idx]
            if len(candidate) % 2 != 0:
                continue
            metrics = FeatureEngineer._compute_work_and_break(candidate)
            last_out = metrics.get('last_valid_out')
            if last_out is None or pd.isna(last_out):
                continue
            candidates.append((candidate, metrics, drop_idx))

        if not candidates:
            return None, None, "no_even_candidate"

        # Prefer smallest meal_hours, then smallest max_break_gap_seconds
        candidates.sort(key=lambda c: (c[1].get('meal_hours', float('inf')), c[1].get('max_break_gap_seconds', float('inf'))))
        best_candidate, best_metrics, drop_idx = candidates[0]
        return best_candidate, best_metrics, f"dropped_idx_{drop_idx}"
    
    @staticmethod
    def calculate_daily_attendance(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate daily attendance metrics per employee
        Returns: Daily summary DataFrame
        """
        # Filter valid records and exclude weekends
        valid_df = df[
            (~df['Missing Timestamp']) & 
            (~df['Incomplete Employee Record']) &
            (~df['Duplicate Punch']) &
            (~df['Day of Week'].isin(['Saturday', 'Sunday']))
        ].copy()
        
        # Group by employee and date
        daily_records = []

        def normalize_type(series: pd.Series) -> pd.Series:
            return series.fillna('').astype(str).str.strip().str.lower()
        
        for (emp_num, emp_name), group in valid_df.groupby(['Employee Number', 'Employee Full Name']):
            for date, date_group in group.groupby('Punch Date'):
                date_group = date_group.sort_values('Timestamp')
                raw_punch_count = len(date_group)
                raw_day_times = date_group['Timestamp'].dropna()
                day_first_raw = raw_day_times.iloc[0] if not raw_day_times.empty else pd.NaT
                day_last_raw = raw_day_times.iloc[-1] if not raw_day_times.empty else pd.NaT
                leading_meal_gap_seconds = 0.0
                used_normal_subset = False

                # Prefer Normal punches when available; fall back to all punches when
                # normal-only creates odd meal flows (common with duplicate meal toggles).
                punch_group = date_group.copy()
                if 'Type' in punch_group.columns:
                    type_norm = normalize_type(punch_group['Type'])
                    has_normal = (type_norm == 'normal').any()
                    if has_normal:
                        normal_group = punch_group[(type_norm == 'normal') | (type_norm == '')]
                        if not normal_group.empty:
                            normal_times = normal_group['Timestamp'].dropna()
                            has_meal = (type_norm == 'meal').any()
                            normal_odd = (len(normal_times) % 2 != 0)

                            if not (has_meal and normal_odd):
                                punch_group = normal_group
                                used_normal_subset = True

                            # If a day starts in Meal mode before the first Normal punch, treat
                            # that lead gap as a meal interval to avoid losing valid lunch time.
                            if (
                                used_normal_subset and
                                len(type_norm) > 0 and
                                type_norm.iloc[0] == 'meal' and
                                pd.notna(day_first_raw) and
                                (not normal_times.empty) and
                                (normal_times.iloc[0] > day_first_raw)
                            ):
                                gap_seconds = (normal_times.iloc[0] - day_first_raw).total_seconds()
                                if 0 < gap_seconds <= (Config.MEAL_SANITY_HOURS * 3600):
                                    leading_meal_gap_seconds = gap_seconds
                    if punch_group.empty:
                        punch_group = date_group.copy()

                punch_group = punch_group.dropna(subset=['Timestamp']).sort_values('Timestamp')

                # Remove exact duplicates without collapsing distinct same-timestamp events
                dedupe_cols = [
                    'Timestamp', 'Type', 'Mode', 'Source', 'Clock', 'Door', 'Location',
                    'Status', 'Notes', 'Additional Notes', 'User', 'IP Address'
                ]
                dedupe_cols = [col for col in dedupe_cols if col in punch_group.columns]
                if len(dedupe_cols) > 1:
                    punch_group = punch_group.drop_duplicates(subset=dedupe_cols)
                if Config.ENABLE_PUNCH_CLEANING:
                    punch_times, clean_debug = FeatureEngineer._clean_punch_times(punch_group)
                else:
                    punch_times = punch_group['Timestamp'].tolist()
                    clean_debug = {
                        'raw_count': len(punch_times),
                        'clean_count': len(punch_times),
                        'removed_exact': 0,
                        'removed_near': 0,
                        'first_raw': punch_times[0] if punch_times else None,
                        'last_raw': punch_times[-1] if punch_times else None
                    }
                punch_count = len(punch_times)

                # Count Normal punches in the sequence (if available)
                if 'Type' in punch_group.columns:
                    normal_punch_count = int((normalize_type(punch_group['Type']) == 'normal').sum())
                else:
                    normal_punch_count = punch_count

                # Compute work/break intervals with pairing guard
                metrics = FeatureEngineer._compute_work_and_break(punch_times)
                work_seconds = metrics['work_seconds']
                break_seconds = metrics['break_seconds']
                valid_pairs = metrics['valid_pairs']
                invalid_pairs = metrics['invalid_pairs']
                break_issues = metrics['break_issues']
                break_count = metrics['break_count']
                first_valid_in = metrics['first_valid_in']
                last_valid_out = metrics['last_valid_out']
                max_break_gap_seconds = metrics['max_break_gap_seconds']
                meal_hours = metrics['meal_hours']
                alt_applied = False
                alt_reason = None
                guard_triggered = False

                if Config.ENABLE_PUNCH_SANITY_GUARD:
                    needs_guard = (
                        (punch_count % 2 != 0) or
                        (meal_hours > Config.MEAL_SANITY_HOURS) or
                        ((max_break_gap_seconds / 3600) > Config.MEAL_SANITY_HOURS)
                    )
                    guard_triggered = needs_guard
                    if needs_guard:
                        alt_times, alt_metrics, alt_reason = FeatureEngineer._try_alternate_pairing(punch_times)
                        if alt_times is not None:
                            alt_meal_hours = alt_metrics.get('meal_hours', meal_hours)
                            # Only adopt alternate if it shortens unrealistic meals and keeps last-out
                            if alt_meal_hours <= min(Config.MEAL_REALISTIC_MAX_HOURS, meal_hours):
                                punch_times = alt_times
                                punch_count = len(punch_times)
                                metrics = alt_metrics
                                work_seconds = metrics['work_seconds']
                                break_seconds = metrics['break_seconds']
                                valid_pairs = metrics['valid_pairs']
                                invalid_pairs = metrics['invalid_pairs']
                                break_issues = metrics['break_issues']
                                break_count = metrics['break_count']
                                first_valid_in = metrics['first_valid_in']
                                last_valid_out = metrics['last_valid_out']
                                max_break_gap_seconds = metrics['max_break_gap_seconds']
                                meal_hours = alt_meal_hours
                                alt_applied = True

                if used_normal_subset and leading_meal_gap_seconds > 0:
                    break_seconds += leading_meal_gap_seconds

                if pd.notna(day_first_raw):
                    first_punch = day_first_raw
                    last_punch = day_last_raw
                elif punch_count > 0:
                    first_punch = punch_times[0]
                    last_punch = punch_times[-1]
                else:
                    first_punch = pd.NaT
                    last_punch = pd.NaT

                work_hours = max(0.0, work_seconds / 3600)
                meal_hours = max(0.0, break_seconds / 3600)

                has_punch_in = punch_count > 0
                has_punch_out = valid_pairs > 0
                odd_punch_count = (punch_count % 2 != 0)

                first_punch_in = first_punch if pd.notna(first_punch) else first_valid_in
                last_punch_out = last_punch if pd.notna(last_punch) else last_valid_out

                issue_tags = []
                if odd_punch_count:
                    issue_tags.append("odd_punch_count")
                if invalid_pairs > 0:
                    issue_tags.append("invalid_work_pairs")
                if break_issues > 0:
                    issue_tags.append("invalid_break_gaps")
                if has_punch_in and not has_punch_out:
                    issue_tags.append("missing_punch_out")
                if alt_applied:
                    issue_tags.append(f"alt_pairing:{alt_reason}")
                if guard_triggered and not alt_applied:
                    issue_tags.append(f"pairing_guard:{alt_reason or 'no_alternate'}")
                if meal_hours > Config.MEAL_SANITY_HOURS:
                    issue_tags.append("meal_gt_sanity")
                
                # Get additional info - handle missing values gracefully
                if 'Department' in date_group.columns and pd.notna(date_group['Department'].iloc[0]):
                    department = date_group['Department'].iloc[0]
                else:
                    department = 'Unknown'
                
                if 'Employee Supervisor' in date_group.columns and pd.notna(date_group['Employee Supervisor'].iloc[0]):
                    supervisor = date_group['Employee Supervisor'].iloc[0]
                else:
                    supervisor = 'Unknown'
                
                daily_records.append({
                    'Employee Number': emp_num,
                    'Employee Full Name': emp_name,
                    'Department': department,
                    'Supervisor': supervisor,
                    'Date': date,
                    'First Punch In': first_punch_in,
                    'Last Punch Out': last_punch_out,
                    'Working Hours': round(work_hours, 2),
                    'Net Working Hours': round(work_hours, 2),
                    'Meal Hours': round(meal_hours, 2),
                    'Punch Count': punch_count,
                    'Raw Punch Count': raw_punch_count,
                    'Normal Punch Count': normal_punch_count,
                    'Has Punch In': has_punch_in,
                    'Has Punch Out': has_punch_out,
                    'Odd Punch Count': odd_punch_count,
                    'Break Count': break_count,
                    'Punch Issues': '; '.join(issue_tags),
                    'Has Punch Issues': len(issue_tags) > 0,
                    'Day of Week': first_punch.strftime('%A'),
                    'Week Number': first_punch.isocalendar()[1],
                    'Month': first_punch.strftime('%B'),
                    'Year': first_punch.year
                })
        
        daily_df = pd.DataFrame(daily_records)
        return daily_df
    
    @staticmethod
    def add_compliance_metrics(daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add compliance-related flags and metrics based on updated business rules.
        """
        if daily_df.empty:
            return daily_df

        if 'Odd Punch Count' not in daily_df.columns:
            daily_df['Odd Punch Count'] = daily_df['Punch Count'] % 2 != 0

        daily_df['Missing Punch Out'] = (
            daily_df['Has Punch In'] &
            (daily_df['Odd Punch Count'] | (~daily_df['Has Punch Out']))
        )
        daily_df['Missing Punch In'] = ~daily_df['Has Punch In']

        def apply_compliance_rules(row):
            # ===================
            # Late Arrival Logic
            # ===================
            punch_in_dt = row.get('First Punch In')
            if pd.notna(punch_in_dt):
                punch_in_time = punch_in_dt.time()
                is_late = punch_in_time > Config.LATE_GRACE_PERIOD_END
                is_very_late = punch_in_time > Config.VERY_LATE_THRESHOLD
            else:
                punch_in_time = None
                is_late = False
                is_very_late = False
            
            minutes_late = 0
            if is_late and punch_in_time is not None:
                minutes_late = max(0, (
                    datetime.combine(datetime.today(), punch_in_time) -
                    datetime.combine(datetime.today(), Config.STANDARD_START_TIME)
                ).total_seconds() / 60)

            # =======================
            # Early Departure Logic
            # =======================
            day_of_week = row['Day of Week']
            missing_out = bool(row.get('Missing Punch Out', False))
            punch_out_dt = row.get('Last Punch Out')
            punch_out_time = punch_out_dt.time() if pd.notna(punch_out_dt) and not missing_out else None
            
            if day_of_week == 'Friday':
                early_departure_threshold = Config.EARLY_DEPARTURE_TIME_FRI
            else:
                early_departure_threshold = Config.EARLY_DEPARTURE_TIME_MON_THU

            is_early_departure = False
            if punch_out_time is not None:
                is_early_departure = punch_out_time < early_departure_threshold
            
            minutes_early = 0
            if is_early_departure:
                minutes_early = max(0, (
                    datetime.combine(datetime.today(), early_departure_threshold) -
                    datetime.combine(datetime.today(), punch_out_time)
                ).total_seconds() / 60)

            # ========================
            # Shift Classification
            # ========================
            working_hours = row['Working Hours']
            shift_type = 'Short Shift' # Default
            if day_of_week == 'Friday' and punch_out_time is not None:
                # Friday is a full day if they punch out after the designated time
                if punch_out_time >= Config.FRIDAY_FULL_DAY_PUNCH_OUT:
                    shift_type = 'Full Day'
                else:
                    shift_type = 'Short Shift' # Or could be another category if needed
            elif day_of_week in {'Monday', 'Tuesday', 'Wednesday', 'Thursday'}:
                # Monday-Thursday full-day threshold override
                if working_hours >= Config.MON_THU_FULL_DAY_THRESHOLD:
                    shift_type = 'Full Day'
                elif working_hours >= Config.HALF_DAY_THRESHOLD:
                    shift_type = 'Half Day'
            else:
                # Preserve legacy threshold behavior for all other days.
                if working_hours >= Config.FULL_DAY_THRESHOLD:
                    shift_type = 'Full Day'
                elif working_hours >= Config.HALF_DAY_THRESHOLD:
                    shift_type = 'Half Day'

            return pd.Series([
                is_late, is_very_late, minutes_late,
                is_early_departure, minutes_early,
                shift_type
            ])

        # Apply rules
        daily_df[[
            'Is Late', 'Is Very Late', 'Minutes Late',
            'Is Early Departure', 'Minutes Early',
            'Shift Type'
        ]] = daily_df.apply(apply_compliance_rules, axis=1)
        
        return daily_df
    
    @staticmethod
    def calculate_productivity_metrics(daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate employee-level productivity metrics
        Based on actual worked hours from valid attendance days
        """
        # Ensure Department is not NaN for grouping (fill with 'Unknown' if missing)
        daily_df = daily_df.copy()
        daily_df['Department'] = daily_df['Department'].fillna('Unknown')
        
        # Filter to valid working days: must have valid punch-in and actual worked hours
        # Include days with missing punch-out for completeness, but flag them separately
        valid_working_days = daily_df[
            (daily_df['Has Punch In']) &  # Must have punch-in
            (daily_df['Working Hours'] > 0)  # Must have worked some hours (duration > 0)
        ].copy()
        
        # If filtering removes all records, use all days (shouldn't happen normally)
        if len(valid_working_days) == 0:
            valid_working_days = daily_df.copy()
        
        # Group by employee to calculate productivity metrics from valid working days
        emp_metrics = valid_working_days.groupby(['Employee Number', 'Employee Full Name', 'Department']).agg({
            'Working Hours': ['sum', 'mean', 'std', 'min', 'max', 'count'],
            'Is Late': 'sum',
            'Is Early Departure': 'sum',
            'Missing Punch Out': 'sum',
            'Date': 'nunique'
        }).reset_index()
        
        # Flatten column names
        emp_metrics.columns = [
            'Employee Number', 'Employee Full Name', 'Department',
            'Total Hours', 'Avg Daily Hours', 'Std Hours', 'Min Hours', 'Max Hours', 'Total Days',
            'Late Count', 'Early Departure Count', 'Missing Punch Out Count', 'Unique Dates'
        ]
        
        # Round numeric columns
        numeric_cols = ['Total Hours', 'Avg Daily Hours', 'Std Hours', 'Min Hours', 'Max Hours']
        emp_metrics[numeric_cols] = emp_metrics[numeric_cols].round(2)
        
        # Handle division by zero for scores - ensure Total Days is at least 1
        emp_metrics['Total Days'] = emp_metrics['Total Days'].fillna(0)
        emp_metrics['Total Days'] = emp_metrics['Total Days'].replace(0, 1)  # Avoid division by zero for scores
        
        # Ensure all employees from daily_df are included (left merge to preserve all employees)
        all_employees = daily_df[['Employee Number', 'Employee Full Name', 'Department']].drop_duplicates()
        
        # Merge with all employees, keeping metrics for employees with valid working days
        # Fill missing values with 0 for employees with no valid working days
        emp_metrics = all_employees.merge(
            emp_metrics,
            on=['Employee Number', 'Employee Full Name', 'Department'],
            how='left'
        )
        
        # Fill missing numeric columns with 0 (for employees with no valid working days)
        fill_cols = ['Total Hours', 'Avg Daily Hours', 'Std Hours', 'Min Hours', 'Max Hours', 
                     'Total Days', 'Late Count', 'Early Departure Count', 'Missing Punch Out Count',
                     'Unique Dates']
        emp_metrics[fill_cols] = emp_metrics[fill_cols].fillna(0)
        
        # Ensure Total Days is at least 1 for employees with data (to avoid division issues)
        emp_metrics.loc[emp_metrics['Total Hours'] > 0, 'Total Days'] = emp_metrics.loc[
            emp_metrics['Total Hours'] > 0, 'Total Days'
        ].clip(lower=1)
        
        return emp_metrics
    
    @staticmethod
    def detect_anomalies(daily_df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag various anomalies in attendance data
        """
        # Short shifts
        daily_df['Unusually Short'] = daily_df['Working Hours'] < Config.SHORT_SHIFT_HOURS
        
        # Excessive shifts
        daily_df['Unusually Long'] = daily_df['Working Hours'] > Config.EXCESSIVE_SHIFT_HOURS
        
        # Combined anomaly flag
        daily_df['Has Anomaly'] = (
            daily_df['Unusually Short'] |
            daily_df['Unusually Long'] |
            daily_df['Missing Punch Out'] |
            daily_df['Odd Punch Count']
        )
        
        return daily_df

    @staticmethod
    def calculate_overtime_metrics(daily_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculate weekly and monthly overtime metrics.
        """
        if daily_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Ensure 'Date' is datetime
        daily_df['Date'] = pd.to_datetime(daily_df['Date'])

        # Weekly Overtime
        weekly_df = daily_df.copy()
        weekly_df['Week'] = weekly_df['Date'].dt.isocalendar().week
        weekly_df['Year'] = weekly_df['Date'].dt.year

        overtime_hours_col = 'Overtime Eligible Hours' if 'Overtime Eligible Hours' in weekly_df.columns else 'Working Hours'
        weekly_hours = weekly_df.groupby(['Employee Full Name', 'Year', 'Week'])[overtime_hours_col].sum().reset_index()
        if overtime_hours_col != 'Working Hours':
            weekly_hours = weekly_hours.rename(columns={overtime_hours_col: 'Working Hours'})
        weekly_hours['Expected Hours'] = Config.WEEKLY_STANDARD_HOURS
        weekly_hours['Weekly Overtime'] = weekly_hours['Working Hours'] - Config.WEEKLY_STANDARD_HOURS
        weekly_hours['Weekly Overtime'] = weekly_hours['Weekly Overtime'].clip(lower=0)
        
        # Monthly Overtime
        monthly_df = daily_df.copy()
        monthly_df['Month'] = monthly_df['Date'].dt.month
        monthly_df['Year'] = monthly_df['Date'].dt.year

        overtime_hours_col_m = 'Overtime Eligible Hours' if 'Overtime Eligible Hours' in monthly_df.columns else 'Working Hours'
        monthly_hours = monthly_df.groupby(['Employee Full Name', 'Year', 'Month'])[overtime_hours_col_m].sum().reset_index()
        if overtime_hours_col_m != 'Working Hours':
            monthly_hours = monthly_hours.rename(columns={overtime_hours_col_m: 'Working Hours'})
        monthly_hours['Expected Hours'] = Config.MONTHLY_STANDARD_HOURS
        monthly_hours['Monthly Overtime'] = monthly_hours['Working Hours'] - Config.MONTHLY_STANDARD_HOURS
        monthly_hours['Monthly Overtime'] = monthly_hours['Monthly Overtime'].clip(lower=0)

        return weekly_hours, monthly_hours

def calculate_15_day_overtime(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> pd.DataFrame:
    """
    Calculate 15-day overtime for the first and second halves of a month.
    Respects optional date-range limits and excludes holidays from expected hours.
    """
    if daily_df.empty:
        return pd.DataFrame()

    work_df = daily_df.copy()
    work_df['Date'] = pd.to_datetime(work_df['Date'])
    work_df = work_df[(work_df['Date'].dt.year == year) & (work_df['Date'].dt.month == month)]
    if work_df.empty:
        return pd.DataFrame()

    if start_date and isinstance(start_date, datetime):
        start_date = start_date.date()
    if end_date and isinstance(end_date, datetime):
        end_date = end_date.date()

    last_day = calendar.monthrange(year, month)[1]
    spans = [
        ("Days 1-15", date(year, month, 1), date(year, month, 15)),
        (f"Days 16-{last_day}", date(year, month, 16), date(year, month, last_day))
    ]

    results = []
    for label, span_start, span_end in spans:
        if start_date:
            span_start = max(span_start, start_date)
        if end_date:
            span_end = min(span_end, end_date)
        if span_start > span_end:
            continue

        span_df = work_df[
            (work_df['Date'].dt.date >= span_start) &
            (work_df['Date'].dt.date <= span_end)
        ]
        if span_df.empty:
            continue

        overtime_hours_col = 'Overtime Eligible Hours' if 'Overtime Eligible Hours' in span_df.columns else 'Working Hours'
        actual_hours = span_df.groupby('Employee Full Name')[overtime_hours_col].sum().reset_index()
        if overtime_hours_col != 'Working Hours':
            actual_hours = actual_hours.rename(columns={overtime_hours_col: 'Working Hours'})
        if actual_hours.empty:
            continue

        actual_hours['Expected Hours'] = 80.0
        actual_hours['15-Day Overtime'] = (actual_hours['Working Hours'] - 80.0).clip(lower=0)
        actual_hours['Year'] = year
        actual_hours['Month'] = month
        actual_hours['Span'] = label
        actual_hours['Span Start'] = span_start
        actual_hours['Span End'] = span_end
        results.append(actual_hours)

    if not results:
        return pd.DataFrame()
    return pd.concat(results, ignore_index=True)

def plot_overtime_charts(overtime_df: pd.DataFrame, time_period: str, top_n: int = 15):
    """
    Create bar chart for weekly or monthly overtime from a pre-filtered DataFrame.
    """
    if overtime_df.empty:
        return None

    period_key = str(time_period).strip().lower()
    if period_key in ['15-day', '15 day', '15day']:
        overtime_col = '15-Day Overtime'
        period_label = '15-Day'
    else:
        period_label = time_period.capitalize()
        overtime_col = f'{period_label} Overtime'

    if overtime_col not in overtime_df.columns:
        return None
    
    # Filter for entries with actual overtime and get the top N
    overtime_df = overtime_df[overtime_df[overtime_col] > 0]
    top_performers = overtime_df.nlargest(top_n, overtime_col)
    
    if top_performers.empty:
        return None

    fig = px.bar(
        top_performers,
        x=overtime_col,
        y='Employee Full Name',
        orientation='h',
        title=f'Top {top_n} Employees by {period_label} Overtime',
        labels={overtime_col: 'Overtime Hours', 'Employee Full Name': 'Employee'},
        color=overtime_col,
        color_continuous_scale='Plasma'
    )
    fig.update_layout(height=400, showlegend=False, yaxis={'categoryorder':'total ascending'})
    return fig


# ============================================================================
# PHASE 3: DATA PERSISTENCE
# ============================================================================

class DataManager:
    """Handles data persistence and file management"""

    @staticmethod
    def _get_excel_extension(name_or_path: str) -> str:
        return os.path.splitext(str(name_or_path or ""))[1].lower()

    @staticmethod
    def _get_excel_engine(ext: str) -> Optional[str]:
        if ext == ".xlsx":
            return "openpyxl"
        if ext == ".xls":
            return "xlrd"
        return None
    
    @staticmethod
    def merge_and_save(uploaded_file, target_path: str):
        """
        Merge uploaded data with existing data and save to disk
        """
        try:
            # Load new data
            uploaded_file.seek(0)
            upload_ext = DataManager._get_excel_extension(getattr(uploaded_file, "name", ""))
            if upload_ext not in [".xlsx", ".xls"]:
                st.error("Unsupported file type. Please upload a .xlsx or .xls file.")
                return False
            upload_engine = DataManager._get_excel_engine(upload_ext)
            try:
                new_df = pd.read_excel(uploaded_file, engine=upload_engine)
            except Exception:
                uploaded_file.seek(0)
                try:
                    new_df = pd.read_excel(uploaded_file)
                except Exception:
                    st.error("Error reading the uploaded Excel file. Please verify it is a valid .xlsx or .xls.")
                    return False
            
            # Check if target file exists
            if os.path.exists(target_path):
                try:
                    target_ext = DataManager._get_excel_extension(target_path)
                    target_engine = DataManager._get_excel_engine(target_ext)
                    existing_df = pd.read_excel(target_path, engine=target_engine)
                    # Combine datasets
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True, sort=False)
                    # Remove exact duplicates
                    combined_df = combined_df.drop_duplicates()
                except Exception:
                    combined_df = new_df
            else:
                combined_df = new_df
            
            # Save merged data
            base_path, target_ext = os.path.splitext(target_path)
            if target_ext.lower() not in [".xlsx", ".xls"]:
                st.error("Data file path must be .xlsx or .xls.")
                return False
            temp_path = f"{base_path}.tmp{target_ext}"
            target_engine = DataManager._get_excel_engine(target_ext.lower())
            with pd.ExcelWriter(temp_path, engine=target_engine) as writer:
                combined_df.to_excel(writer, index=False)
            # Validate temp output before replacing production file
            _read_excel_file(temp_path, sample_only=True)

            backup_path = _get_backup_path(target_path)
            if os.path.exists(target_path):
                try:
                    shutil.copy2(target_path, backup_path)
                except Exception:
                    pass
            os.replace(temp_path, target_path)
            # Validate final file and roll back if write became corrupted
            _read_excel_file(target_path, sample_only=True)
            return True
            
        except Exception as e:
            try:
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            try:
                if 'backup_path' in locals() and os.path.exists(backup_path):
                    shutil.copy2(backup_path, target_path)
            except Exception:
                pass
            st.error(f"Error saving data: {str(e)}")
            return False

# ============================================================================
# DATA LOADING & PROCESSING PIPELINE
# ============================================================================

def _get_backup_path(path: str) -> str:
    base_path, ext = os.path.splitext(path)
    return f"{base_path}.bak{ext}"

def _read_excel_file(path: str, sample_only: bool = False) -> pd.DataFrame:
    """
    Read an Excel file with extension-aware engine fallback.
    Raises a clean ValueError for corrupted zip/compression payloads.
    """
    ext = DataManager._get_excel_extension(path)
    engine = DataManager._get_excel_engine(ext)
    read_kwargs = {"nrows": 5} if sample_only else {}
    errors: List[Exception] = []

    if engine:
        try:
            return pd.read_excel(path, engine=engine, **read_kwargs)
        except Exception as ex:
            errors.append(ex)

    try:
        return pd.read_excel(path, **read_kwargs)
    except Exception as ex:
        errors.append(ex)

    error_text = " | ".join(str(err) for err in errors)
    lowered = error_text.lower()
    if "error -3" in lowered or "decompressing data" in lowered or "zlib" in lowered:
        raise ValueError(f"Data file '{os.path.basename(path)}' is corrupted or partially written.")
    raise ValueError(f"Unable to read Excel file '{os.path.basename(path)}'. {error_text}")

def _restore_data_file_from_backup(target_path: str) -> bool:
    """
    Restore the primary data file from its backup if the backup is valid.
    """
    backup_path = _get_backup_path(target_path)
    if not os.path.exists(backup_path):
        return False

    try:
        _read_excel_file(backup_path, sample_only=True)
    except Exception:
        return False

    restore_tmp = f"{target_path}.restore_tmp"
    try:
        shutil.copy2(backup_path, restore_tmp)
        os.replace(restore_tmp, target_path)
        return True
    except Exception:
        try:
            if os.path.exists(restore_tmp):
                os.remove(restore_tmp)
        except Exception:
            pass
        return False

def _get_data_source_signature(path: str) -> str:
    """
    Build a deterministic signature for cache invalidation based on file metadata.
    """
    try:
        stat = os.stat(path)
    except OSError:
        return "missing"
    return f"{stat.st_mtime_ns}-{stat.st_size}"

def _ensure_cache_version() -> None:
    """
    Clear cached artifacts when the cache schema version changes.
    Uses a small on-disk version marker to persist across sessions.
    """
    version_path = os.path.join(os.path.dirname(__file__), ".cache_version")
    last_version = None
    try:
        if os.path.exists(version_path):
            with open(version_path, "r", encoding="utf-8") as handle:
                last_version = handle.read().strip()
    except OSError:
        last_version = None

    if last_version != Config.CACHE_VERSION:
        st.cache_data.clear()
        st.cache_resource.clear()
        try:
            with open(version_path, "w", encoding="utf-8") as handle:
                handle.write(Config.CACHE_VERSION)
        except OSError:
            # If we can't write the version marker, proceed without failing
            pass

def _clear_all_caches() -> None:
    """Clear both data and resource caches in one place."""
    st.cache_data.clear()
    st.cache_resource.clear()

def _clear_mysql_temp_loaded_data() -> None:
    """
    Best-effort cleanup for attendance staging/temp tables, if they exist.
    """
    conn = None
    cursor = None
    try:
        db_name = (_load_db_config() or {}).get("database")
        if not db_name:
            return

        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND (
                    table_name LIKE 'tmp_attendance%%'
                    OR table_name LIKE 'attendance_tmp%%'
                    OR table_name LIKE 'stg_attendance%%'
                    OR table_name LIKE 'attendance_staging%%'
                    OR table_name LIKE '%%attendance%%temp%%'
                    OR table_name LIKE '%%attendance%%staging%%'
              )
            """,
            (db_name,),
        )
        table_names = [row[0] for row in (cursor.fetchall() or []) if row and row[0]]
        for table_name in table_names:
            safe_name = str(table_name).replace("`", "")
            cursor.execute(f"TRUNCATE TABLE `{safe_name}`")
        conn.commit()
    except Exception:
        # Reset must remain safe even when optional temp tables are absent/inaccessible.
        pass
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def _reset_dashboard_data_state() -> Tuple[bool, str]:
    """
    Clear loaded attendance state and return app to upload-ready condition.
    Keeps auth context so users remain signed in.
    """
    if not _current_user_is_admin():
        return False, "Admin access required to reset dashboard data."

    errors: List[str] = []

    _clear_all_caches()

    for path in (Config.DATA_FILE_PATH, _get_backup_path(Config.DATA_FILE_PATH)):
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError as ex:
            errors.append(f"{os.path.basename(path)}: {ex}")

    _clear_mysql_temp_loaded_data()

    if errors:
        return False, "Reset failed; no partial reset applied. " + " | ".join(errors)

    preserve_keys = {"auth_authenticated", "auth_user", "auth_user_id", "auth_role", "allowed_employees"}
    for key in list(st.session_state.keys()):
        if key not in preserve_keys:
            st.session_state.pop(key, None)
    return True, "Data reset complete. Dashboard is ready for a fresh upload."

def _is_cache_corruption_error(message: str) -> bool:
    """Detect known cache corruption/decompression signatures."""
    text = str(message or "").lower()
    signatures = (
        "error -3",
        "decompressing data",
        "invalid literal/lengths set",
        "pickle data was truncated",
        "unpickling",
        "invalid load key",
        "zlib",
        "corrupted or partially written",
        "processed data is missing required tables",
        "processed daily data is missing required columns",
        "processed aggregate tables are incomplete"
    )
    return any(sig in text for sig in signatures)

def _validate_raw_df(raw_df: pd.DataFrame) -> None:
    """
    Defensive validation to detect corrupted or incompatible data reads.
    """
    if raw_df is None or raw_df.empty:
        raise ValueError("Uploaded file produced no readable rows.")

    required_any_time = {'Actual Date Time', 'Punch Date Time', 'Created Date Time (UTC)'}
    required_any_employee = {'Employee Number', 'Employee First Name', 'Employee Last Name', 'Employee Full Name'}

    if not (set(raw_df.columns) & required_any_time):
        raise ValueError("Missing required time columns in the uploaded file.")
    if not (set(raw_df.columns) & required_any_employee):
        raise ValueError("Missing required employee columns in the uploaded file.")

def _validate_processed_frames(
    raw_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    emp_metrics_df: pd.DataFrame,
    weekly_overtime_df: pd.DataFrame,
    monthly_overtime_df: pd.DataFrame
) -> None:
    """
    Validate processed outputs before serving from cache/resource.
    """
    if raw_df is None or daily_df is None:
        raise ValueError("Processed data is missing required tables.")
    if daily_df.empty:
        raise ValueError("No valid attendance data found in the provided file for weekdays.")

    required_daily_cols = {
        'Employee Number', 'Employee Full Name', 'Date',
        'Working Hours', 'Meal Hours', 'Is Late',
        'Is Early Departure', 'Has Anomaly'
    }
    if not required_daily_cols.issubset(set(daily_df.columns)):
        raise ValueError("Processed daily data is missing required columns.")

    if emp_metrics_df is None or weekly_overtime_df is None or monthly_overtime_df is None:
        raise ValueError("Processed aggregate tables are incomplete.")

@st.cache_resource(show_spinner=False)
def load_and_process_data(
    data_source_path: str,
    source_signature: str,
    cache_version: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Complete data processing pipeline
    Returns: (raw_df, daily_df, employee_metrics_df, weekly_overtime_df, monthly_overtime_df)
    """
    if not data_source_path or not os.path.exists(data_source_path):
        raise FileNotFoundError("Attendance data file not found.")

    # Load data
    raw_df = _read_excel_file(data_source_path)
    _validate_raw_df(raw_df)
    
    # Phase 1: Data Cleaning
    cleaner = DataCleaner()
    raw_df = cleaner.standardize_names(raw_df)
    raw_df = cleaner.fill_missing_departments(raw_df)
    raw_df = cleaner.clean_datetime_columns(raw_df)
    raw_df = cleaner.detect_duplicates(raw_df)
    raw_df = cleaner.create_data_quality_flags(raw_df)
    raw_df = cleaner.clean_system_metadata(raw_df)
    
    # Phase 2: Feature Engineering
    engineer = FeatureEngineer()
    daily_df = engineer.calculate_daily_attendance(raw_df)
    if daily_df.empty:
        raise ValueError("No valid attendance data found in the provided file for weekdays.")

    daily_df = engineer.add_compliance_metrics(daily_df)
    daily_df = engineer.detect_anomalies(daily_df)
    
    # Employee-level metrics
    emp_metrics_df = engineer.calculate_productivity_metrics(daily_df)

    # Overtime metrics
    weekly_overtime_df, monthly_overtime_df = engineer.calculate_overtime_metrics(daily_df)

    _validate_processed_frames(
        raw_df,
        daily_df,
        emp_metrics_df,
        weekly_overtime_df,
        monthly_overtime_df
    )
    
    return raw_df, daily_df, emp_metrics_df, weekly_overtime_df, monthly_overtime_df

# ============================================================================
# CACHED AGGREGATIONS (PERFORMANCE)
# ============================================================================

def get_productivity_metrics(daily_df: pd.DataFrame) -> pd.DataFrame:
    return FeatureEngineer.calculate_productivity_metrics(daily_df)

def get_dow_summary(view_df: pd.DataFrame) -> pd.Series:
    return view_df.groupby('Day of Week')['Working Hours'].mean().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'
    ])

def get_monthly_metrics_cached(daily_df: pd.DataFrame) -> pd.DataFrame:
    return calculate_monthly_metrics(daily_df)

def get_recent_changes(monthly_df: pd.DataFrame) -> pd.DataFrame:
    monthly_df_sorted = monthly_df.sort_values(['Employee Full Name', 'YearMonth'])
    monthly_df_sorted['Prev Total Hours'] = monthly_df_sorted.groupby('Employee Full Name')['Total Hours'].shift(1)
    monthly_df_sorted['Hours Change'] = monthly_df_sorted['Total Hours'] - monthly_df_sorted['Prev Total Hours']
    monthly_df_sorted['Hours Change %'] = (monthly_df_sorted['Hours Change'] / monthly_df_sorted['Prev Total Hours'] * 100).round(1)
    available_months = sorted(monthly_df_sorted['YearMonth'].unique().tolist())
    if not available_months:
        return monthly_df_sorted.iloc[0:0]
    recent_changes = monthly_df_sorted[monthly_df_sorted['YearMonth'] == available_months[-1]].copy()
    recent_changes = recent_changes[recent_changes['Prev Total Hours'].notna()].sort_values('Hours Change', ascending=False)
    return recent_changes

def get_work_pattern_kpis_cached(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> Dict[str, float]:
    return calculate_work_pattern_kpis(daily_df, employee_name, year, month, special_day_items)

def get_work_pattern_distribution_cached(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> pd.DataFrame:
    return calculate_work_pattern_distribution(daily_df, employee_name, year, month, special_day_items)

def get_work_pattern_calendar_cached(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    kpi_data: Optional[Dict[str, float]] = None,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> str:
    return create_work_pattern_calendar(
        daily_df,
        employee_name,
        year,
        month,
        kpi_data,
        special_day_items
    )


def apply_annotation_overrides(
    daily_df: pd.DataFrame,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> pd.DataFrame:
    """
    Apply persisted annotation overrides to compliance flags only.
    Core punch data and working-hour computation remain unchanged.
    """
    if daily_df is None or daily_df.empty:
        return daily_df

    result_df = daily_df.copy()
    result_df['Date'] = pd.to_datetime(result_df['Date'])
    result_df['DateOnly'] = result_df['Date'].dt.date

    special_day_map = build_special_day_map(special_day_items)
    type_map = {d: meta.get('type', '') for d, meta in special_day_map.items()}
    reason_map = {d: meta.get('reason', '') for d, meta in special_day_map.items()}

    result_df['Annotation Type'] = result_df['DateOnly'].map(type_map).fillna('')
    result_df['Annotation Reason'] = result_df['DateOnly'].map(reason_map).fillna('')
    result_df['Is Annotation Holiday'] = result_df['Annotation Type'].eq('Full Off')

    if 'Working Hours' not in result_df.columns:
        result_df['Working Hours'] = 0.0
    result_df['Overtime Eligible Hours'] = pd.to_numeric(
        result_df['Working Hours'], errors='coerce'
    ).fillna(0.0)
    result_df.loc[result_df['Is Annotation Holiday'], 'Overtime Eligible Hours'] = 0.0

    # Open Late: suppress all late flags for that day.
    open_late_mask = result_df['Annotation Type'].eq('Open Late')
    if open_late_mask.any():
        result_df.loc[open_late_mask, 'Is Late'] = False
        result_df.loc[open_late_mask, 'Is Very Late'] = False
        result_df.loc[open_late_mask, 'Minutes Late'] = 0.0

    # Early Close: suppress all early-departure flags for that day.
    early_close_mask = result_df['Annotation Type'].eq('Early Close')
    if early_close_mask.any():
        result_df.loc[early_close_mask, 'Is Early Departure'] = False
        result_df.loc[early_close_mask, 'Minutes Early'] = 0.0

    # Full Off: treat as holiday for compliance/overtime purposes.
    full_off_mask = result_df['Annotation Type'].eq('Full Off')
    if full_off_mask.any():
        result_df.loc[full_off_mask, 'Is Late'] = False
        result_df.loc[full_off_mask, 'Is Very Late'] = False
        result_df.loc[full_off_mask, 'Minutes Late'] = 0.0
        result_df.loc[full_off_mask, 'Is Early Departure'] = False
        result_df.loc[full_off_mask, 'Minutes Early'] = 0.0

    # Special Hours: use adjusted open/close windows for compliance checks only.
    late_grace_minutes = int(
        (
            datetime.combine(date.today(), Config.LATE_GRACE_PERIOD_END) -
            datetime.combine(date.today(), Config.STANDARD_START_TIME)
        ).total_seconds() / 60
    )
    very_late_minutes = int(
        (
            datetime.combine(date.today(), Config.VERY_LATE_THRESHOLD) -
            datetime.combine(date.today(), Config.STANDARD_START_TIME)
        ).total_seconds() / 60
    )

    for day_date, meta in special_day_map.items():
        if meta.get('type') != 'Special Hours':
            continue
        open_time, close_time, _ = parse_special_hours_reason(meta.get('reason', ''))
        if open_time is None and close_time is None:
            continue

        mask = result_df['DateOnly'].eq(day_date)
        if not mask.any():
            continue

        if open_time is not None:
            late_cutoff = _add_minutes_to_time(open_time, late_grace_minutes)
            very_late_cutoff = _add_minutes_to_time(open_time, very_late_minutes)
            punch_in_series = result_df.loc[mask, 'First Punch In']
            is_late_series = punch_in_series.apply(
                lambda x: pd.notna(x) and x.time() > late_cutoff
            )
            is_very_late_series = punch_in_series.apply(
                lambda x: pd.notna(x) and x.time() > very_late_cutoff
            )
            minutes_late_series = punch_in_series.apply(
                lambda x: max(
                    0.0,
                    (
                        datetime.combine(date.today(), x.time()) -
                        datetime.combine(date.today(), open_time)
                    ).total_seconds() / 60
                ) if pd.notna(x) and x.time() > late_cutoff else 0.0
            )
            result_df.loc[mask, 'Is Late'] = is_late_series.values
            result_df.loc[mask, 'Is Very Late'] = is_very_late_series.values
            result_df.loc[mask, 'Minutes Late'] = minutes_late_series.values

        if close_time is not None:
            punch_out_series = result_df.loc[mask, 'Last Punch Out']
            if 'Missing Punch Out' in result_df.columns:
                missing_out_series = result_df.loc[mask, 'Missing Punch Out'].fillna(False).astype(bool)
            else:
                missing_out_series = pd.Series(False, index=punch_out_series.index)
            is_early_series = punch_out_series.apply(
                lambda x: pd.notna(x) and x.time() < close_time
            ) & (~missing_out_series)
            minutes_early_series = punch_out_series.apply(
                lambda x: max(
                    0.0,
                    (
                        datetime.combine(date.today(), close_time) -
                        datetime.combine(date.today(), x.time())
                    ).total_seconds() / 60
                ) if pd.notna(x) and x.time() < close_time else 0.0
            )
            minutes_early_series = minutes_early_series.where(~missing_out_series, 0.0)
            result_df.loc[mask, 'Is Early Departure'] = is_early_series.values
            result_df.loc[mask, 'Minutes Early'] = minutes_early_series.values

    return result_df.drop(columns=['DateOnly'], errors='ignore')

def get_weekly_employee_comparison_cached(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    employee_tuple: Tuple[str, ...],
    weekday_tuple: Tuple[str, ...]
) -> pd.DataFrame:
    employees = list(employee_tuple) if employee_tuple else None
    weekdays = list(weekday_tuple) if weekday_tuple else None
    return calculate_weekly_employee_comparison(daily_df, year, month, employees, weekdays)

def get_lunch_break_risk_cached(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    employee_tuple: Tuple[str, ...],
    high_work_hours: float,
    short_lunch_minutes: int,
    avg_lunch_warning_minutes: int,
    long_continuous_hours: float
) -> pd.DataFrame:
    employees = list(employee_tuple) if employee_tuple else None
    return calculate_lunch_break_risk(
        daily_df=daily_df,
        year=year,
        month=month,
        employees=employees,
        high_work_hours=high_work_hours,
        short_lunch_minutes=short_lunch_minutes,
        avg_lunch_warning_minutes=avg_lunch_warning_minutes,
        long_continuous_hours=long_continuous_hours
    )

@st.cache_data(show_spinner=False)
def count_working_days(start_date, end_date) -> int:
    if start_date is None or end_date is None:
        return 0
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    date_index = pd.date_range(start=start_date, end=end_date, freq="D")
    holiday_set = get_company_holiday_set(start_date, end_date)
    return int(sum(
        (dt.weekday() < 5) and (dt.date() not in holiday_set)
        for dt in date_index
    ))

# ============================================================================
# DEBUG / VALIDATION HELPERS (opt-in)
# ============================================================================

def _build_debug_case_df() -> Tuple[pd.DataFrame, List[Dict[str, object]]]:
    """Create a minimal dataframe for the three provided edge cases."""
    cases = [
        {
            'name': 'Megan Blevins',
            'date': '2025-06-24',
            'times': ['07:35:00', '07:40:00', '09:30:00', '11:32:00', '11:32:00', '12:28:00', '12:28:00', '16:16:00'],
            'expected_first': time(7, 35),
            'expected_last': time(16, 16),
            'expected_meal': 0.93
        },
        {
            'name': 'Shelbie Clark',
            'date': '2025-02-18',
            'times': ['07:42:00', '11:40:00', '11:40:00', '12:40:00', '12:41:00', '17:19:00'],
            'expected_first': time(7, 42),
            'expected_last': time(17, 19),
            'expected_meal': 1.00
        },
        {
            'name': 'Shelbie Clark',
            'date': '2025-05-06',
            'times': ['07:45:00', '11:28:00', '11:28:00', '12:23:00', '12:24:00', '16:47:00'],
            'expected_first': time(7, 45),
            'expected_last': time(16, 47),
            'expected_meal': 0.92
        },
    ]
    rows = []
    for idx, case in enumerate(cases, start=1):
        for ts_str in case['times']:
            ts = pd.to_datetime(f"{case['date']} {ts_str}")
            rows.append({
                'Employee Number': idx,
                'Employee Full Name': case['name'],
                'Punch Date': pd.to_datetime(case['date']).date(),
                'Timestamp': ts,
                'Missing Timestamp': False,
                'Incomplete Employee Record': False,
                'Duplicate Punch': False,
                'Day of Week': ts.strftime('%A'),
                'Type': 'Normal'
            })
    df = pd.DataFrame(rows)
    # Reuse existing duplicate logic so the tests mirror the real pipeline
    df = DataCleaner.detect_duplicates(df)
    return df, cases


def _calculate_with_toggle(df: pd.DataFrame, enable_cleaning: bool, enable_guard: bool) -> pd.DataFrame:
    """Run daily calculation with temporary toggles to compare before/after behavior."""
    prior_clean = Config.ENABLE_PUNCH_CLEANING
    prior_guard = Config.ENABLE_PUNCH_SANITY_GUARD
    Config.ENABLE_PUNCH_CLEANING = enable_cleaning
    Config.ENABLE_PUNCH_SANITY_GUARD = enable_guard
    try:
        return FeatureEngineer.calculate_daily_attendance(df)
    finally:
        Config.ENABLE_PUNCH_CLEANING = prior_clean
        Config.ENABLE_PUNCH_SANITY_GUARD = prior_guard


def run_debug_unit_checks(tolerance: float = 0.08) -> Dict[str, object]:
    """
    Unit-style checks for the three provided edge cases.
    Returns a structured report; execution is opt-in (not run in normal UI flow).
    """
    test_df, cases = _build_debug_case_df()
    before_df = _calculate_with_toggle(test_df, enable_cleaning=False, enable_guard=False)
    after_df = FeatureEngineer.calculate_daily_attendance(test_df)

    results = []
    for case in cases:
        case_date = pd.to_datetime(case['date']).date()
        before_row = before_df[
            (before_df['Employee Full Name'] == case['name']) &
            (before_df['Date'] == case_date)
        ]
        after_row = after_df[
            (after_df['Employee Full Name'] == case['name']) &
            (after_df['Date'] == case_date)
        ]

        def _safe_time(val):
            if isinstance(val, pd.Timestamp):
                return val.time()
            return None

        before_meal = float(before_row['Meal Hours'].iloc[0]) if not before_row.empty else None
        after_meal = float(after_row['Meal Hours'].iloc[0]) if not after_row.empty else None
        after_punch_count = int(after_row['Punch Count'].iloc[0]) if not after_row.empty else None
        after_first = _safe_time(after_row['First Punch In'].iloc[0]) if not after_row.empty else None
        after_last = _safe_time(after_row['Last Punch Out'].iloc[0]) if not after_row.empty else None

        case_pass = (
            (after_row.shape[0] == 1) and
            (after_first == case['expected_first']) and
            (after_last == case['expected_last']) and
            (after_punch_count is not None and after_punch_count % 2 == 0) and
            (after_meal is not None and abs(after_meal - case['expected_meal']) <= tolerance)
        )

        results.append({
            'case': f"{case['name']} {case_date}",
            'before_meal': before_meal,
            'after_meal': after_meal,
            'expected_meal': case['expected_meal'],
            'after_first': after_first,
            'after_last': after_last,
            'expected_first': case['expected_first'],
            'expected_last': case['expected_last'],
            'after_punch_count': after_punch_count,
            'pass': case_pass
        })

    return {
        'cases': results,
        'before_df': before_df,
        'after_df': after_df
    }


def run_regression_sample(raw_df: Optional[pd.DataFrame], sample_size: int = 200) -> Dict[str, object]:
    """
    Compare legacy vs guarded logic on a small random sample of days.
    Returns change ratios and flagged counts; caller decides whether to act.
    """
    if raw_df is None or raw_df.empty:
        return {'sampled': 0, 'changed': 0, 'flagged': 0, 'ratio': 0.0}

    keys = raw_df[['Employee Number', 'Employee Full Name', 'Punch Date']].drop_duplicates()
    if keys.empty:
        return {'sampled': 0, 'changed': 0, 'flagged': 0, 'ratio': 0.0}

    sample_keys = keys.sample(n=min(sample_size, len(keys)), random_state=42)
    changed = 0
    flagged = 0

    for _, key_row in sample_keys.iterrows():
        mask = (
            (raw_df['Employee Number'] == key_row['Employee Number']) &
            (raw_df['Employee Full Name'] == key_row['Employee Full Name']) &
            (raw_df['Punch Date'] == key_row['Punch Date'])
        )
        day_df = raw_df[mask]
        if day_df.empty:
            continue

        legacy_df = _calculate_with_toggle(day_df, enable_cleaning=False, enable_guard=False)
        guarded_df = FeatureEngineer.calculate_daily_attendance(day_df)

        if legacy_df.empty or guarded_df.empty:
            continue

        legacy_row = legacy_df.iloc[0]
        guarded_row = guarded_df.iloc[0]

        meal_changed = not np.isclose(legacy_row['Meal Hours'], guarded_row['Meal Hours'])
        work_changed = not np.isclose(legacy_row['Working Hours'], guarded_row['Working Hours'])
        if meal_changed or work_changed:
            changed += 1

        issues = str(guarded_row.get('Punch Issues', ''))
        if 'alt_pairing' in issues:
            flagged += 1

    sampled = len(sample_keys)
    ratio = (changed / sampled) if sampled else 0.0
    return {'sampled': sampled, 'changed': changed, 'flagged': flagged, 'ratio': ratio}


# Optional debug hook: set DEBUG_PUNCH_FIX=1 to print quick diagnostics to console.
if os.environ.get("DEBUG_PUNCH_FIX") == "1":
    try:
        dbg = run_debug_unit_checks()
        print("DEBUG_PUNCH_FIX unit checks:")
        for item in dbg['cases']:
            print(
                f" - {item['case']}: meal {item['before_meal']} -> {item['after_meal']} "
                f"(expected {item['expected_meal']}), first {item['after_first']}, last {item['after_last']}, "
                f"punches {item['after_punch_count']}, pass={item['pass']}"
            )

        regression_summary = {'sampled': 0, 'changed': 0, 'flagged': 0, 'ratio': 0.0}
        try:
            if os.path.exists(Config.DATA_FILE_PATH):
                cleaner = DataCleaner()
                raw_debug_df = pd.read_excel(Config.DATA_FILE_PATH)
                raw_debug_df = cleaner.standardize_names(raw_debug_df)
                raw_debug_df = cleaner.fill_missing_departments(raw_debug_df)
                raw_debug_df = cleaner.clean_datetime_columns(raw_debug_df)
                raw_debug_df = cleaner.detect_duplicates(raw_debug_df)
                raw_debug_df = cleaner.create_data_quality_flags(raw_debug_df)
                raw_debug_df = cleaner.clean_system_metadata(raw_debug_df)
                regression_summary = run_regression_sample(raw_debug_df, sample_size=200)
        except Exception as reg_ex:
            print(f"DEBUG_PUNCH_FIX regression sample failed: {reg_ex}")

        print(f"DEBUG_PUNCH_FIX regression sample: {regression_summary}")
    except Exception as debug_ex:
        print(f"DEBUG_PUNCH_FIX failed: {debug_ex}")

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_metric_card(label: str, value, delta=None, help_text=None):
    """Create a styled metric card"""
    col = st.container()
    with col:
        if delta:
            st.metric(label=label, value=value, delta=delta, help=help_text)
        else:
            st.metric(label=label, value=value, help=help_text)

def plot_compliance_trend(daily_df: pd.DataFrame):
    """Line chart of compliance metrics over time"""
    # Aggregate by date
    trend = daily_df.groupby('Date').agg({
        'Is Late': lambda x: (x.sum() / len(x) * 100),
        'Is Early Departure': lambda x: (x.sum() / len(x) * 100)
    }).reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=trend['Date'], y=trend['Is Late'], 
                             mode='lines+markers', name='Late Arrivals %',
                             line=dict(color='red')))
    fig.add_trace(go.Scatter(x=trend['Date'], y=trend['Is Early Departure'],
                             mode='lines+markers', name='Early Departures %',
                             line=dict(color='orange')))
    
    fig.update_layout(
        title='Compliance Trend Over Time',
        xaxis_title='Date',
        yaxis_title='Percentage (%)',
        height=400,
        hovermode='x unified'
    )
    return fig

def plot_employee_ranking(emp_metrics_df: pd.DataFrame, metric: str, top_n: int = 10):
    """Horizontal bar chart for employee rankings"""
    top_emp = emp_metrics_df.nlargest(top_n, metric)
    
    fig = px.bar(
        top_emp,
        y='Employee Full Name',
        x=metric,
        orientation='h',
        title=f'Top {top_n} Employees by {metric}',
        labels={metric: metric, 'Employee Full Name': 'Employee'},
        color=metric,
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=400, showlegend=False)
    return fig

# ============================================================================
# MONTHLY ANALYTICS FUNCTIONS
# ============================================================================

def calculate_monthly_metrics(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly performance metrics per employee
    Returns DataFrame with monthly aggregations
    """
    # Ensure Date is datetime
    daily_df = daily_df.copy()
    daily_df['Date'] = pd.to_datetime(daily_df['Date'])
    daily_df['YearMonth'] = daily_df['Date'].dt.to_period('M').astype(str)
    
    # Calculate monthly metrics per employee
    monthly_metrics = daily_df.groupby(['Employee Number', 'Employee Full Name', 'Department', 'YearMonth']).agg({
        'Working Hours': ['sum', 'mean'],
        'Meal Hours': 'sum',
        'Date': 'nunique',
        'Is Late': 'sum',
        'Is Early Departure': 'sum',
        'Missing Punch Out': 'sum'
    }).reset_index()
    
    # Flatten column names
    monthly_metrics.columns = [
        'Employee Number', 'Employee Full Name', 'Department', 'YearMonth',
        'Total Hours', 'Avg Daily Hours', 'Total Meal Hours', 'Attendance Days',
        'Late Count', 'Early Departure Count', 'Missing Punch Out Count'
    ]
    
    # Round numeric columns
    monthly_metrics['Total Hours'] = monthly_metrics['Total Hours'].round(2)
    monthly_metrics['Avg Daily Hours'] = monthly_metrics['Avg Daily Hours'].round(2)
    monthly_metrics['Total Meal Hours'] = monthly_metrics['Total Meal Hours'].round(2)
    
    # Sort by YearMonth for proper chronological order
    monthly_metrics = monthly_metrics.sort_values(['Employee Full Name', 'YearMonth'])
    
    return monthly_metrics

def plot_monthly_trend(monthly_df: pd.DataFrame, employee_name: str, metric: str = 'Total Hours'):
    """Line chart showing employee's monthly performance trend"""
    emp_data = monthly_df[monthly_df['Employee Full Name'] == employee_name].sort_values('YearMonth')
    
    if len(emp_data) == 0:
        return None
    
    fig = px.line(
        emp_data,
        x='YearMonth',
        y=metric,
        markers=True,
        title=f'{employee_name} - Monthly {metric} Trend',
        labels={'YearMonth': 'Month', metric: metric},
        line_shape='linear'
    )
    fig.update_traces(line_color='#2E86AB', marker_size=8)
    fig.update_layout(
        height=400,
        xaxis_tickangle=-45,
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def plot_monthly_comparison(monthly_df: pd.DataFrame, year_month: str, metric: str = 'Total Hours', top_n: int = 10):
    """Bar chart comparing employees for a specific month"""
    month_data = monthly_df[monthly_df['YearMonth'] == year_month].sort_values(metric, ascending=False).head(top_n)
    
    if len(month_data) == 0:
        return None
    
    fig = px.bar(
        month_data,
        x=metric,
        y='Employee Full Name',
        orientation='h',
        title=f'Top {top_n} Employees - {year_month} ({metric})',
        labels={metric: metric, 'Employee Full Name': 'Employee'},
        color=metric,
        color_continuous_scale='Blues'
    )
    fig.update_layout(
        height=400,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def calculate_attendance_distribution(daily_df: pd.DataFrame, employee_name: str, year: int, month: int) -> pd.DataFrame:
    """
    Calculate distribution of attendance types for an employee in a specific month
    Returns DataFrame with counts for each attendance type
    """
    # Filter to employee and month
    emp_df = daily_df[daily_df['Employee Full Name'] == employee_name].copy()
    emp_df['Date'] = pd.to_datetime(emp_df['Date'])
    emp_df = emp_df[(emp_df['Date'].dt.year == year) & (emp_df['Date'].dt.month == month)]
    
    # Categorize each day
    def categorize_day(row):
        # Use Shift Type from compliance logic to ensure Friday Full Days are counted correctly
        shift_type = row.get('Shift Type', 'Absent')
        has_anomaly = row.get('Has Anomaly', False)
        
        if has_anomaly:
            return 'Anomaly'
        elif shift_type == 'Full Day':
            return 'Full Day'
        elif shift_type == 'Half Day':
            return 'Half Day'
        elif shift_type == 'Short Shift':
            return 'Short Day'
        elif row['Working Hours'] > 0:
            return 'Short Day'
        else:
            return 'Absent'
    
    emp_df['Attendance Type'] = emp_df.apply(categorize_day, axis=1)
    
    # Count distribution
    distribution = emp_df['Attendance Type'].value_counts().reset_index()
    distribution.columns = ['Attendance Type', 'Count']
    
    # Ensure all categories are present (fill missing with 0)
    all_types = ['Full Day', 'Half Day', 'Short Day', 'Absent', 'Anomaly']
    for atype in all_types:
        if atype not in distribution['Attendance Type'].values:
            distribution = pd.concat([distribution, pd.DataFrame({'Attendance Type': [atype], 'Count': [0]})], ignore_index=True)
    
    # Sort by predefined order
    type_order = {atype: i for i, atype in enumerate(all_types)}
    distribution['Order'] = distribution['Attendance Type'].map(type_order)
    distribution = distribution.sort_values('Order').drop('Order', axis=1)
    
    return distribution

# ============================================================================
# ADVANCED ANALYTICS: WEEKLY COMPARISON + LUNCH RISK
# ============================================================================

def calculate_expected_hours_for_range_filtered(
    employee_name: str,
    start_date: date,
    end_date: date,
    weekday_filter: Optional[List[str]] = None
) -> float:
    """
    Expected hours helper with optional weekday filtering (e.g., Fridays only).
    """
    if start_date is None or end_date is None:
        return 0.0
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    if not weekday_filter:
        return calculate_expected_hours_for_range(employee_name, start_date, end_date)

    weekday_filter_set = set(weekday_filter)
    expected_workdays, early_departure_override = get_employee_work_pattern(employee_name)
    holiday_set = get_company_holiday_set(start_date, end_date)

    total_hours = 0.0
    current = start_date
    while current <= end_date:
        weekday = current.weekday()
        weekday_name = current.strftime('%A')
        if (
            weekday < 5 and
            weekday_name in weekday_filter_set and
            weekday in expected_workdays and
            current not in holiday_set
        ):
            total_hours += get_expected_daily_hours(weekday, early_departure_override)
        current += timedelta(days=1)
    return total_hours

def calculate_weekly_employee_comparison(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    employees: Optional[List[str]] = None,
    weekdays: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Build week-wise employee comparison metrics for a selected month.
    """
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    work_df = daily_df.copy()
    work_df['Date'] = pd.to_datetime(work_df['Date'])
    work_df = work_df[
        (work_df['Date'].dt.year == year) &
        (work_df['Date'].dt.month == month)
    ].copy()

    if employees:
        work_df = work_df[work_df['Employee Full Name'].isin(employees)]
    if weekdays:
        work_df = work_df[work_df['Date'].dt.day_name().isin(set(weekdays))]

    if work_df.empty:
        return pd.DataFrame()

    for bool_col in ['Is Late', 'Is Early Departure', 'Has Anomaly', 'Missing Punch Out']:
        if bool_col not in work_df.columns:
            work_df[bool_col] = False
        work_df[bool_col] = work_df[bool_col].fillna(False).astype(bool)

    if 'Working Hours' not in work_df.columns:
        work_df['Working Hours'] = 0.0
    if 'Meal Hours' not in work_df.columns:
        work_df['Meal Hours'] = 0.0

    work_df['Working Hours'] = pd.to_numeric(work_df['Working Hours'], errors='coerce').fillna(0.0)
    if 'Overtime Eligible Hours' in work_df.columns:
        work_df['Overtime Calc Hours'] = pd.to_numeric(
            work_df['Overtime Eligible Hours'], errors='coerce'
        ).fillna(work_df['Working Hours'])
    else:
        work_df['Overtime Calc Hours'] = work_df['Working Hours']
    work_df['Meal Hours'] = pd.to_numeric(work_df['Meal Hours'], errors='coerce').fillna(0.0)
    friday_mask = work_df['Date'].dt.weekday == 4
    base_no_lunch = work_df['Meal Hours'] <= (10 / 60)
    work_df['High Risk No Lunch Day'] = (
        (work_df['Working Hours'] >= 8.0) &
        base_no_lunch
    )
    # Friday rule: ignore normal Friday lunch cases, keep 8h+ no-lunch Fridays.
    lunch_analysis_mask = (~friday_mask) | work_df['High Risk No Lunch Day']
    work_df['No Lunch Day'] = base_no_lunch & lunch_analysis_mask
    work_df['Meal Analysis Day'] = lunch_analysis_mask.astype(int)
    work_df['Meal Hours Analysis'] = np.where(lunch_analysis_mask, work_df['Meal Hours'], 0.0)

    work_df['Week Start'] = work_df['Date'] - pd.to_timedelta(work_df['Date'].dt.weekday, unit='D')
    work_df['Week End'] = work_df['Week Start'] + pd.to_timedelta(4, unit='D')

    weekly_df = work_df.groupby(['Employee Full Name', 'Week Start', 'Week End']).agg({
        'Working Hours': 'sum',
        'Overtime Calc Hours': 'sum',
        'Date': 'nunique',
        'Is Late': 'sum',
        'Is Early Departure': 'sum',
        'Has Anomaly': 'sum',
        'Missing Punch Out': 'sum',
        'Meal Hours': 'sum',
        'Meal Analysis Day': 'sum',
        'Meal Hours Analysis': 'sum',
        'No Lunch Day': 'sum',
        'High Risk No Lunch Day': 'sum'
    }).reset_index()

    weekly_df.columns = [
        'Employee Full Name', 'Week Start', 'Week End',
        'Total Working Hours', 'Overtime Calc Hours', 'Working Days',
        'Late Days', 'Early Departure Days', 'Anomaly Days',
        'Missing Punch-Out Days', 'Total Meal Hours', 'Meal Analysis Days',
        'Meal Hours (Analysis)',
        'No Lunch Days', '8h+ No Lunch Days'
    ]

    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])

    expected_hours = []
    weekday_filter = weekdays if weekdays else None
    for _, row in weekly_df.iterrows():
        week_start = max(row['Week Start'].date(), month_start)
        week_end = min(row['Week End'].date(), month_end)
        expected_hours.append(
            calculate_expected_hours_for_range_filtered(
                employee_name=row['Employee Full Name'],
                start_date=week_start,
                end_date=week_end,
                weekday_filter=weekday_filter
            )
        )

    weekly_df['Expected Hours'] = expected_hours
    weekly_df['Hours Gap'] = weekly_df['Overtime Calc Hours'] - weekly_df['Expected Hours']
    weekly_df['Overtime Hours'] = weekly_df['Hours Gap'].clip(lower=0)
    weekly_df['Avg Meal / Day (min)'] = np.where(
        weekly_df['Meal Analysis Days'] > 0,
        (weekly_df['Meal Hours (Analysis)'] * 60) / weekly_df['Meal Analysis Days'],
        0.0
    )
    weekly_df = weekly_df.drop(columns=['Meal Analysis Days', 'Meal Hours (Analysis)', 'Overtime Calc Hours'])

    week_starts = sorted(weekly_df['Week Start'].dropna().unique())
    week_index_map = {ws: idx + 1 for idx, ws in enumerate(week_starts)}
    weekly_df['Week Index'] = weekly_df['Week Start'].map(week_index_map)

    def build_week_label(ws):
        rank = week_index_map.get(ws, 0)
        ws_ts = pd.Timestamp(ws)
        label_start = max(ws_ts.date(), month_start)
        label_end = min((ws_ts + pd.Timedelta(days=4)).date(), month_end)
        return f"W{rank}: {label_start.strftime('%b %d')} - {label_end.strftime('%b %d')}"

    weekly_df['Week Label'] = weekly_df['Week Start'].apply(build_week_label)

    numeric_cols = [
        'Total Working Hours', 'Expected Hours', 'Hours Gap', 'Overtime Hours',
        'Total Meal Hours', 'Avg Meal / Day (min)'
    ]
    weekly_df[numeric_cols] = weekly_df[numeric_cols].round(2)

    weekly_df = weekly_df.sort_values(
        ['Week Index', 'Employee Full Name'],
        ascending=[True, True]
    )
    return weekly_df

def ensure_week_index_column(weekly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a canonical 'Week Index' column exists for downstream sorting.
    Backward-compatible with alternate week columns from older code paths.
    """
    if weekly_df is None or weekly_df.empty:
        return weekly_df

    normalized_df = weekly_df.copy()

    if 'Week Index' in normalized_df.columns:
        normalized_df['Week Index'] = pd.to_numeric(
            normalized_df['Week Index'],
            errors='coerce'
        )
        return normalized_df

    # 1) Preferred derivation from week-start dates (consistent with current logic)
    week_start_candidates = ['Week Start', 'Week_Start', 'WeekStart']
    week_start_col = next((c for c in week_start_candidates if c in normalized_df.columns), None)
    if week_start_col is not None:
        week_start_series = pd.to_datetime(normalized_df[week_start_col], errors='coerce')
        unique_week_starts = sorted(week_start_series.dropna().unique().tolist())
        week_map = {pd.Timestamp(ws): idx + 1 for idx, ws in enumerate(unique_week_starts)}
        normalized_df['Week Index'] = week_start_series.map(week_map)
        normalized_df['Week Index'] = pd.to_numeric(normalized_df['Week Index'], errors='coerce')
        return normalized_df

    # 2) Alternate canonical week columns from legacy/variant dataframes
    alt_week_cols = ['Week', 'Week Number', 'Week_Number', 'Week No', 'WeekNo']
    alt_col = next((c for c in alt_week_cols if c in normalized_df.columns), None)
    if alt_col is not None:
        normalized_df['Week Index'] = pd.to_numeric(normalized_df[alt_col], errors='coerce')
        return normalized_df

    # 3) Parse from labels like "W1: Jan 01 - Jan 05"
    if 'Week Label' in normalized_df.columns:
        parsed = pd.to_numeric(
            normalized_df['Week Label'].astype(str).str.extract(r'W(\d+)')[0],
            errors='coerce'
        )
        if parsed.notna().any():
            normalized_df['Week Index'] = parsed
            return normalized_df

        # If labels are non-standard, keep deterministic ordering by label text
        sorted_labels = sorted(normalized_df['Week Label'].dropna().unique().tolist())
        label_map = {label: idx + 1 for idx, label in enumerate(sorted_labels)}
        normalized_df['Week Index'] = normalized_df['Week Label'].map(label_map)
        normalized_df['Week Index'] = pd.to_numeric(normalized_df['Week Index'], errors='coerce')
        return normalized_df

    # 4) Defensive fallback: stable row-order index (keeps app functional)
    normalized_df['Week Index'] = np.arange(1, len(normalized_df) + 1, dtype=float)
    return normalized_df

def plot_weekly_comparison_heatmap(
    weekly_df: pd.DataFrame,
    selected_employees: Optional[List[str]] = None
):
    """
    Compact week-vs-employee heatmap with HR-centric hover details.
    """
    if weekly_df is None or weekly_df.empty:
        return None

    employee_pool = sorted(weekly_df['Employee Full Name'].unique().tolist())
    if selected_employees:
        ordered_selected = [emp for emp in selected_employees if emp in set(employee_pool)]
        remaining = [emp for emp in employee_pool if emp not in set(ordered_selected)]
        employee_order = ordered_selected + remaining
    else:
        employee_order = employee_pool

    week_meta = (
        weekly_df[['Week Index', 'Week Label']]
        .drop_duplicates()
        .sort_values('Week Index')
    )
    week_labels = week_meta['Week Label'].tolist()

    lookup = {}
    for _, row in weekly_df.iterrows():
        lookup[(row['Week Label'], row['Employee Full Name'])] = row

    z_values = []
    text_values = []
    custom_values = []

    for week_label in week_labels:
        z_row = []
        text_row = []
        custom_row = []
        for emp in employee_order:
            key = (week_label, emp)
            row = lookup.get(key)
            if row is None:
                total_hours = 0.0
                working_days = 0
                overtime_hours = 0.0
                late_days = 0
                early_days = 0
                anomaly_days = 0
                no_lunch_days = 0
                avg_meal_min = 0.0
            else:
                total_hours = float(row['Total Working Hours'])
                working_days = int(row['Working Days'])
                overtime_hours = float(row['Overtime Hours'])
                late_days = int(row['Late Days'])
                early_days = int(row['Early Departure Days'])
                anomaly_days = int(row['Anomaly Days'])
                no_lunch_days = int(row['No Lunch Days'])
                avg_meal_min = float(row['Avg Meal / Day (min)'])

            z_row.append(total_hours)
            text_row.append(f"{total_hours:.1f}h<br>{working_days}d")
            custom_row.append([
                working_days, overtime_hours, late_days, early_days,
                anomaly_days, no_lunch_days, avg_meal_min
            ])
        z_values.append(z_row)
        text_values.append(text_row)
        custom_values.append(custom_row)

    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=employee_order,
            y=week_labels,
            colorscale='YlGnBu',
            colorbar=dict(title='Hours'),
            text=text_values,
            texttemplate="%{text}",
            textfont={"size": 10},
            customdata=custom_values,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "%{y}<br>"
                "Total Hours: %{z:.1f}h<br>"
                "Working Days: %{customdata[0]}<br>"
                "Overtime: %{customdata[1]:.1f}h<br>"
                "Late / Early: %{customdata[2]} / %{customdata[3]}<br>"
                "Anomaly Days: %{customdata[4]}<br>"
                "No Lunch Days: %{customdata[5]}<br>"
                "Avg Meal / Day: %{customdata[6]:.1f} min"
                "<extra></extra>"
            )
        )
    )

    fig.update_layout(
        title='Weekly Employee Comparison (Hours + Presence Signal)',
        xaxis_title='Employee',
        yaxis_title='Week of Month',
        height=max(420, len(week_labels) * 65),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    fig.update_yaxes(autorange='reversed')
    return fig

def _longest_true_streak(flags: pd.Series) -> int:
    """
    Return the longest consecutive streak of truthy values.
    """
    max_streak = 0
    current_streak = 0
    for val in flags.fillna(False).tolist():
        if bool(val):
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    return max_streak

def calculate_lunch_break_risk(
    daily_df: pd.DataFrame,
    year: int,
    month: int,
    employees: Optional[List[str]] = None,
    high_work_hours: float = 8.0,
    short_lunch_minutes: int = 20,
    avg_lunch_warning_minutes: int = 25,
    long_continuous_hours: float = 6.0
) -> pd.DataFrame:
    """
    Build monthly lunch-break behavior and risk profile per employee.
    """
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    work_df = daily_df.copy()
    work_df['Date'] = pd.to_datetime(work_df['Date'])
    work_df = work_df[
        (work_df['Date'].dt.year == year) &
        (work_df['Date'].dt.month == month)
    ].copy()

    if employees:
        work_df = work_df[work_df['Employee Full Name'].isin(employees)]
    if work_df.empty:
        return pd.DataFrame()

    if 'Working Hours' not in work_df.columns:
        work_df['Working Hours'] = 0.0
    if 'Meal Hours' not in work_df.columns:
        work_df['Meal Hours'] = 0.0

    work_df['Working Hours'] = pd.to_numeric(work_df['Working Hours'], errors='coerce').fillna(0.0)
    work_df['Meal Hours'] = pd.to_numeric(work_df['Meal Hours'], errors='coerce').fillna(0.0)
    work_df = work_df[work_df['Working Hours'] > 0].copy()
    if work_df.empty:
        return pd.DataFrame()

    if 'Break Count' in work_df.columns:
        break_counts = pd.to_numeric(work_df['Break Count'], errors='coerce').fillna(0)
    else:
        break_counts = pd.Series(0, index=work_df.index)

    work_df['Meal Minutes'] = work_df['Meal Hours'] * 60
    friday_mask = work_df['Date'].dt.weekday == 4
    no_lunch_base = work_df['Meal Minutes'] <= 1
    work_df['High-Risk No Lunch Day'] = (
        (work_df['Working Hours'] >= high_work_hours) &
        no_lunch_base
    )
    # Friday rule: ignore normal Friday lunch cases, keep 8h+ no-lunch Fridays.
    lunch_analysis_mask = (~friday_mask) | work_df['High-Risk No Lunch Day']
    work_df['Lunch Analysis Day'] = lunch_analysis_mask
    work_df['No Lunch Day'] = no_lunch_base & lunch_analysis_mask
    work_df['Short Lunch Day'] = (
        (work_df['Meal Minutes'] > 1) &
        (work_df['Meal Minutes'] < short_lunch_minutes) &
        lunch_analysis_mask
    )
    long_continuous_mask = (
        ((break_counts <= 0) & (work_df['Working Hours'] >= long_continuous_hours)) |
        ((work_df['Meal Minutes'] < max(10, short_lunch_minutes * 0.5)) & (work_df['Working Hours'] >= high_work_hours))
    )
    work_df['Long Continuous Work Day'] = long_continuous_mask & lunch_analysis_mask

    risk_rows = []
    for employee_name, emp_df in work_df.groupby('Employee Full Name'):
        emp_df = emp_df.sort_values('Date')
        analysis_df = emp_df[emp_df['Lunch Analysis Day']].copy()
        working_days = int(analysis_df['Date'].nunique())
        total_work_hours = float(analysis_df['Working Hours'].sum())
        total_meal_hours = float(analysis_df['Meal Hours'].sum())
        avg_lunch_minutes = (total_meal_hours * 60 / working_days) if working_days > 0 else 0.0

        no_lunch_days = int(analysis_df['No Lunch Day'].sum())
        short_lunch_days = int(analysis_df['Short Lunch Day'].sum())
        long_continuous_days = int(analysis_df['Long Continuous Work Day'].sum())
        high_risk_days = int(analysis_df['High-Risk No Lunch Day'].sum())
        max_no_lunch_streak = _longest_true_streak(analysis_df['No Lunch Day']) if working_days > 0 else 0
        max_high_risk_streak = _longest_true_streak(analysis_df['High-Risk No Lunch Day']) if working_days > 0 else 0

        risk_score = 0
        risk_score += high_risk_days * 2
        risk_score += 2 if (working_days > 0 and avg_lunch_minutes < avg_lunch_warning_minutes) else 0
        risk_score += 2 if no_lunch_days >= 3 else 0
        risk_score += 2 if short_lunch_days >= 4 else 0
        risk_score += 2 if long_continuous_days >= 3 else 0
        risk_score += 3 if max_high_risk_streak >= 3 else 0

        if max_high_risk_streak >= 3 or high_risk_days >= 5 or (working_days > 0 and avg_lunch_minutes < 10):
            risk_level = 'Critical'
        elif risk_score >= 8 or high_risk_days >= 3:
            risk_level = 'High'
        elif risk_score >= 4 or (working_days > 0 and avg_lunch_minutes < avg_lunch_warning_minutes):
            risk_level = 'Warning'
        else:
            risk_level = 'Low'

        reasons = []
        if high_risk_days > 0:
            reasons.append(f"{high_risk_days} day(s) worked {high_work_hours:.1f}h+ with no lunch")
        if working_days > 0 and avg_lunch_minutes < avg_lunch_warning_minutes:
            reasons.append(f"Average lunch is only {avg_lunch_minutes:.1f} min")
        if max_high_risk_streak >= 2:
            reasons.append(f"{max_high_risk_streak}-day consecutive high-risk streak")
        if long_continuous_days > 0:
            reasons.append(f"{long_continuous_days} long continuous-work day(s)")
        if not reasons:
            if working_days == 0:
                reasons.append("Only standard Friday cases found; excluded from lunch analysis.")
            else:
                reasons.append("Lunch behavior appears stable in this month.")

        risk_rows.append({
            'Employee Full Name': employee_name,
            'Working Days': working_days,
            'Total Working Hours': round(total_work_hours, 2),
            'Total Meal Hours': round(total_meal_hours, 2),
            'Avg Lunch Minutes': round(avg_lunch_minutes, 2),
            'No Lunch Days': no_lunch_days,
            'Short Lunch Days': short_lunch_days,
            'Long Continuous Work Days': long_continuous_days,
            'High-Risk No Lunch Days': high_risk_days,
            'Max No Lunch Streak': max_no_lunch_streak,
            'Max High-Risk Streak': max_high_risk_streak,
            'Risk Score': int(risk_score),
            'Risk Level': risk_level,
            'Risk Drivers': '; '.join(reasons[:3])
        })

    risk_df = pd.DataFrame(risk_rows)
    if risk_df.empty:
        return risk_df

    severity_order = {'Critical': 0, 'High': 1, 'Warning': 2, 'Low': 3}
    risk_df['Risk Order'] = risk_df['Risk Level'].map(severity_order).fillna(99)
    risk_df = risk_df.sort_values(
        ['Risk Order', 'Risk Score', 'High-Risk No Lunch Days', 'No Lunch Days'],
        ascending=[True, False, False, False]
    ).drop(columns=['Risk Order'])
    return risk_df

def plot_lunch_risk_bar_chart(risk_df: pd.DataFrame, top_n: int = 15):
    """
    Horizontal bar chart ranking employees by lunch-break risk score.
    """
    if risk_df is None or risk_df.empty:
        return None

    chart_df = risk_df.head(top_n).sort_values('Risk Score', ascending=True)
    color_map = {
        'Critical': '#b42323',
        'High': '#e67e22',
        'Warning': '#caa531',
        'Low': '#2f9e44'
    }

    fig = px.bar(
        chart_df,
        x='Risk Score',
        y='Employee Full Name',
        orientation='h',
        color='Risk Level',
        color_discrete_map=color_map,
        hover_data={
            'High-Risk No Lunch Days': True,
            'No Lunch Days': True,
            'Short Lunch Days': True,
            'Avg Lunch Minutes': ':.1f',
            'Risk Score': True
        },
        title=f'Top {min(top_n, len(chart_df))} Employees by Lunch Risk Score'
    )
    fig.update_layout(
        height=max(420, len(chart_df) * 32),
        xaxis_title='Risk Score',
        yaxis_title='Employee',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def plot_lunch_risk_scatter(risk_df: pd.DataFrame, avg_lunch_warning_minutes: int = 25):
    """
    Relationship view: work volume vs lunch quality, by risk level.
    """
    if risk_df is None or risk_df.empty:
        return None

    color_map = {
        'Critical': '#b42323',
        'High': '#e67e22',
        'Warning': '#caa531',
        'Low': '#2f9e44'
    }

    fig = px.scatter(
        risk_df,
        x='Total Working Hours',
        y='Avg Lunch Minutes',
        size='No Lunch Days',
        color='Risk Level',
        hover_name='Employee Full Name',
        hover_data={
            'Risk Score': True,
            'Working Days': True,
            'High-Risk No Lunch Days': True,
            'Long Continuous Work Days': True,
            'No Lunch Days': True
        },
        color_discrete_map=color_map,
        title='Lunch Risk Positioning: Workload vs Meal Recovery'
    )
    fig.add_hline(
        y=avg_lunch_warning_minutes,
        line_dash='dash',
        line_color='#cf6d21',
        annotation_text=f'Warning threshold: {avg_lunch_warning_minutes} min',
        annotation_position='bottom right'
    )
    fig.update_layout(
        height=460,
        xaxis_title='Total Working Hours (Month)',
        yaxis_title='Average Lunch per Working Day (min)',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def create_attendance_calendar(daily_df: pd.DataFrame, employee_name: str, year: int, month: int):
    """
    Create a calendar view for employee attendance in a specific month, with updated business logic.
    """
    # Filter to employee and month
    emp_df = daily_df[daily_df['Employee Full Name'] == employee_name].copy()
    emp_df['Date'] = pd.to_datetime(emp_df['Date'])
    emp_df = emp_df[(emp_df['Date'].dt.year == year) & (emp_df['Date'].dt.month == month)]
    
    # Create calendar data structure (weekdays only)
    cal = calendar.Calendar(firstweekday=0)  # Start with Monday
    
    # Get all days in the month and keep Monday-Friday only
    month_days = cal.monthdayscalendar(year, month)
    weekday_month_days = [week[:5] for week in month_days if any(day != 0 for day in week[:5])]
    
    # Create date mapping
    date_status = {row['Date'].day: row for _, row in emp_df.iterrows()}
    holiday_map = get_company_holidays(year)
    holiday_map = get_company_holidays(year)
    holiday_map = get_company_holidays(year)
    
    # Build HTML calendar - Start with header
    month_name = calendar.month_name[month]
    html = f'<div style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px;">'
    html += f'<h3 style="text-align: center; color: #2E86AB; margin-bottom: 20px;">{month_name} {year} - {employee_name}</h3>'
    html += '<table style="width: 100%; border-collapse: collapse; background-color: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">'
    html += '<thead><tr style="background-color: #2E86AB; color: white;">'
    html += '<th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Mon</th>'
    html += '<th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Tue</th>'
    html += '<th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Wed</th>'
    html += '<th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Thu</th>'
    html += '<th style="padding: 12px; text-align: center; border: 1px solid #ddd;">Fri</th>'
    html += '</tr></thead><tbody>'
    
    # Color mapping
    colors = {
        'full': '#4CAF50',      # Green
        'half': '#FFC107',      # Yellow
        'short': '#FF9800',     # Orange
        'absent': '#F44336',    # Red
        'anomaly': '#9C27B0',    # Purple
        'holiday': '#e0ecff',   # Light blue
        'weekoff': '#e0e0e0'    # Grey
    }
    
    status_labels = {
        'full': 'Full Day',
        'half': 'Half Day',
        'short': 'Short Day',
        'absent': 'Absent',
        'anomaly': 'Anomaly',
        'holiday': 'Holiday',
        'weekoff': 'Week Off'
    }
    
    for week in weekday_month_days:
        html += "<tr>"
        for day in week:
            if day == 0:
                html += '<td style="padding: 15px; border: 1px solid #ddd; background-color: #f5f5f5;"></td>'
                continue

            # Determine weekday (0=Mon, 6=Sun)
            current_date = datetime(year, month, day)
            weekday = current_date.weekday()
            holiday_name = holiday_map.get(current_date.date())
            is_holiday = (holiday_name is not None) and weekday < 5

            # Handle Weekdays
            day_info = date_status.get(day)
            
            if day_info is not None:
                status = 'absent' # Default
                # Use Shift Type calculated in compliance logic
                shift_type = day_info.get('Shift Type', 'Absent')
                
                if day_info.get('Has Anomaly', False):
                    status = 'anomaly'
                elif shift_type == 'Full Day':
                    status = 'full'
                elif shift_type == 'Half Day':
                    status = 'half'
                elif shift_type == 'Short Shift':
                    status = 'short'
                
                hours = day_info['Working Hours']
                meal_hours = float(day_info.get('Meal Hours', 0.0) or 0.0)
                bg_color = colors.get(status, '#ffffff')
                label = status_labels.get(status, '')
                if is_holiday:
                    bg_color = colors['holiday']
                    label = status_labels['holiday']
                
                # Tooltip info
                info = f"Status: {shift_type} | Hours: {hours:.1f}h"
                if is_holiday and holiday_name:
                    info = f"Holiday: {holiday_name} | {info}"
                info += f" | Meal: {meal_hours:.1f}h"
                if pd.notna(day_info.get('First Punch In')):
                    info += f" | In: {day_info['First Punch In'].strftime('%H:%M')}"
                if pd.notna(day_info.get('Last Punch Out')):
                    info += f" | Out: {day_info['Last Punch Out'].strftime('%H:%M')}"
                if day_info.get('Is Late', False):
                    info += " | Late"
                if day_info.get('Is Very Late', False):
                    info += " (Very Late)"
                if day_info.get('Is Early Departure', False):
                    info += " | Early Departure"
                
                info_escaped = info.replace('"', '&quot;')
                html += f'<td style="padding: 10px; text-align: center; border: 1px solid #ddd; background-color: {bg_color}; color: white; font-weight: bold; min-width: 100px;" title="{info_escaped}">'
                html += f'<div style="font-size: 16px; font-weight: bold;">{day}</div>'
                html += f'<div style="font-size: 10px; margin-top: 3px; opacity: 0.9;">{label}</div>'
                
                # In/Out times
                if pd.notna(day_info.get('First Punch In')) or pd.notna(day_info.get('Last Punch Out')):
                    html += '<div style="font-size: 9px; margin-top: 4px; line-height: 1.2; opacity: 0.85;">'
                    if pd.notna(day_info.get('First Punch In')):
                        html += f'<div>In: {day_info["First Punch In"].strftime("%H:%M")}</div>'
                    if pd.notna(day_info.get('Last Punch Out')):
                        html += f'<div>Out: {day_info["Last Punch Out"].strftime("%H:%M")}</div>'
                    html += f'<div>Meal: {meal_hours:.1f}h</div>'
                    html += '</div>'
                
                html += '</td>'
            else:
                if is_holiday:
                    bg_color = colors['holiday']
                    label = status_labels['holiday']
                    title_text = f"Holiday: {holiday_name}" if holiday_name else "Holiday"
                    html += f'<td style="padding: 10px; text-align: center; border: 1px solid #ddd; background-color: {bg_color}; color: #1a1a1a; font-weight: bold; min-width: 100px;" title="{title_text}">'
                    html += f'<div style="font-size: 16px; font-weight: bold;">{day}</div>'
                    html += f'<div style="font-size: 10px; margin-top: 3px; opacity: 0.9;">{label}</div>'
                    html += '</td>'
                else:
                    # Day with no punches (absent)
                    bg_color = colors['absent']
                    label = status_labels['absent']
                    html += f'<td style="padding: 10px; text-align: center; border: 1px solid #ddd; background-color: {bg_color}; color: white; font-weight: bold; min-width: 100px;" title="Absent">'
                    html += f'<div style="font-size: 16px; font-weight: bold;">{day}</div>'
                    html += f'<div style="font-size: 10px; margin-top: 3px; opacity: 0.9;">{label}</div>'
                    html += '</td>'

        html += "</tr>"
    
    html += '</tbody></table>'
    html += '<div style="margin-top: 20px; padding: 15px; background-color: #f9f9f9; border-radius: 5px;">'
    html += '<div style="display: flex; flex-wrap: wrap; gap: 20px; justify-content: center;">'
    html += '<div style="display: flex; align-items: center;"><div style="width: 30px; height: 20px; background-color: #4CAF50; margin-right: 8px; border: 1px solid #ddd;"></div><span>Full Day</span></div>'
    html += '<div style="display: flex; align-items: center;"><div style="width: 30px; height: 20px; background-color: #FFC107; margin-right: 8px; border: 1px solid #ddd;"></div><span>Half Day</span></div>'
    html += '<div style="display: flex; align-items: center;"><div style="width: 30px; height: 20px; background-color: #FF9800; margin-right: 8px; border: 1px solid #ddd;"></div><span>Short Day</span></div>'
    html += '<div style="display: flex; align-items.center;"><div style="width: 30px; height: 20px; background-color: #F44336; margin-right: 8px; border: 1px solid #ddd;"></div><span>Absent / No Punch</span></div>'
    html += '<div style="display: flex; align-items: center;"><div style="width: 30px; height: 20px; background-color: #9C27B0; margin-right: 8px; border: 1px solid #ddd;"></div><span>Anomaly</span></div>'
    html += '<div style="display: flex; align-items: center;"><div style="width: 30px; height: 20px; background-color: #e0e0e0; margin-right: 8px; border: 1px solid #ddd;"></div><span>Week Off</span></div>'
    html += '</div></div></div>'
    
    return html

def get_employee_work_pattern(employee_name: str):
    """
    Return expected workdays and optional early departure override for an employee.
    """
    first_name = str(employee_name).strip().split()[0].title() if employee_name else ''
    default_workdays = {0, 1, 2, 3, 4}
    work_patterns = {
        'Jaime': {'workdays': {0, 3}, 'early_departure': time(15, 0)},
        'Susan': {'workdays': {0, 4}},
        'Breanne': {'workdays': {1, 2, 3, 4}},
        'Mhykeisha': {'workdays': {0, 1, 2, 3}},
        'Candice': {'workdays': {1, 2, 3}}
    }
    pattern = work_patterns.get(first_name, {'workdays': default_workdays})
    return pattern['workdays'], pattern.get('early_departure')

def calculate_work_pattern_summary(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> Dict[str, int]:
    """
    Summarize attendance counts using employee-specific work patterns.
    """
    emp_df = daily_df[daily_df['Employee Full Name'] == employee_name].copy()
    emp_df['Date'] = pd.to_datetime(emp_df['Date'])
    emp_df = emp_df[(emp_df['Date'].dt.year == year) & (emp_df['Date'].dt.month == month)]
    
    expected_workdays, _ = get_employee_work_pattern(employee_name)
    holiday_map = get_effective_holiday_map(year, special_day_items)
    date_status = {row['Date'].day: row for _, row in emp_df.iterrows()}
    days_in_month = calendar.monthrange(year, month)[1]
    
    summary = {
        'total_days': days_in_month,
        'full_days': 0,
        'half_days': 0,
        'short_days': 0,
        'absent_days': 0,
        'week_off_days': 0,
        'holiday_days': 0,
        'worked_non_working_days': 0
    }
    
    for day in range(1, days_in_month + 1):
        current_date = datetime(year, month, day)
        weekday = current_date.weekday()
        is_weekend = weekday >= 5
        holiday_name = holiday_map.get(current_date.date())
        is_holiday = (holiday_name is not None) and not is_weekend
        is_expected_workday = (weekday in expected_workdays) and not is_weekend and not is_holiday
        day_info = date_status.get(day)

        if is_holiday:
            if day_info is not None:
                summary['worked_non_working_days'] += 1
            else:
                summary['holiday_days'] += 1
            continue
        
        if day_info is not None:
            if not is_expected_workday:
                summary['worked_non_working_days'] += 1
                continue
            
            shift_type = day_info.get('Shift Type', 'Absent')
            if pd.isna(shift_type):
                shift_type = 'Absent'
            
            if shift_type == 'Full Day':
                summary['full_days'] += 1
            elif shift_type == 'Half Day':
                summary['half_days'] += 1
            elif shift_type == 'Short Shift' or day_info.get('Working Hours', 0) > 0:
                summary['short_days'] += 1
            else:
                summary['absent_days'] += 1
        else:
            if is_expected_workday:
                summary['absent_days'] += 1
            else:
                summary['week_off_days'] += 1
    
    return summary

def calculate_work_pattern_distribution(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> pd.DataFrame:
    """
    Build a distribution DataFrame based on work pattern summary.
    """
    summary = calculate_work_pattern_summary(daily_df, employee_name, year, month, special_day_items)
    distribution = [
        {'Attendance Type': 'Full Day', 'Count': summary['full_days']},
        {'Attendance Type': 'Half Day', 'Count': summary['half_days']},
        {'Attendance Type': 'Short Day', 'Count': summary['short_days']},
        {'Attendance Type': 'Absent', 'Count': summary['absent_days']},
        {'Attendance Type': 'Holiday', 'Count': summary['holiday_days']},
        {'Attendance Type': 'Week Off', 'Count': summary['week_off_days']},
        {'Attendance Type': 'Worked on Non-Working Day', 'Count': summary['worked_non_working_days']}
    ]
    return pd.DataFrame(distribution)

def get_work_pattern_context_text(employee_name: str) -> str:
    """
    Build a short, human-readable message for custom work patterns.
    """
    expected_workdays, early_departure_override = get_employee_work_pattern(employee_name)
    default_workdays = {0, 1, 2, 3, 4}
    if expected_workdays == default_workdays and not early_departure_override:
        return ""

    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    workday_names = [day_names[idx] for idx in sorted(expected_workdays)]

    if len(workday_names) == 1:
        days_text = workday_names[0]
    elif len(workday_names) == 2:
        days_text = f"{workday_names[0]} and {workday_names[1]}"
    else:
        days_text = f"{', '.join(workday_names[:-1])}, and {workday_names[-1]}"

    first_name = str(employee_name).strip().split()[0].title() if employee_name else "This employee"
    message = f"{first_name} works only on {days_text}."

    if early_departure_override:
        time_text = early_departure_override.strftime('%I:%M %p').lstrip('0')
        message += f" Early departure threshold: {time_text}."

    return message

def get_expected_daily_hours(weekday: int, early_departure_override=None) -> float:
    """
    Return expected daily hours based on weekday rules or an override.
    """
    if weekday >= 5:
        return 0.0

    if early_departure_override:
        end_time = early_departure_override
    else:
        end_time = Config.EARLY_DEPARTURE_TIME_FRI if weekday == 4 else Config.EARLY_DEPARTURE_TIME_MON_THU

    start_time = Config.STANDARD_START_TIME
    expected_hours = (
        datetime.combine(datetime.today(), end_time) -
        datetime.combine(datetime.today(), start_time)
    ).total_seconds() / 3600
    return max(0.0, expected_hours)

def calculate_expected_hours_for_range(employee_name: str, start_date: date, end_date: date) -> float:
    """
    Calculate expected hours for an employee across a date range (inclusive),
    excluding weekends and company holidays.
    """
    if start_date is None or end_date is None:
        return 0.0
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    expected_workdays, early_departure_override = get_employee_work_pattern(employee_name)
    holiday_set = get_company_holiday_set(start_date, end_date)

    total_hours = 0.0
    current = start_date
    while current <= end_date:
        weekday = current.weekday()
        if weekday < 5 and weekday in expected_workdays and current not in holiday_set:
            total_hours += get_expected_daily_hours(weekday, early_departure_override)
        current += timedelta(days=1)
    return total_hours

def calculate_work_pattern_kpis(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
) -> Dict[str, float]:
    """
    Calculate expected vs actual and punctuality KPIs for the work pattern calendar.
    """
    emp_df = daily_df[daily_df['Employee Full Name'] == employee_name].copy()
    emp_df['Date'] = pd.to_datetime(emp_df['Date'])
    emp_df = emp_df[(emp_df['Date'].dt.year == year) & (emp_df['Date'].dt.month == month)]
    actual_hours_all_days = emp_df['Working Hours'].sum() if not emp_df.empty else 0.0

    expected_workdays, early_departure_override = get_employee_work_pattern(employee_name)
    days_in_month = calendar.monthrange(year, month)[1]
    holiday_map = get_effective_holiday_map(year, special_day_items)

    expected_dates = []
    expected_hours = 0.0
    for day in range(1, days_in_month + 1):
        current_date = datetime(year, month, day)
        weekday = current_date.weekday()
        is_weekend = weekday >= 5
        holiday_name = holiday_map.get(current_date.date())
        is_holiday = (holiday_name is not None) and not is_weekend
        is_expected = (weekday in expected_workdays) and not is_weekend and not is_holiday
        if is_expected:
            expected_dates.append(current_date.date())
            expected_hours += get_expected_daily_hours(weekday, early_departure_override)

    expected_days = len(expected_dates)

    if emp_df.empty:
        expected_df = emp_df
        non_working_df = emp_df
    else:
        emp_df['DateOnly'] = emp_df['Date'].dt.date
        expected_df = emp_df[emp_df['DateOnly'].isin(expected_dates)]
        non_working_df = emp_df[~emp_df['DateOnly'].isin(expected_dates)]

    actual_days = len(expected_df)
    actual_hours = expected_df['Working Hours'].sum() if not expected_df.empty else 0.0
    missed_days = max(0, expected_days - actual_days)

    if not expected_df.empty:
        late_series = expected_df['Is Late'].fillna(False)
        if early_departure_override:
            early_series = expected_df['Last Punch Out'].apply(
                lambda x: pd.notna(x) and x.time() < early_departure_override
            )
        else:
            early_series = expected_df['Is Early Departure'].fillna(False)

        early_series = early_series.fillna(False)
        late_count = int(late_series.sum())
        early_count = int(early_series.sum())
        on_time_days = int((~late_series & ~early_series).sum())

        work_hours_series = pd.to_numeric(expected_df['Working Hours'], errors='coerce').fillna(0.0)
        if 'Meal Hours' in expected_df.columns:
            meal_hours_series = pd.to_numeric(expected_df['Meal Hours'], errors='coerce').fillna(0.0)
        else:
            meal_hours_series = pd.Series(0.0, index=expected_df.index)

        # Working-day lunch KPIs (expected workdays only -> excludes holidays and week-offs)
        worked_day_mask = work_hours_series > 0
        no_lunch_mask = meal_hours_series <= 0
        friday_mask = expected_df['Date'].dt.weekday == 4
        high_risk_no_lunch_mask = worked_day_mask & (work_hours_series >= 8.0) & no_lunch_mask
        # Friday rule: ignore normal Friday no-lunch counts, keep 8h+ no-lunch Fridays.
        lunch_analysis_mask = (~friday_mask) | high_risk_no_lunch_mask
        no_lunch_working_days = int((worked_day_mask & no_lunch_mask & lunch_analysis_mask).sum())
        high_risk_no_lunch_days = int(
            high_risk_no_lunch_mask.sum()
        )
    else:
        late_count = 0
        early_count = 0
        on_time_days = 0
        no_lunch_working_days = 0
        high_risk_no_lunch_days = 0

    return {
        'expected_days': expected_days,
        'actual_days': actual_days,
        'missed_days': missed_days,
        'expected_hours': expected_hours,
        'actual_hours': actual_hours,
        'actual_hours_all_days': actual_hours_all_days,
        'hours_diff': actual_hours - expected_hours,
        'late_arrivals': late_count,
        'early_departures': early_count,
        'on_time_days': on_time_days,
        'worked_non_working_days': len(non_working_df),
        'high_risk_no_lunch_days': high_risk_no_lunch_days,
        'no_lunch_working_days': no_lunch_working_days
    }

def create_work_pattern_calendar(
    daily_df: pd.DataFrame,
    employee_name: str,
    year: int,
    month: int,
    kpi_data: Optional[Dict[str, float]] = None,
    special_day_items: Optional[Tuple[Tuple[str, str, str], ...]] = None
):
    """
    Create a calendar view for employee attendance with employee-specific work patterns.
    """
    # Filter to employee and month
    emp_df = daily_df[daily_df['Employee Full Name'] == employee_name].copy()
    emp_df['Date'] = pd.to_datetime(emp_df['Date'])
    emp_df = emp_df[(emp_df['Date'].dt.year == year) & (emp_df['Date'].dt.month == month)]
    
    # Employee-specific work patterns (weekday: 0=Mon, 6=Sun)
    expected_workdays, early_departure_override = get_employee_work_pattern(employee_name)
    
    # Create calendar data structure (weekdays only)
    cal = calendar.Calendar(firstweekday=0)  # Start with Monday
    
    # Get all days in the month and keep Monday-Friday only
    month_days = cal.monthdayscalendar(year, month)
    weekday_month_days = [week[:5] for week in month_days if any(day != 0 for day in week[:5])]
    
    # Create date mapping
    date_status = {row['Date'].day: row for _, row in emp_df.iterrows()}
    holiday_map = get_effective_holiday_map(year, special_day_items)

    if kpi_data is None:
        kpi_data = calculate_work_pattern_kpis(daily_df, employee_name, year, month, special_day_items)

    special_day_map = build_special_day_map(special_day_items)

    expected_days = int(kpi_data.get('expected_days', 0) or 0)
    actual_days = int(kpi_data.get('actual_days', 0) or 0)
    missed_days = int(kpi_data.get('missed_days', 0) or 0)
    expected_hours = float(kpi_data.get('expected_hours', 0.0) or 0.0)
    actual_hours = float(kpi_data.get('actual_hours', 0.0) or 0.0)
    hours_diff = float(kpi_data.get('hours_diff', 0.0) or 0.0)
    on_time_days = int(kpi_data.get('on_time_days', 0) or 0)
    late_arrivals = int(kpi_data.get('late_arrivals', 0) or 0)
    early_departures = int(kpi_data.get('early_departures', 0) or 0)
    worked_non_working_days = int(kpi_data.get('worked_non_working_days', 0) or 0)
    anomaly_days = int(emp_df['Has Anomaly'].sum()) if 'Has Anomaly' in emp_df.columns else 0

    attendance_rate = round((actual_days / expected_days) * 100) if expected_days else 0
    punctuality_rate = round((on_time_days / expected_days) * 100) if expected_days else 0

    def rate_class(value: float, good: float, warn: float) -> str:
        if value >= good:
            return 'good'
        if value >= warn:
            return 'warn'
        return 'bad'

    if expected_days == 0:
        attendance_class = punctuality_class = hours_class = exceptions_class = 'neutral'
        overall_class = 'neutral'
        overall_label = 'No Expected Days'
        overall_score_text = 'NA'
        status_note = 'No scheduled workdays in this month.'
    else:
        attendance_class = rate_class(attendance_rate, 90, 75)
        punctuality_class = rate_class(punctuality_rate, 85, 70)

        abs_hours_diff = abs(hours_diff)
        if abs_hours_diff <= 4:
            hours_class = 'good'
        elif abs_hours_diff <= 10:
            hours_class = 'warn'
        else:
            hours_class = 'bad'

        if missed_days == 0 and anomaly_days == 0 and worked_non_working_days == 0:
            exceptions_class = 'good'
        elif missed_days <= 2 and anomaly_days <= 1:
            exceptions_class = 'warn'
        else:
            exceptions_class = 'bad'

        overall_score = attendance_rate * 0.6 + punctuality_rate * 0.4
        overall_class = rate_class(overall_score, 85, 70)
        overall_label = {'good': 'On Track', 'warn': 'Needs Attention', 'bad': 'At Risk'}[overall_class]
        overall_score_text = f"{round(overall_score)}%"

        if missed_days > 0:
            status_note = f"{missed_days} expected day(s) missed."
        elif late_arrivals > 0 or early_departures > 0:
            status_note = "Timing flags present."
        else:
            status_note = "Attendance and timing look steady."
    
    # Build HTML calendar - Start with header
    month_name = calendar.month_name[month]
    hours_balance_text = f"{hours_diff:+.1f}h"
    if expected_days:
        hours_balance_note = f"{actual_hours:.1f}h actual / {expected_hours:.1f}h expected"
    else:
        hours_balance_note = f"{actual_hours:.1f}h logged"
    attendance_value = f"{attendance_rate}%" if expected_days else "NA"
    punctuality_value = f"{punctuality_rate}%" if expected_days else "NA"

    html = '<div class="cal-wrap">'
    html += """
        <style>
        html,body{margin:0;padding:0;height:auto;max-height:none;overflow:visible;}
        .cal-wrap{font-family:Arial,sans-serif;max-width:980px;height:auto;max-height:none;overflow:visible;margin:0 auto 20px;padding:16px 16px 24px;box-sizing:border-box;background:linear-gradient(180deg,#f5f8fb 0%,#fff 60%);border:1px solid #dde6ef;border-radius:12px;box-shadow:0 6px 18px rgba(22,43,60,.08);}
        .cal-header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;}
        .cal-title{font-size:20px;font-weight:700;color:#1f5f7a;}
        .cal-subtitle{font-size:14px;font-weight:600;color:#4b5b66;}
        .status-summary{background:#fff;border:1px solid #dde6ef;border-radius:10px;padding:8px 10px;min-width:220px;}
        .status-label{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:#5d6c79;}
        .status-value{font-size:20px;font-weight:700;margin-top:4px;}
        .status-chip{display:inline-block;margin-top:6px;padding:4px 8px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;}
        .status-chip.good{background:#e8f6ee;color:#1e5d2a;border:1px solid #bfe4c9;}
        .status-chip.warn{background:#fff7dd;color:#7a5d00;border:1px solid #f3dd97;}
        .status-chip.bad{background:#ffe7e7;color:#8a1f1f;border:1px solid #f1b5b5;}
        .status-chip.neutral{background:#eef2f6;color:#4b5b66;border:1px solid #d5dde6;}
        .status-note{font-size:12px;color:#5d6c79;margin-top:6px;}
        .insight-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-top:12px;}
        .insight-card{background:#fff;border:1px solid #dde6ef;border-radius:10px;padding:10px;}
        .insight-card.good{border-left:4px solid #2f9e44;}
        .insight-card.warn{border-left:4px solid #f2c94c;}
        .insight-card.bad{border-left:4px solid #eb5757;}
        .insight-card.neutral{border-left:4px solid #cbd5df;}
        .insight-label{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:#5d6c79;}
        .insight-value{font-size:18px;font-weight:700;margin-top:4px;}
        .insight-sub,.insight-foot{font-size:12px;color:#5d6c79;margin-top:2px;}
        .cal-table{width:100%;border-collapse:separate;border-spacing:6px;margin-top:14px;margin-bottom:8px;table-layout:fixed;}
        .cal-table th{background:#2E86AB;color:#fff;padding:8px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;border-radius:8px;}
        .cell-body{display:flex;flex-direction:column;height:100%;padding-bottom:16px;}
        .day-top{display:flex;justify-content:space-between;align-items:center;gap:6px;min-height:18px;}
        .day-num{font-size:14px;font-weight:700;}
        .status-pill{display:inline-block;padding:2px 6px;border-radius:999px;font-size:9px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:#fff;}
        .hours-pill{display:inline-block;padding:2px 6px;border-radius:6px;font-size:10px;font-weight:700;background:#fff;border:1px solid #dde6ef;}
        .badge-row{display:flex;flex-wrap:wrap;gap:4px;margin-top:4px;min-height:18px;}
        .badge{font-size:9px;font-weight:700;padding:2px 5px;border-radius:4px;border:1px solid transparent;background:#fff;color:#1a1a1a;}
        .badge-late{border-color:#eb5757;color:#8a1f1f;}
        .badge-vlate{border-color:#b42323;background:#ffe1e1;color:#7b1515;}
        .badge-early{border-color:#f2994a;color:#8a4a00;}
        .badge-miss{border-color:#6c757d;color:#3f4a54;}
        .badge-anom{border-color:#6c4ab6;color:#3d2a6d;}
        .badge-holiday{border-color:#2E86AB;background:#e8f1ff;color:#1f5f7a;}
        .ot-pill{font-size:9px;font-weight:700;padding:2px 6px;border-radius:6px;background:#edf0f3;border:1px dashed #c7d1dc;color:#2f3a43;}
        .badge-special{border-color:#1f7a6b;background:#e6f6f4;color:#145b4f;}
        .time-range{font-size:10px;color:#4b5b66;margin-top:4px;min-height:12px;line-height:1.35;}
        .time-chunk{display:inline-block;margin-right:8px;}
        .time-label{color:#4b5b66;}
        .time-value{font-weight:600;color:#4b5b66;}
        .time-value.alert{color:#c7392f;}
        .meal-text{font-size:10px;color:#6b7280;margin-top:2px;min-height:12px;}
        .meal-text.meal-risk-warning{color:#7a5d00;background:rgba(242,201,76,0.18);border:1px solid rgba(242,201,76,0.35);padding:1px 4px;border-radius:4px;font-weight:600;display:inline-block;}
        .meal-text.meal-risk-critical{color:#8a1f1f;background:rgba(235,87,87,0.16);border:1px solid rgba(235,87,87,0.35);padding:1px 4px;border-radius:4px;font-weight:700;display:inline-block;}
        .cell-bar{position:absolute;left:8px;right:8px;bottom:8px;}
        .hours-track{margin-top:6px;height:6px;background:#e5ebf1;border-radius:999px;overflow:hidden;}
        .hours-track.placeholder{opacity:0;}
        .hours-fill{height:100%;display:block;background:#2f9e44;}
        .hours-fill.warn{background:#f2c94c;}
        .hours-fill.bad{background:#eb5757;}
        .hours-fill.off{background:#2e86ab;}
        .hours-fill.zero{background:#c8d0d9;}
        .off-tag{position:absolute;top:6px;right:6px;font-size:9px;font-weight:700;padding:2px 5px;border-radius:6px;background:#fff;border:1px solid #607d8b;color:#42535e;}
        .legend{height:auto;max-height:none;overflow:visible;margin-top:14px;margin-bottom:8px;padding:10px;background:#f7f9fb;border:1px solid #dde6ef;border-radius:10px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;}
        .legend-title{font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:#5d6c79;margin-bottom:6px;}
        .legend-item{display:flex;align-items:center;gap:6px;font-size:12px;color:#1f2933;margin-bottom:4px;}
        .legend-swatch{width:14px;height:14px;border-radius:4px;border:1px solid #dde6ef;background:#fff;}
        .legend-bar{width:24px;height:6px;border-radius:999px;background:#e5ebf1;position:relative;overflow:hidden;}
        .legend-bar:after{content:'';position:absolute;left:0;top:0;height:100%;width:60%;background:#2f9e44;}
        @media (max-width:900px){.cal-wrap{padding:10px}.cal-table{border-spacing:4px}.cal-title{font-size:18px}}
        </style>
    """
    html += (
        f'<div class="cal-header"><div><div class="cal-title">{month_name} {year}</div>'
        f'<div class="cal-subtitle">{employee_name}</div></div>'
        f'<div class="status-summary"><div class="status-label">Expectation Fit</div>'
        f'<div class="status-value">{overall_score_text}</div>'
        f'<div class="status-chip {overall_class}">{overall_label}</div>'
        f'<div class="status-note">{status_note}</div></div></div>'
    )
    html += '<div class="insight-row">'
    html += (
        f'<div class="insight-card {attendance_class}"><div class="insight-label">Attendance</div>'
        f'<div class="insight-value">{attendance_value}</div>'
        f'<div class="insight-sub">{actual_days} of {expected_days} expected days</div>'
        f'<div class="insight-foot">{missed_days} missed day(s)</div></div>'
    )
    html += (
        f'<div class="insight-card {punctuality_class}"><div class="insight-label">Punctuality</div>'
        f'<div class="insight-value">{punctuality_value}</div>'
        f'<div class="insight-sub">On time {on_time_days} of {expected_days} days</div>'
        f'<div class="insight-foot">Late {late_arrivals} | Early {early_departures}</div></div>'
    )
    html += (
        f'<div class="insight-card {hours_class}"><div class="insight-label">Hours Balance</div>'
        f'<div class="insight-value">{hours_balance_text}</div>'
        f'<div class="insight-sub">{hours_balance_note}</div>'
        f'<div class="insight-foot">Gap vs expectation</div></div>'
    )
    html += (
        f'<div class="insight-card {exceptions_class}"><div class="insight-label">Exceptions</div>'
        f'<div class="insight-value">{missed_days} missed</div>'
        f'<div class="insight-sub">Off-day work {worked_non_working_days}</div>'
        f'<div class="insight-foot">Anomaly days {anomaly_days}</div></div>'
    )
    html += '</div>'
    html += '<table class="cal-table"><thead><tr>'
    html += '<th>Mon</th><th>Tue</th><th>Wed</th><th>Thu</th><th>Fri</th>'
    html += '</tr></thead><tbody>'
    
    # Color mapping
    colors = {
        'full': '#e9f7ee',
        'half': '#fff7dd',
        'short': '#fff0e1',
        'absent': '#ffe7e7',
        'anomaly': '#f4eeff',
        'holiday': '#e8f1ff',
        'weekoff': '#edf0f3',
        'special': '#e6f6f4'
    }
    pill_colors = {
        'full': '#2f9e44',
        'half': '#caa531',
        'short': '#cf6d21',
        'absent': '#b73b3b',
        'anomaly': '#5a3fa0',
        'holiday': '#2E86AB',
        'weekoff': '#7a8794',
        'special': '#1f7a6b'
    }
    text_colors = {
        'holiday': '#1f5f7a',
        'weekoff': '#4b5b66',
        'special': '#145b4f'
    }
    
    status_labels = {
        'full': 'Full Day',
        'half': 'Half Day',
        'short': 'Short Day',
        'absent': 'Absent',
        'anomaly': 'Anomaly',
        'holiday': 'Holiday',
        'weekoff': 'Week Off',
        'special': 'Special Day'
    }
    
    non_working_border = '#607d8b'
    holiday_border = '#2E86AB'
    
    for week in weekday_month_days:
        html += "<tr>"
        for day in week:
            if day == 0:
                html += '<td style="padding: 15px; border: 1px solid #ddd; background-color: #f5f5f5;"></td>'
                continue

            # Determine weekday (0=Mon, 6=Sun)
            current_date = datetime(year, month, day)
            weekday = current_date.weekday()
            is_weekend = weekday >= 5
            holiday_name = holiday_map.get(current_date.date())
            is_holiday = (holiday_name is not None) and not is_weekend
            is_expected_workday = (weekday in expected_workdays) and not is_weekend and not is_holiday

            day_info = date_status.get(day)
            expected_hours_day = get_expected_daily_hours(weekday, early_departure_override) if is_expected_workday else 0.0
            special_day = special_day_map.get(current_date.date())
            special_label = special_day.get('type') if special_day else ''
            special_reason = special_day.get('reason') if special_day else ''
            
            if day_info is not None:
                status = 'absent'  # Default
                shift_type = day_info.get('Shift Type', 'Absent')
                if pd.isna(shift_type):
                    shift_type = 'Absent'
                
                if day_info.get('Has Anomaly', False):
                    status = 'anomaly'
                elif shift_type == 'Full Day':
                    status = 'full'
                elif shift_type == 'Half Day':
                    status = 'half'
                elif shift_type == 'Short Shift':
                    status = 'short'
                elif day_info.get('Working Hours', 0) > 0:
                    status = 'short'
                
                worked_on_non_working = not is_expected_workday
                hours = float(day_info.get('Working Hours', 0.0) or 0.0)
                meal_hours = float(day_info.get('Meal Hours', 0.0) or 0.0)
                overtime_hours_basis = float(day_info.get('Overtime Eligible Hours', hours) or 0.0)
                meal_minutes = meal_hours * 60.0
                is_friday = weekday == 4
                meal_risk = None
                if hours >= Config.MEAL_RISK_LONG_DAY_HOURS and meal_minutes <= Config.MEAL_RISK_CRITICAL_MINUTES:
                    meal_risk = 'critical'
                elif (
                    (not is_friday) and
                    (hours >= Config.MEAL_RISK_LONG_DAY_HOURS) and
                    (meal_minutes < Config.MEAL_RISK_WARNING_MINUTES)
                ):
                    meal_risk = 'warning'
                if meal_risk == 'critical':
                    meal_class = 'meal-risk-critical'
                elif meal_risk == 'warning':
                    meal_class = 'meal-risk-warning'
                else:
                    meal_class = ''
                if weekday == 4:
                    daily_overtime = max(0.0, overtime_hours_basis - 5.0)
                else:
                    daily_overtime = max(0.0, overtime_hours_basis - 8.75)
                bg_color = colors.get(status, '#ffffff')
                pill_color = pill_colors.get(status, '#2E86AB')
                text_color = text_colors.get(status, '#1a1a1a')
                label = status_labels.get(status, '')
                if is_holiday:
                    label = status_labels['holiday']
                    pill_color = pill_colors['holiday']

                # Tooltip info
                info = f"Status: {shift_type} | Hours: {hours:.1f}h"
                if special_day:
                    if special_label == 'Full Off':
                        special_text = "Annotation: Full Off"
                    else:
                        special_text = f"Special Day: {special_label or 'Special Day'}"
                    if special_reason:
                        special_text += f" ({special_reason})"
                    info = f"{special_text} | {info}"
                if is_holiday and holiday_name:
                    info = f"Holiday: {holiday_name} | {info}"
                info += f" | Meal: {meal_hours:.1f}h"
                if meal_risk == 'critical':
                    info += " | Meal Risk: Critical (no meal)"
                elif meal_risk == 'warning':
                    info += " | Meal Risk: Warning (<30 min)"
                if expected_hours_day > 0:
                    info += f" | Expected: {expected_hours_day:.1f}h"
                if worked_on_non_working:
                    info = f"Worked on Non-Working Day | {info}"
                if pd.notna(day_info.get('First Punch In')):
                    info += f" | In: {day_info['First Punch In'].strftime('%H:%M')}"
                if pd.notna(day_info.get('Last Punch Out')):
                    info += f" | Out: {day_info['Last Punch Out'].strftime('%H:%M')}"
                if day_info.get('Is Late', False):
                    info += " | Late"
                if day_info.get('Is Very Late', False):
                    info += " (Very Late)"

                is_early_departure = day_info.get('Is Early Departure', False)
                if early_departure_override and pd.notna(day_info.get('Last Punch Out')):
                    is_early_departure = day_info['Last Punch Out'].time() < early_departure_override
                if is_early_departure:
                    info += " | Early Departure"
                    if early_departure_override:
                        info += f" ({early_departure_override.strftime('%H:%M')})"
                if day_info.get('Missing Punch Out', False):
                    info += " | Missing Punch Out"

                info_escaped = info.replace('"', '&quot;')
                cell_style = (
                    f"padding: 8px; border: 1px solid #d7dee7; border-radius: 10px; "
                    f"background-color: {bg_color}; color: {text_color}; min-height: 110px; "
                    "vertical-align: top; position: relative;"
                )
                if worked_on_non_working:
                    border_color = holiday_border if is_holiday else non_working_border
                    cell_style += f" box-shadow: inset 0 0 0 2px {border_color};"

                badges = []
                if day_info.get('Is Very Late', False):
                    badges.append('<span class="badge badge-vlate" title="Very Late">VL</span>')
                elif day_info.get('Is Late', False):
                    badges.append('<span class="badge badge-late" title="Late">L</span>')
                if is_early_departure:
                    badges.append('<span class="badge badge-early" title="Early Departure">E</span>')
                if day_info.get('Missing Punch Out', False):
                    badges.append('<span class="badge badge-miss" title="Missing Punch Out">M</span>')
                if day_info.get('Has Anomaly', False):
                    badges.append('<span class="badge badge-anom" title="Anomaly">A</span>')
                if special_day and special_label != 'Full Off':
                    special_badge_text = special_label or 'Special'
                    special_title = special_badge_text
                    if special_reason:
                        special_title += f" - {special_reason}"
                    special_title = special_title.replace('"', '&quot;')
                    badges.append(
                        f'<span class="badge badge-special" title="{special_title}">{special_badge_text}</span>'
                    )

                is_late_arrival = bool(day_info.get('Is Late', False))
                in_time = day_info['First Punch In'].strftime('%H:%M') if pd.notna(day_info.get('First Punch In')) else '--'
                out_time = day_info['Last Punch Out'].strftime('%H:%M') if pd.notna(day_info.get('Last Punch Out')) else '--'
                time_chunks = []
                if in_time != '--':
                    in_time_class = "time-value alert" if is_late_arrival else "time-value"
                    time_chunks.append(
                        f'<span class="time-chunk"><span class="time-label">In:</span> '
                        f'<span class="{in_time_class}">{in_time}</span></span>'
                    )
                if out_time != '--':
                    out_time_class = "time-value alert" if is_early_departure else "time-value"
                    time_chunks.append(
                        f'<span class="time-chunk"><span class="time-label">Out:</span> '
                        f'<span class="{out_time_class}">{out_time}</span></span>'
                    )
                time_range = ''.join(time_chunks)

                if expected_hours_day > 0:
                    ratio = max(0.0, min(hours / expected_hours_day, 1.0))
                    if ratio >= 0.95:
                        bar_class = 'good'
                    elif ratio >= 0.7:
                        bar_class = 'warn'
                    else:
                        bar_class = 'bad'
                else:
                    ratio = 1.0 if hours > 0 else 0.0
                    bar_class = 'off' if hours > 0 else 'zero'

                html += f'<td class="cal-cell" style="{cell_style}" title="{info_escaped}">'
                html += '<div class="cell-body">'
                html += '<div class="day-top">'
                html += f'<div class="day-num">{day}</div>'
                html += f'<span class="status-pill" style="background-color: {pill_color};">{label}</span>'
                html += '</div>'
                if worked_on_non_working:
                    html += '<div class="off-tag">OFF</div>'
                html += '<div class="badge-row">'
                html += f'<span class="hours-pill">{hours:.1f}h</span>'
                if daily_overtime > 0:
                    html += f'<span class="ot-pill">OT: +{daily_overtime:.1f}h</span>'
                if badges:
                    html += ''.join(badges)
                html += '</div>'
                html += f'<div class="time-range">{time_range or "&nbsp;"}</div>'
                html += f'<div class="meal-text {meal_class}">Meal: {meal_hours:.1f}h</div>'
                html += '<div class="cell-bar">'
                if is_expected_workday or hours > 0:
                    html += (
                        f'<div class="hours-track"><span class="hours-fill {bar_class}" '
                        f'style="width: {ratio * 100:.0f}%;"></span></div>'
                    )
                else:
                    html += '<div class="hours-track placeholder"><span class="hours-fill zero" style="width: 0%;"></span></div>'
                html += '</div>'
                html += '</div>'
                html += '</td>'
            else:
                if special_day and special_label != 'Full Off':
                    bg_color = colors['special']
                    pill_color = pill_colors['special']
                    cell_style = (
                        f"padding: 8px; border: 1px solid #d7dee7; border-radius: 10px; "
                        f"background-color: {bg_color}; color: {text_colors.get('special', '#1a1a1a')}; min-height: 110px; "
                        "vertical-align: top; position: relative;"
                    )
                    title_text = f"Special Day: {special_label or 'Special Day'}"
                    if special_reason:
                        title_text += f" | {special_reason}"
                    if is_holiday and holiday_name:
                        title_text = f"Holiday: {holiday_name} | {title_text}"
                    html += f'<td class="cal-cell" style="{cell_style}" title="{title_text}">'
                    html += '<div class="cell-body">'
                    html += '<div class="day-top">'
                    html += f'<div class="day-num">{day}</div>'
                    html += f'<span class="status-pill" style="background-color: {pill_color};">{status_labels["special"]}</span>'
                    html += '</div>'
                    html += '<div class="badge-row">'
                    html += '<span class="hours-pill">0.0h</span>'
                    html += f'<span class="badge badge-special">{special_label or "Special"}</span>'
                    html += '</div>'
                    html += '<div class="time-range">&nbsp;</div>'
                    html += '<div class="cell-bar">'
                    html += '<div class="hours-track placeholder"><span class="hours-fill zero" style="width: 0%;"></span></div>'
                    html += '</div>'
                    html += '</div>'
                    html += '</td>'
                elif is_holiday:
                    # Holiday (no punches)
                    bg_color = colors['holiday']
                    pill_color = pill_colors['holiday']
                    cell_style = (
                        f"padding: 8px; border: 1px solid #d7dee7; border-radius: 10px; "
                        f"background-color: {bg_color}; color: {text_colors.get('holiday', '#1a1a1a')}; min-height: 110px; "
                        "vertical-align: top; position: relative;"
                    )
                    title_text = f'Holiday: {holiday_name}' if holiday_name else 'Holiday'
                    html += f'<td class="cal-cell" style="{cell_style}" title="{title_text}">'
                    html += '<div class="cell-body">'
                    html += '<div class="day-top">'
                    html += f'<div class="day-num">{day}</div>'
                    html += f'<span class="status-pill" style="background-color: {pill_color};">{status_labels["holiday"]}</span>'
                    html += '</div>'
                    html += '<div class="badge-row">'
                    html += '<span class="hours-pill">0.0h</span>'
                    html += '</div>'
                    html += '<div class="time-range">&nbsp;</div>'
                    html += '<div class="cell-bar">'
                    html += '<div class="hours-track placeholder"><span class="hours-fill zero" style="width: 0%;"></span></div>'
                    html += '</div>'
                    html += '</div>'
                    html += '</td>'
                elif is_expected_workday:
                    # Expected workday with no punches (absent)
                    bg_color = colors['absent']
                    pill_color = pill_colors['absent']
                    cell_style = (
                        f"padding: 8px; border: 1px solid #d7dee7; border-radius: 10px; "
                        f"background-color: {bg_color}; color: #1a1a1a; min-height: 110px; "
                        "vertical-align: top; position: relative;"
                    )
                    html += f'<td class="cal-cell" style="{cell_style}" title="Absent">'
                    html += '<div class="cell-body">'
                    html += '<div class="day-top">'
                    html += f'<div class="day-num">{day}</div>'
                    html += f'<span class="status-pill" style="background-color: {pill_color};">{status_labels["absent"]}</span>'
                    html += '</div>'
                    html += '<div class="badge-row">'
                    html += '<span class="hours-pill">0.0h</span>'
                    html += '</div>'
                    html += '<div class="time-range">&nbsp;</div>'
                    html += '<div class="cell-bar">'
                    html += '<div class="hours-track"><span class="hours-fill zero" style="width: 0%;"></span></div>'
                    html += '</div>'
                    html += '</div>'
                    html += '</td>'
                else:
                    # Non-working day (week off)
                    bg_color = colors['weekoff']
                    pill_color = pill_colors['weekoff']
                    cell_style = (
                        f"padding: 8px; border: 1px solid #d7dee7; border-radius: 10px; "
                        f"background-color: {bg_color}; color: #4b5b66; min-height: 110px; "
                        "vertical-align: top; position: relative;"
                    )
                    html += f'<td class="cal-cell" style="{cell_style}">'
                    html += '<div class="cell-body">'
                    html += '<div class="day-top">'
                    html += f'<div class="day-num">{day}</div>'
                    html += f'<span class="status-pill" style="background-color: {pill_color};">{status_labels["weekoff"]}</span>'
                    html += '</div>'
                    html += '<div class="badge-row"></div>'
                    html += '<div class="time-range">&nbsp;</div>'
                    html += '<div class="cell-bar">'
                    html += '<div class="hours-track placeholder"><span class="hours-fill zero" style="width: 0%;"></span></div>'
                    html += '</div>'
                    html += '</div>'
                    html += '</td>'

        html += "</tr>"
    
    html += '</tbody></table>'
    html += '<div class="legend">'
    html += '<div>'
    html += '<div class="legend-title">Status</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["full"]};border-color:#bfe4c9;"></span> Full Day</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["half"]};border-color:#f3dd97;"></span> Half Day</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["short"]};border-color:#f6caa1;"></span> Short Day</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["absent"]};border-color:#f1b5b5;"></span> Absent</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["holiday"]};border-color:#c6d7f2;"></span> Holiday</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["special"]};border-color:#b7e0d8;"></span> Special Day</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["anomaly"]};border-color:#cbb6f5;"></span> Anomaly</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="background:{colors["weekoff"]};border-color:#dbe1e8;"></span> Week Off</div>'
    html += '</div>'
    html += '<div>'
    html += '<div class="legend-title">Signals</div>'
    html += '<div class="legend-item"><span class="badge badge-late">L</span> Late</div>'
    html += '<div class="legend-item"><span class="badge badge-vlate">VL</span> Very Late</div>'
    html += '<div class="legend-item"><span class="badge badge-early">E</span> Early Departure</div>'
    html += '<div class="legend-item"><span class="badge badge-miss">M</span> Missing Punch Out</div>'
    html += '<div class="legend-item"><span class="badge badge-anom">A</span> Anomaly Flag</div>'
    html += '<div class="legend-item"><span class="badge badge-special">Special</span> Operational Note</div>'
    html += f'<div class="legend-item"><span class="legend-swatch" style="border:2px solid {non_working_border};background:#fff;"></span> Worked on Off-Day</div>'
    html += '<div class="legend-item"><span class="legend-bar"></span> Hours vs Expected</div>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    
    return html

# ============================================================================
# STREAMLIT APPLICATION
# ============================================================================

def main():
    # Page configuration
    st.set_page_config(
        page_title="HR Attendance Analytics",
        page_icon="HR",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    _ensure_cache_version()

    # ------------------------------------------------------------------------
    # AUTHENTICATION GATE
    # ------------------------------------------------------------------------
    if "auth_authenticated" not in st.session_state:
        st.session_state.auth_authenticated = False

    if not st.session_state.auth_authenticated:
        st.title("Login")
        st.markdown("Please sign in to access the dashboard.")
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")

        if submitted:
            ok, message, user = authenticate_user(username, password)
            if ok:
                role = (user or {}).get("role") or "employee"
                user_id = (user or {}).get("id")
                if role == "admin":
                    allowed = "ALL"
                else:
                    if not user_id:
                        st.error("User record incomplete. Contact admin.")
                        st.stop()
                    allowed = get_allowed_employees(user_id)
                    if not allowed:
                        st.error("No employee access assigned. Contact admin.")
                        st.stop()

                st.session_state.auth_authenticated = True
                st.session_state.auth_user = (user or {}).get("username", username)
                st.session_state.auth_user_id = user_id
                st.session_state.auth_role = role
                st.session_state.allowed_employees = allowed
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
            else:
                st.error(message or "Invalid credentials.")

        st.stop()

    with st.sidebar:
        st.markdown("---")
        auth_user = st.session_state.get("auth_user", "")
        auth_role = st.session_state.get("auth_role", "employee")
        st.caption(f"Signed in as {auth_user} ({auth_role})")
        with st.expander("Account Settings", expanded=False):
            render_account_security_panel()
        if st.button("Logout"):
            for key in ("auth_authenticated", "auth_user", "auth_user_id", "auth_role", "allowed_employees"):
                st.session_state.pop(key, None)
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()

    if st.session_state.get("auth_role") == "admin":
        with st.sidebar.expander("Admin Panel", expanded=False):
            render_admin_panel()

    
    # Custom CSS - Professional HR Dashboard Styling
    st.markdown("""
        <style>
        .main > div {padding-top: 2rem;}
        
        /* KPI Metric Cards */
        .stMetric {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
            transition: box-shadow 0.3s ease;
        }
        .stMetric:hover {
            box-shadow: 0 4px 8px rgba(0,0,0,0.12);
        }
        .stMetric label {
            color: #2E86AB;
            font-weight: 600;
            font-size: 14px;
        }
        .stMetric [data-testid="stMetricValue"] {
            color: #1a1a1a;
            font-size: 32px;
            font-weight: 700;
        }
        .stMetric [data-testid="stMetricDelta"] {
            font-size: 14px;
        }
        
        /* Section Headers */
        h1, h2, h3 {
            color: #2E86AB;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 12px 24px;
            border-radius: 6px 6px 0 0;
        }
        
        /* DataFrames */
        .dataframe {
            border-radius: 6px;
            overflow: hidden;
        }
        
        /* Captions */
        .stCaption {
            color: #666;
            font-size: 13px;
            font-style: italic;
        }
        </style>
    """, unsafe_allow_html=True)
        
    # Header
    st.title("\U0001F4CA HR Attendance Analytics Dashboard")
    st.markdown("**Complete workforce attendance insights for data-driven HR decisions**")
    st.markdown("---")
    
    # FILE MANAGEMENT & PERSISTENCE
    st.sidebar.markdown("---")
    st.sidebar.subheader("\U0001F4CA Data Management")
    is_admin_session = (st.session_state.get("auth_role") or "").strip().lower() == "admin"
    if is_admin_session:
        if st.sidebar.button("RESET DATA", key="reset_data_btn", use_container_width=True):
            reset_ok, reset_msg = _reset_dashboard_data_state()
            if reset_ok:
                st.sidebar.success(reset_msg)
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
            else:
                st.sidebar.error(reset_msg)
    else:
        st.sidebar.button(
            "RESET DATA (Admin Only)",
            key="reset_data_btn_disabled",
            use_container_width=True,
            disabled=True,
        )
        st.sidebar.caption("Only admin accounts can reset dashboard data.")

    uploaded_file = st.sidebar.file_uploader("Update Data Source", type=['xlsx', 'xls'], key="data_upload_source")
    
    data_source = None
    
    # 1. Handle new file upload (Append/Refresh)
    if uploaded_file is not None:
        with st.spinner("Merging and updating data..."):
            merge_ok = DataManager.merge_and_save(uploaded_file, Config.DATA_FILE_PATH)
        if merge_ok:
            st.sidebar.success("\u2705 Data updated successfully!")
            _clear_all_caches()
            data_source = Config.DATA_FILE_PATH
        else:
            st.stop()
    # 2. Check for existing persistent file
    elif os.path.exists(Config.DATA_FILE_PATH):
        data_source = Config.DATA_FILE_PATH
    
    if data_source is None:
        st.info("Welcome! Please upload an Excel file in the sidebar to initialize the dashboard.")
        st.stop()
    
    # Load data (cache-aware, defensive against corrupted cache payloads)
    source_signature = _get_data_source_signature(data_source)
    try:
        with st.spinner("Loading and processing attendance data..."):
            raw_df, daily_df, emp_metrics_df, weekly_overtime_df, monthly_overtime_df = load_and_process_data(
                data_source,
                source_signature,
                Config.CACHE_VERSION
            )
        _validate_processed_frames(
            raw_df,
            daily_df,
            emp_metrics_df,
            weekly_overtime_df,
            monthly_overtime_df
        )
        # Apply employee-level access filtering (non-admin users only)
        allowed_employees = st.session_state.get("allowed_employees", "ALL")
        if allowed_employees != "ALL":
            raw_df = _filter_df_by_employees(raw_df, allowed_employees)
            daily_df = _filter_df_by_employees(daily_df, allowed_employees)
            emp_metrics_df = _filter_df_by_employees(emp_metrics_df, allowed_employees)
            weekly_overtime_df = _filter_df_by_employees(weekly_overtime_df, allowed_employees)
            monthly_overtime_df = _filter_df_by_employees(monthly_overtime_df, allowed_employees)
    except Exception as e:
        message = str(e)
        if _is_cache_corruption_error(message):
            _clear_all_caches()
            backup_restored = _restore_data_file_from_backup(data_source)
            if backup_restored:
                source_signature = _get_data_source_signature(data_source)
            try:
                with st.spinner("Rebuilding cached data..."):
                    raw_df, daily_df, emp_metrics_df, weekly_overtime_df, monthly_overtime_df = load_and_process_data(
                        data_source,
                        source_signature,
                        Config.CACHE_VERSION
                    )
                _validate_processed_frames(
                    raw_df,
                    daily_df,
                    emp_metrics_df,
                    weekly_overtime_df,
                    monthly_overtime_df
                )
                # Apply employee-level access filtering (non-admin users only)
                allowed_employees = st.session_state.get("allowed_employees", "ALL")
                if allowed_employees != "ALL":
                    raw_df = _filter_df_by_employees(raw_df, allowed_employees)
                    daily_df = _filter_df_by_employees(daily_df, allowed_employees)
                    emp_metrics_df = _filter_df_by_employees(emp_metrics_df, allowed_employees)
                    weekly_overtime_df = _filter_df_by_employees(weekly_overtime_df, allowed_employees)
                    monthly_overtime_df = _filter_df_by_employees(monthly_overtime_df, allowed_employees)
            except Exception as retry_error:
                retry_text = str(retry_error)
                if _is_cache_corruption_error(retry_text):
                    st.error(
                        "Error loading data: Data file appears corrupted. "
                        "Please upload a fresh Excel file from the Data Management panel."
                    )
                else:
                    st.error(f"Error loading data: {retry_text}")
                st.stop()
        else:
            st.error(f"Error loading data: {message}")
            st.stop()

    # Apply persisted annotation overrides after base processing.
    annotation_items = get_annotation_items_from_db()
    daily_df = apply_annotation_overrides(daily_df, annotation_items)
    emp_metrics_df = FeatureEngineer.calculate_productivity_metrics(daily_df)
    weekly_overtime_df, monthly_overtime_df = FeatureEngineer.calculate_overtime_metrics(daily_df)
    
    # ========================================================================
    # SIDEBAR CONTROLS
    # ========================================================================
    
    st.sidebar.header("\U0001F3AF Filters & Controls")
    
    # Date range filter
    min_date = daily_df['Date'].min()
    max_date = daily_df['Date'].max()
    min_date_value = min_date.date() if hasattr(min_date, "date") else min_date
    max_date_value = max_date.date() if hasattr(max_date, "date") else max_date

    date_col1, date_col2 = st.sidebar.columns(2)
    with date_col1:
        start_date = st.date_input(
            "Start Date",
            value=min_date_value,
            min_value=min_date_value,
            max_value=max_date_value,
            key="start_date"
        )
    with date_col2:
        end_date = st.date_input(
            "End Date",
            value=max_date_value,
            min_value=start_date,
            max_value=max_date_value,
            key="end_date"
        )

    if start_date > end_date:
        st.sidebar.error("Start Date must be on or before End Date.")
        daily_filtered = daily_df.copy()
    else:
        daily_filtered = daily_df.loc[
            (daily_df['Date'].dt.date >= start_date) &
            (daily_df['Date'].dt.date <= end_date)
        ].copy()
    
    # Department filter (multi-select)
    selected_depts = []
    if 'Department' in daily_df.columns:
        dept_options = sorted(daily_filtered['Department'].dropna().unique().tolist())
        selected_depts = st.sidebar.multiselect("Select Department", dept_options)
        
        if selected_depts:
            daily_filtered = daily_filtered[daily_filtered['Department'].isin(selected_depts)].copy()
    
    # Employee filter (multi-select)
    employee_options = sorted(daily_filtered['Employee Full Name'].unique().tolist())
    selected_employees = st.sidebar.multiselect("Select Employee", employee_options)
    
    if selected_employees:
        daily_filtered = daily_filtered[daily_filtered['Employee Full Name'].isin(selected_employees)].copy()
    st.sidebar.markdown("---")
    st.sidebar.subheader("\U0001F4CA View Options")
    
    # Toggle options
    exclude_duplicates = st.sidebar.checkbox("Exclude Duplicate Punches", value=True)
    show_anomalies_only = st.sidebar.checkbox("Show Anomalies Only", value=False)
    show_late_only = st.sidebar.checkbox("Late Arrivals Only", value=False)
    show_early_only = st.sidebar.checkbox("Early Departures Only", value=False)
    
    # Apply filters
    view_df = daily_filtered.copy()
    
    if show_anomalies_only:
        view_df = view_df[view_df['Has Anomaly']]
    
    if show_late_only:
        view_df = view_df[view_df['Is Late']]
    
    if show_early_only:
        view_df = view_df[view_df['Is Early Departure']]
    # Debug info
    st.sidebar.markdown("---")
    st.sidebar.subheader("\U0001F4CA Filter Debug")
    st.sidebar.write(f"Records after filters: {len(view_df)}")
    st.sidebar.write(f"Date range in data: {daily_df['Date'].min()} to {daily_df['Date'].max()}")
    
    # ========================================================================
    # KPI SUMMARY CARDS
    # ========================================================================
    
    st.header("\U0001F4CA Key Performance Indicators")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        total_employees = view_df['Employee Number'].nunique()
        create_metric_card("\U0001F4CA Total Employees", total_employees)
    
    with col2:
        total_days = count_working_days(start_date, end_date)
        create_metric_card("\U0001F5D3 Working Days", total_days)
    
    with col3:
        avg_hours = view_df['Working Hours'].mean()
        create_metric_card("\u23F1 Avg Daily Hours", f"{avg_hours:.2f}h")
    
    with col4:
        late_pct = (view_df['Is Late'].sum() / len(view_df) * 100) if len(view_df) > 0 else 0
        create_metric_card("\u26A0 Late Arrivals", f"{late_pct:.1f}%")
    
    with col5:
        early_pct = (view_df['Is Early Departure'].sum() / len(view_df) * 100) if len(view_df) > 0 else 0
        create_metric_card("\u26A0 Early Departures", f"{early_pct:.1f}%")
    
    with col6:
        anomaly_count = view_df['Has Anomaly'].sum()
        create_metric_card("\u26A0 Anomalies", anomaly_count)
    
    st.markdown("---")
    
    # ========================================================================
    # TABBED DASHBOARDS
    # ========================================================================
    
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "\U0001F3AF Productivity",
        "\U0001F4C8 Overtime Analysis",
        "\U0001F4C8 Monthly Performance",
        "\U0001F4CA Month-to-Month Comparison",
        "\U0001F5D3 Work Pattern Calendar",
        "\U0001F5D3 Week-Wise Comparison",
        "\U0001F37D Lunch Risk",
        "\u26A0 Anomalies",
        "\U0001F4CA Data Table",
    ])
    # ------------------------------------------------------------------------
    # TAB 1: PRODUCTIVITY DASHBOARD
    # ------------------------------------------------------------------------
    with tab1:
        st.subheader("\U0001F3AF Productivity Dashboard")
        
        # Recalculate metrics based on filtered data (Date, Employee, Dept)
        # This ensures the Productivity tab respects the Date Range filter
        emp_metrics_filtered = get_productivity_metrics(daily_filtered)
        # Day of week analysis
        dow_summary = get_dow_summary(view_df)
        fig = px.bar(
            x=dow_summary.index,
            y=dow_summary.values,
            title='Average Working Hours by Day of Week',
            labels={'x': 'Day', 'y': 'Avg Hours'},
            color=dow_summary.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Shows average working hours across different days of the week")        
        st.plotly_chart(
            plot_employee_ranking(emp_metrics_filtered, 'Total Hours', 10),
            use_container_width=True
        )
        st.caption("Top 10 employees ranked by total working hours")
        
        # Employee performance table
        st.subheader("Employee Performance Summary")

        display_cols = [
            'Employee Full Name', 'Department', 'Total Hours', 'Avg Daily Hours',
            'Total Days', 'Late Count', 'Early Departure Count'
        ]

        if len(emp_metrics_filtered) > 0:
            st.dataframe(
                emp_metrics_filtered[display_cols].sort_values('Total Hours', ascending=False),
                use_container_width=True,
                height=400
            )
        else:
            st.info("No employee data available for the selected filters.")

    # ------------------------------------------------------------------------
    # TAB 2: OVERTIME ANALYSIS
    # ------------------------------------------------------------------------
    with tab2:
        st.subheader("\U0001F4C8 Overtime Analysis")
        st.markdown("**Weekly, 15-day, and monthly overtime hours**")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Weekly Overtime")
            
            if not weekly_overtime_df.empty:
                # Filter controls
                years = sorted(weekly_overtime_df['Year'].unique(), reverse=True)
                selected_year_w = st.selectbox("Select Year", years, key="ot_w_year")
                
                available_months_w = sorted(
                    daily_df[daily_df['Date'].dt.year == selected_year_w]['Date'].dt.month.unique(),
                    reverse=True
                )
                
                if not available_months_w:
                    st.info(f"No monthly data available for {selected_year_w}.")
                    selected_week = None
                else:
                    selected_month_w = st.selectbox(
                        "Select Month",
                        available_months_w,
                        format_func=lambda x: calendar.month_name[x],
                        key="ot_w_month"
                    )
                    
                    weeks_in_month = sorted(
                        daily_df[
                            (daily_df['Date'].dt.year == selected_year_w) &
                            (daily_df['Date'].dt.month == selected_month_w)
                        ]['Date'].dt.isocalendar().week.unique(),
                        reverse=True
                    )
                    if not weeks_in_month:
                        st.info(f"No weeks available for {calendar.month_name[selected_month_w]} {selected_year_w}.")
                        selected_week = None
                    else:
                        selected_week = st.selectbox("Select Week Number", weeks_in_month, key="ot_w_num")
                
                # Apply filter
                if selected_week is None:
                    filtered_weekly = weekly_overtime_df.iloc[0:0]
                else:
                    filtered_weekly = weekly_overtime_df[
                        (weekly_overtime_df['Year'] == selected_year_w) & 
                        (weekly_overtime_df['Week'] == selected_week)
                    ]
                
                weekly_chart = plot_overtime_charts(filtered_weekly, 'weekly')
                if weekly_chart:
                    st.plotly_chart(weekly_chart, use_container_width=True)
                else:
                    st.info(f"No overtime recorded for Week {selected_week}, {selected_year_w}.")
            else:
                st.info("No weekly overtime data available.")

        with col2:
            st.markdown("### Monthly Overtime")
            
            if not monthly_overtime_df.empty:
                # Filter controls
                years_m = sorted(monthly_overtime_df['Year'].unique(), reverse=True)
                selected_year_m = st.selectbox("Select Year", years_m, key="ot_m_year")
                
                available_months = sorted(monthly_overtime_df[monthly_overtime_df['Year'] == selected_year_m]['Month'].unique(), reverse=True)
                selected_month = st.selectbox("Select Month", available_months, format_func=lambda x: calendar.month_name[x], key="ot_m_num")
                
                # Apply filter
                filtered_monthly = monthly_overtime_df[
                    (monthly_overtime_df['Year'] == selected_year_m) & 
                    (monthly_overtime_df['Month'] == selected_month)
                ]
                
                monthly_chart = plot_overtime_charts(filtered_monthly, 'monthly')
                if monthly_chart:
                    st.plotly_chart(monthly_chart, use_container_width=True)
                else:
                    st.info(f"No overtime recorded for {calendar.month_name[selected_month]} {selected_year_m}.")
            else:
                st.info("No monthly overtime data available.")

        st.markdown("---")
        st.markdown("### 15-Day Overtime")
        st.markdown("**First half and second half of the selected month**")

        if daily_filtered.empty:
            st.info("No data available for 15-day overtime analysis.")
        else:
            years_15 = sorted(daily_filtered['Date'].dt.year.unique(), reverse=True)
            selected_year_15 = st.selectbox("Select Year", years_15, key="ot_15_year")

            available_months_15 = sorted(
                daily_filtered[daily_filtered['Date'].dt.year == selected_year_15]['Date'].dt.month.unique(),
                reverse=True
            )
            if not available_months_15:
                st.info(f"No monthly data available for {selected_year_15}.")
            else:
                selected_month_15 = st.selectbox(
                    "Select Month",
                    available_months_15,
                    format_func=lambda x: calendar.month_name[x],
                    key="ot_15_month"
                )

                fifteen_df = calculate_15_day_overtime(
                    daily_filtered,
                    selected_year_15,
                    selected_month_15,
                    start_date,
                    end_date
                )

                if fifteen_df.empty:
                    st.info(f"No 15-day overtime data available for {calendar.month_name[selected_month_15]} {selected_year_15}.")
                else:
                    last_day = calendar.monthrange(selected_year_15, selected_month_15)[1]
                    span_labels = ["Days 1-15", f"Days 16-{last_day}"]
                    col_a, col_b = st.columns(2)
                    for col, span_label in zip([col_a, col_b], span_labels):
                        with col:
                            st.markdown(f"#### {span_label}")
                            span_df = fifteen_df[fifteen_df['Span'] == span_label]
                            span_chart = plot_overtime_charts(span_df, '15-day')
                            if span_chart:
                                st.plotly_chart(span_chart, use_container_width=True)
                            else:
                                st.info(f"No overtime recorded for {span_label}.")

        st.markdown("---")
        st.markdown("### Employee Monthly Overtime Trend")
        if not monthly_overtime_df.empty:
            employee_ot_options = sorted(monthly_overtime_df['Employee Full Name'].unique().tolist())
            selected_emp_ot = st.selectbox("Select Employee", employee_ot_options, key="ot_emp_monthly")
            emp_monthly_ot = monthly_overtime_df[
                monthly_overtime_df['Employee Full Name'] == selected_emp_ot
            ].copy()
            if emp_monthly_ot.empty:
                st.info(f"No monthly overtime data available for {selected_emp_ot}.")
            else:
                emp_monthly_ot['Month Start'] = pd.to_datetime(
                    dict(year=emp_monthly_ot['Year'], month=emp_monthly_ot['Month'], day=1)
                )
                emp_monthly_ot = emp_monthly_ot.sort_values('Month Start')
                fig = px.line(
                    emp_monthly_ot,
                    x='Month Start',
                    y='Monthly Overtime',
                    markers=True,
                    title=f"Monthly Overtime Trend: {selected_emp_ot}",
                    labels={'Month Start': 'Month', 'Monthly Overtime': 'Overtime Hours'}
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Month-by-month overtime hours for the selected employee")
        else:
            st.info("No monthly overtime data available for trend analysis.")
    # ------------------------------------------------------------------------
    # TAB 3: MONTHLY PERFORMANCE TRACKING
    # ------------------------------------------------------------------------
    with tab3:
        st.subheader("\U0001F4C8 Monthly Performance Tracking")
        st.markdown("**Long-term performance trends and month-over-month analytics**")
        
        # Calculate monthly metrics (respect sidebar filters)
        monthly_df = get_monthly_metrics_cached(daily_filtered)
        
        if len(monthly_df) == 0:
            st.info("No monthly data available for the selected filters.")
        else:
            # Employee selection for trend view
            st.markdown("### Employee Performance Trend")
            employee_options = sorted(daily_filtered['Employee Full Name'].unique().tolist())
            selected_emp_trend = st.selectbox("Select Employee for Trend Analysis", employee_options, key="trend_emp")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Monthly total hours trend
                fig_hours = plot_monthly_trend(monthly_df, selected_emp_trend, 'Total Hours')
                if fig_hours:
                    st.plotly_chart(fig_hours, use_container_width=True)
                    st.caption("Monthly total working hours trend")
            
            with col2:
                # Monthly attendance days trend
                fig_days = plot_monthly_trend(monthly_df, selected_emp_trend, 'Attendance Days')
                if fig_days:
                    st.plotly_chart(fig_days, use_container_width=True)
                    st.caption("Monthly attendance days count")
            
            st.markdown("---")
            st.markdown("### Monthly Employee Comparison")
            
            # Month selection for comparison
            available_months = sorted(monthly_df['YearMonth'].unique().tolist())
            selected_month = st.selectbox("Select Month for Comparison", available_months, key="comp_month")
            
            col3, col4 = st.columns(2)
            
            with col3:
                # Top employees by total hours
                fig_comp_hours = plot_monthly_comparison(monthly_df, selected_month, 'Total Hours', 10)
                if fig_comp_hours:
                    st.plotly_chart(fig_comp_hours, use_container_width=True)
                    st.caption(f"Top 10 employees by total hours - {selected_month}")
            
            with col4:
                # Top employees by average daily hours
                fig_comp_avg = plot_monthly_comparison(monthly_df, selected_month, 'Avg Daily Hours', 10)
                if fig_comp_avg:
                    st.plotly_chart(fig_comp_avg, use_container_width=True)
                    st.caption(f"Top 10 employees by average daily hours - {selected_month}")
            
            # Month-over-month trend indicators
            st.markdown("---")
            st.markdown("### Month-over-Month Change Indicators")
            
            # Calculate MoM changes
            recent_changes = get_recent_changes(monthly_df)
            
            if len(recent_changes) > 0:
                st.dataframe(
                    recent_changes[['Employee Full Name', 'YearMonth', 'Total Hours', 'Prev Total Hours', 
                                  'Hours Change', 'Hours Change %']].head(15),
                    use_container_width=True,
                    height=400
                )
                st.caption(f"Month-over-month changes comparing {available_months[-1]} to previous month")
            else:
                st.info("Insufficient data for month-over-month comparison")
            
            # Monthly summary table
            st.markdown("---")
            st.markdown("### Monthly Performance Summary")
            display_monthly_cols = ['Employee Full Name', 'YearMonth', 'Total Hours', 'Avg Daily Hours', 
                                   'Attendance Days', 'Late Count', 'Early Departure Count']
            st.dataframe(
                monthly_df[display_monthly_cols].sort_values(['YearMonth', 'Total Hours'], ascending=[True, False]),
                use_container_width=True,
                height=400
            )
    
    # ------------------------------------------------------------------------
    # TAB 4: MONTH-TO-MONTH COMPARISON
    # ------------------------------------------------------------------------
    with tab4:
        st.subheader("\U0001F4CA Employee Month-to-Month Comparison")
        st.markdown("**Compare employee performance across multiple months**")

        monthly_df_comp = get_monthly_metrics_cached(daily_filtered)
        employee_options = sorted(monthly_df_comp['Employee Full Name'].unique().tolist())

        if len(employee_options) == 0:
            st.warning("No employees available for the selected filters.")
        else:
            selected_emp_comp = st.selectbox("Select Employee", employee_options, key="comp_emp")

            # Get available months for this employee
            emp_months = sorted(
                monthly_df_comp[monthly_df_comp['Employee Full Name'] == selected_emp_comp]['YearMonth']
                .unique()
                .tolist()
            )

            if len(emp_months) < 2:
                st.warning(f"Insufficient data for {selected_emp_comp}. Need at least 2 months of data for comparison.")
            else:
                default_months = emp_months[-4:] if len(emp_months) >= 4 else emp_months
                selected_months = st.multiselect(
                    "Select Months for Comparison (2+)",
                    emp_months,
                    default=default_months,
                    key="comp_months"
                )

                if len(selected_months) < 2:
                    st.info("Select at least two months to compare.")
                else:
                    selected_months = [m for m in emp_months if m in selected_months]
                    emp_monthly = monthly_df_comp[
                        (monthly_df_comp['Employee Full Name'] == selected_emp_comp) &
                        (monthly_df_comp['YearMonth'].isin(selected_months))
                    ].set_index('YearMonth').reindex(selected_months).fillna(0)

                    st.markdown("---")
                    st.markdown(f"### Comparison: {', '.join(selected_months)}")

                    chart_metrics = [
                        ('Total Hours', 'Total Hours'),
                        ('Avg Daily Hours', 'Avg Daily Hours'),
                        ('Attendance Days', 'Attendance Days'),
                        ('Late Count', 'Late Arrivals'),
                        ('Early Departure Count', 'Early Departures')
                    ]

                    fig = go.Figure()
                    for month in selected_months:
                        fig.add_trace(go.Bar(
                            name=month,
                            x=[label for _, label in chart_metrics],
                            y=[emp_monthly.loc[month, metric] for metric, _ in chart_metrics]
                        ))

                    fig.update_layout(
                        title=f'Performance Comparison: {selected_emp_comp}',
                        xaxis_title='Metric',
                        yaxis_title='Value',
                        barmode='group',
                        height=480,
                        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption("Grouped comparison of key metrics across the selected months")

                    st.markdown("### Detailed Comparison")
                    table_metrics = [
                        ('Total Hours', 'Total Hours', 'h', 2),
                        ('Avg Daily Hours', 'Average Daily Hours', 'h', 2),
                        ('Attendance Days', 'Attendance Days', '', 0),
                        ('Late Count', 'Late Arrivals', '', 0),
                        ('Early Departure Count', 'Early Departures', '', 0),
                        ('Missing Punch Out Count', 'Missing Punch-Outs', '', 0)
                    ]

                    table_rows = []
                    for metric_col, metric_label, suffix, decimals in table_metrics:
                        row = {'Metric': metric_label}
                        for month in selected_months:
                            value = emp_monthly.loc[month, metric_col]
                            if decimals == 0:
                                display_value = f"{int(value)}{suffix}"
                            else:
                                display_value = f"{value:.{decimals}f}{suffix}"
                            row[month] = display_value
                        table_rows.append(row)

                    comp_table = pd.DataFrame(table_rows)
                    st.dataframe(comp_table, use_container_width=True, hide_index=True)
    
    # ------------------------------------------------------------------------
    # TAB 6: WEEK-WISE EMPLOYEE COMPARISON
    # ------------------------------------------------------------------------
    with tab6:
        st.subheader("\U0001F5D3 Week-Wise Employee Comparison")
        st.markdown("**Compare employees by weekly patterns instead of daily noise.**")

        week_source_df = daily_df.copy()
        week_source_df['Date'] = pd.to_datetime(week_source_df['Date'])
        if selected_depts and 'Department' in week_source_df.columns:
            week_source_df = week_source_df[week_source_df['Department'].isin(selected_depts)]
        if selected_employees:
            week_source_df = week_source_df[week_source_df['Employee Full Name'].isin(selected_employees)]

        if week_source_df.empty:
            st.warning("No data available for week-wise comparison under the current filters.")
        else:
            st.caption("This view uses month/year controls and summarizes attendance week-by-week.")

            available_years = sorted(week_source_df['Date'].dt.year.unique().tolist(), reverse=True)
            available_month_pairs = sorted(
                {
                    (int(ts.year), int(ts.month))
                    for ts in week_source_df['Date'].dropna()
                }
            )

            if not available_month_pairs:
                st.info("No month data available for week-wise comparison.")
            else:
                latest_year, latest_month = available_month_pairs[-1]
                current_pair = (
                    int(st.session_state.get("wk_cmp_year", latest_year)),
                    int(st.session_state.get("wk_cmp_month", latest_month))
                )
                if current_pair not in available_month_pairs:
                    st.session_state.wk_cmp_year = latest_year
                    st.session_state.wk_cmp_month = latest_month

                def _wk_cmp_prev_month():
                    selected_pair = (
                        int(st.session_state.get("wk_cmp_year", latest_year)),
                        int(st.session_state.get("wk_cmp_month", latest_month))
                    )
                    if selected_pair not in available_month_pairs:
                        st.session_state.wk_cmp_year = latest_year
                        st.session_state.wk_cmp_month = latest_month
                        return
                    idx = available_month_pairs.index(selected_pair)
                    if idx > 0:
                        prev_year, prev_month = available_month_pairs[idx - 1]
                        st.session_state.wk_cmp_year = prev_year
                        st.session_state.wk_cmp_month = prev_month

                def _wk_cmp_next_month():
                    selected_pair = (
                        int(st.session_state.get("wk_cmp_year", latest_year)),
                        int(st.session_state.get("wk_cmp_month", latest_month))
                    )
                    if selected_pair not in available_month_pairs:
                        st.session_state.wk_cmp_year = latest_year
                        st.session_state.wk_cmp_month = latest_month
                        return
                    idx = available_month_pairs.index(selected_pair)
                    if idx < len(available_month_pairs) - 1:
                        next_year, next_month = available_month_pairs[idx + 1]
                        st.session_state.wk_cmp_year = next_year
                        st.session_state.wk_cmp_month = next_month

                ctrl1, ctrl2, ctrl3 = st.columns([1, 1, 2])
                with ctrl1:
                    selected_year_week = st.selectbox(
                        "Select Year",
                        available_years,
                        key="wk_cmp_year"
                    )
                with ctrl2:
                    available_months_week = sorted(
                        [month for year, month in available_month_pairs if year == selected_year_week]
                    )
                    if st.session_state.get("wk_cmp_month") not in available_months_week:
                        st.session_state.wk_cmp_month = available_months_week[-1]
                    selected_month_week = st.selectbox(
                        "Select Month",
                        available_months_week,
                        format_func=lambda x: calendar.month_name[x],
                        key="wk_cmp_month"
                    )
                with ctrl3:
                    weekday_options = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                    selected_weekdays = st.multiselect(
                        "Weekday Focus (Optional)",
                        weekday_options,
                        default=weekday_options,
                        key="wk_cmp_weekdays"
                    )

                selected_pair = (selected_year_week, selected_month_week)
                selected_idx = available_month_pairs.index(selected_pair)
                nav_cols = st.columns([1, 8, 1])
                with nav_cols[0]:
                    st.button(
                        "◀",
                        key="wk_cmp_prev_nav",
                        help="Previous month",
                        on_click=_wk_cmp_prev_month,
                        disabled=selected_idx <= 0
                    )
                with nav_cols[1]:
                    st.markdown(
                        f"<div style='text-align:center; font-weight:600; color:#2E86AB; padding-top:6px;'>"
                        f"{calendar.month_name[selected_month_week]} {selected_year_week}</div>",
                        unsafe_allow_html=True
                    )
                with nav_cols[2]:
                    st.button(
                        "▶",
                        key="wk_cmp_next_nav",
                        help="Next month",
                        on_click=_wk_cmp_next_month,
                        disabled=selected_idx >= (len(available_month_pairs) - 1)
                    )

                employee_options_week = sorted(
                    week_source_df[
                        (week_source_df['Date'].dt.year == selected_year_week) &
                        (week_source_df['Date'].dt.month == selected_month_week)
                    ]['Employee Full Name'].unique().tolist()
                )
                default_week_employees = (
                    [emp for emp in selected_employees if emp in set(employee_options_week)][:10]
                    if selected_employees else employee_options_week[:min(5, len(employee_options_week))]
                )
                selected_week_employees = st.multiselect(
                    "Select Employees for Weekly Comparison (up to 10)",
                    employee_options_week,
                    default=default_week_employees,
                    key="wk_cmp_employees"
                )

                if len(selected_week_employees) > 10:
                    st.warning("Showing first 10 selected employees for readability.")
                    selected_week_employees = selected_week_employees[:10]

                weekday_scope = selected_weekdays if selected_weekdays else weekday_options
                if len(selected_week_employees) == 0:
                    st.info("Select at least one employee to render week-wise comparison.")
                else:
                    weekly_comp_df = get_weekly_employee_comparison_cached(
                        week_source_df,
                        selected_year_week,
                        selected_month_week,
                        tuple(selected_week_employees),
                        tuple(weekday_scope)
                    )
                    weekly_comp_df = ensure_week_index_column(weekly_comp_df)

                    if weekly_comp_df.empty:
                        st.info("No weekly comparison data available for the current month and selection.")
                    else:
                        kpi_a, kpi_b, kpi_c, kpi_d = st.columns(4)
                        with kpi_a:
                            create_metric_card("Employee-Weeks", int(len(weekly_comp_df)))
                        with kpi_b:
                            create_metric_card("Avg Hours / Employee-Week", f"{weekly_comp_df['Total Working Hours'].mean():.1f}h")
                        with kpi_c:
                            create_metric_card("Total Overtime", f"{weekly_comp_df['Overtime Hours'].sum():.1f}h")
                        with kpi_d:
                            flagged_weeks = (
                                (weekly_comp_df['Anomaly Days'] > 0) |
                                (weekly_comp_df['Late Days'] > 0) |
                                (weekly_comp_df['Early Departure Days'] > 0)
                            ).sum()
                            create_metric_card("Flagged Weeks", int(flagged_weeks))

                        week_heatmap = plot_weekly_comparison_heatmap(
                            weekly_comp_df,
                            selected_week_employees
                        )
                        if week_heatmap is not None:
                            st.plotly_chart(week_heatmap, use_container_width=True)
                        st.caption("Cell text shows total hours + worked days. Hover for overtime, timing, anomalies, and lunch signals.")

                        st.markdown("### Weekly Matrix Snapshot")
                        snapshot_df = weekly_comp_df.copy()
                        snapshot_df['Summary'] = snapshot_df.apply(
                            lambda r: (
                                f"{r['Total Working Hours']:.1f}h | {int(r['Working Days'])}d | "
                                f"OT {r['Overtime Hours']:.1f}h | A {int(r['Anomaly Days'])}"
                            ),
                            axis=1
                        )
                        week_order = (
                            snapshot_df[['Week Index', 'Week Label']]
                            .drop_duplicates()
                            .sort_values('Week Index')['Week Label']
                            .tolist()
                        )
                        matrix_df = snapshot_df.pivot(
                            index='Week Label',
                            columns='Employee Full Name',
                            values='Summary'
                        ).reindex(week_order)
                        ordered_cols = [emp for emp in selected_week_employees if emp in matrix_df.columns]
                        ordered_cols += [col for col in matrix_df.columns if col not in set(ordered_cols)]
                        matrix_df = matrix_df[ordered_cols]
                        st.dataframe(matrix_df.fillna("-"), use_container_width=True)

                        st.markdown("### Weekly Detailed Metrics")
                        detail_cols = [
                            'Week Label', 'Employee Full Name', 'Total Working Hours', 'Expected Hours',
                            'Overtime Hours', 'Working Days', 'Late Days', 'Early Departure Days',
                            'Anomaly Days', 'No Lunch Days', '8h+ No Lunch Days', 'Avg Meal / Day (min)'
                        ]
                        sort_cols = ['Total Working Hours']
                        sort_ascending = [False]
                        if 'Week Index' in weekly_comp_df.columns:
                            sort_cols = ['Week Index', 'Total Working Hours']
                            sort_ascending = [True, False]
                        st.dataframe(
                            weekly_comp_df.sort_values(
                                sort_cols,
                                ascending=sort_ascending
                            )[detail_cols],
                            use_container_width=True,
                            height=420
                        )

    # ------------------------------------------------------------------------
    # TAB 7: LUNCH RISK & BEHAVIOR ANALYSIS
    # ------------------------------------------------------------------------
    with tab7:
        st.subheader("\U0001F37D Lunch Break Risk & Behavior Analysis")
        st.markdown("**Identify employees with unhealthy break patterns and sustained no-lunch workdays.**")

        risk_source_df = daily_df.copy()
        risk_source_df['Date'] = pd.to_datetime(risk_source_df['Date'])
        if selected_depts and 'Department' in risk_source_df.columns:
            risk_source_df = risk_source_df[risk_source_df['Department'].isin(selected_depts)]
        if selected_employees:
            risk_source_df = risk_source_df[risk_source_df['Employee Full Name'].isin(selected_employees)]

        if risk_source_df.empty:
            st.warning("No data available for lunch risk analysis under the current filters.")
        else:
            available_years_risk = sorted(risk_source_df['Date'].dt.year.unique().tolist(), reverse=True)
            rcol1, rcol2 = st.columns(2)
            with rcol1:
                selected_year_risk = st.selectbox("Select Year", available_years_risk, key="risk_year")
            with rcol2:
                available_months_risk = sorted(
                    risk_source_df[risk_source_df['Date'].dt.year == selected_year_risk]['Date'].dt.month.unique().tolist()
                )
                selected_month_risk = st.selectbox(
                    "Select Month",
                    available_months_risk,
                    format_func=lambda x: calendar.month_name[x],
                    key="risk_month"
                )

            employee_options_risk = sorted(
                risk_source_df[
                    (risk_source_df['Date'].dt.year == selected_year_risk) &
                    (risk_source_df['Date'].dt.month == selected_month_risk)
                ]['Employee Full Name'].unique().tolist()
            )
            default_risk_employees = (
                [emp for emp in selected_employees if emp in set(employee_options_risk)]
                if selected_employees else employee_options_risk
            )
            selected_risk_employees = st.multiselect(
                "Select Employees for Risk Analysis",
                employee_options_risk,
                default=default_risk_employees,
                key="risk_employees"
            )

            with st.expander("Risk Thresholds", expanded=False):
                high_work_hours = st.slider(
                    "High Workday Threshold (hours)",
                    min_value=6.0,
                    max_value=12.0,
                    value=8.0,
                    step=0.5,
                    key="risk_high_work_hours"
                )
                short_lunch_minutes = st.slider(
                    "Extremely Short Lunch Threshold (minutes)",
                    min_value=5,
                    max_value=45,
                    value=20,
                    step=5,
                    key="risk_short_lunch_minutes"
                )
                avg_lunch_warning_minutes = st.slider(
                    "Average Lunch Warning Threshold (minutes)",
                    min_value=10,
                    max_value=60,
                    value=25,
                    step=5,
                    key="risk_avg_warning_minutes"
                )
                long_continuous_hours = st.slider(
                    "Long Continuous Work Threshold (hours)",
                    min_value=4.0,
                    max_value=10.0,
                    value=6.0,
                    step=0.5,
                    key="risk_long_continuous_hours"
                )

            if len(selected_risk_employees) == 0:
                st.info("Select at least one employee to run lunch risk analysis.")
            else:
                lunch_risk_df = get_lunch_break_risk_cached(
                    risk_source_df,
                    selected_year_risk,
                    selected_month_risk,
                    tuple(selected_risk_employees),
                    float(high_work_hours),
                    int(short_lunch_minutes),
                    int(avg_lunch_warning_minutes),
                    float(long_continuous_hours)
                )

                if lunch_risk_df.empty:
                    st.info("No lunch risk records found for this month and employee set.")
                else:
                    c1, c2, c3, c4, c5 = st.columns(5)
                    with c1:
                        create_metric_card("Critical", int((lunch_risk_df['Risk Level'] == 'Critical').sum()))
                    with c2:
                        create_metric_card("High", int((lunch_risk_df['Risk Level'] == 'High').sum()))
                    with c3:
                        create_metric_card("Warning", int((lunch_risk_df['Risk Level'] == 'Warning').sum()))
                    with c4:
                        create_metric_card("8h+ No-Lunch Days", int(lunch_risk_df['High-Risk No Lunch Days'].sum()))
                    with c5:
                        create_metric_card("Avg Lunch (Team)", f"{lunch_risk_df['Avg Lunch Minutes'].mean():.1f} min")

                    fig_left, fig_right = st.columns(2)
                    with fig_left:
                        risk_bar = plot_lunch_risk_bar_chart(lunch_risk_df, top_n=15)
                        if risk_bar is not None:
                            st.plotly_chart(risk_bar, use_container_width=True)
                    with fig_right:
                        risk_scatter = plot_lunch_risk_scatter(
                            lunch_risk_df,
                            avg_lunch_warning_minutes=avg_lunch_warning_minutes
                        )
                        if risk_scatter is not None:
                            st.plotly_chart(risk_scatter, use_container_width=True)

                    priority_df = lunch_risk_df[lunch_risk_df['Risk Level'].isin(['Critical', 'High'])]
                    if len(priority_df) > 0:
                        st.warning(
                            "Priority follow-up: " +
                            ", ".join(priority_df['Employee Full Name'].head(8).tolist())
                        )
                    else:
                        st.success("No high-severity lunch-break risks identified in this month.")

                    st.markdown("### Risk Detail Table")
                    risk_cols = [
                        'Employee Full Name', 'Risk Level', 'Risk Score', 'Working Days',
                        'Total Working Hours', 'Total Meal Hours', 'Avg Lunch Minutes',
                        'No Lunch Days', 'Short Lunch Days', 'Long Continuous Work Days',
                        'High-Risk No Lunch Days', 'Max No Lunch Streak', 'Risk Drivers'
                    ]
                    st.dataframe(
                        lunch_risk_df[risk_cols],
                        use_container_width=True,
                        height=460
                    )

    # ------------------------------------------------------------------------
    # TAB 8: ANOMALY DASHBOARD
    # ------------------------------------------------------------------------
    with tab8:
        st.subheader("\u26A0 Anomaly Detection & Analysis")
        
        # Anomaly summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            missing_out = view_df['Missing Punch Out'].sum()
            st.metric("Missing Punch-Outs", missing_out)
        
        with col2:
            short_shifts = view_df['Unusually Short'].sum()
            st.metric("Short Shifts (<4h)", short_shifts)
        
        with col3:
            long_shifts = view_df['Unusually Long'].sum()
            st.metric("Long Shifts (>10h)", long_shifts)
        
        with col4:
            odd_punches = view_df['Odd Punch Count'].sum()
            st.metric("Odd Punch Counts", odd_punches)
        
        # Anomaly details
        st.subheader("Anomaly Records")
        
        anomaly_records = view_df[view_df['Has Anomaly']][
            ['Employee Full Name', 'Date', 'Working Hours', 'Punch Count',
             'Missing Punch Out', 'Unusually Short', 'Unusually Long']
        ].sort_values('Date', ascending=False)
        
        if len(anomaly_records) > 0:
            st.dataframe(anomaly_records, use_container_width=True, height=400)
        else:
            st.success("No anomalies detected in selected period!")
    
    # ------------------------------------------------------------------------
    # TAB 9: DATA TABLE & EXPORT
    # ------------------------------------------------------------------------
    with tab9:
        st.subheader("\U0001F4CA Attendance Data Table")
        
        # Column selector
        available_cols = view_df.columns.tolist()
        default_cols = [
            'Employee Full Name', 'Date', 'First Punch In', 'Last Punch Out',
            'Net Working Hours', 'Meal Hours', 'Is Late', 'Is Early Departure', 'Shift Type'
        ]
        
        selected_cols = st.multiselect(
            "Select columns to display",
            available_cols,
            default=[col for col in default_cols if col in available_cols]
        )
        
        if selected_cols:
            display_data = view_df[selected_cols].sort_values('Date', ascending=False)
            st.dataframe(display_data, use_container_width=True, height=500)
            
            # Export options
            st.subheader("\U0001F4CA Export Data")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv_daily = view_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Daily Records (CSV)",
                    data=csv_daily,
                    file_name=f"daily_attendance_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            with col2:
                # Filter employee metrics for current view
                filtered_emp_nums = view_df['Employee Number'].unique()
                emp_export = emp_metrics_df[emp_metrics_df['Employee Number'].isin(filtered_emp_nums)]
                csv_emp = emp_export.to_csv(index=False).encode('utf-8')
                
                st.download_button(
                    label="Download Employee Summary (CSV)",
                    data=csv_emp,
                    file_name=f"employee_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            with col3:
                # Monthly summary
                monthly = view_df.groupby(['Employee Full Name', 'Month']).agg({
                    'Working Hours': 'sum',
                    'Meal Hours': 'sum',
                    'Date': 'count',
                    'Is Late': 'sum',
                    'Is Early Departure': 'sum'
                }).reset_index()
                monthly.columns = [
                    'Employee', 'Month', 'Total Hours', 'Total Meal Hours',
                    'Days Worked', 'Late Count', 'Early Count'
                ]
                
                csv_monthly = monthly.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Monthly Summary (CSV)",
                    data=csv_monthly,
                    file_name=f"monthly_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
        else:
            st.warning("Please select at least one column to display")
    
    # ------------------------------------------------------------------------
    # TAB 5: WORK PATTERN CALENDAR
    # ------------------------------------------------------------------------
    with tab5:
        st.subheader("\U0001F5D3 Work Pattern Calendar")
        st.markdown("**Employee-specific work pattern calendar view**")
        
        # Employee selection (respects sidebar filters)
        wp_source_df = daily_filtered.copy()
        employee_options = sorted(wp_source_df['Employee Full Name'].unique().tolist())
        
        if len(employee_options) == 0:
            st.warning("No employees available for the selected filters.")
        else:
            # Resolve current selections before rendering KPIs (values live in session_state)
            if "wp_cal_emp" not in st.session_state or st.session_state.wp_cal_emp not in employee_options:
                st.session_state.wp_cal_emp = employee_options[0]
            selected_emp_wp = st.session_state.wp_cal_emp

            available_dates = wp_source_df[wp_source_df['Employee Full Name'] == selected_emp_wp]['Date']

            if len(available_dates) > 0:
                min_date = available_dates.min()
                max_date = available_dates.max()
                available_years = list(range(min_date.year, max_date.year + 1))

                if "wp_cal_year" not in st.session_state or st.session_state.wp_cal_year not in available_years:
                    st.session_state.wp_cal_year = available_years[-1]
                selected_year_wp = st.session_state.wp_cal_year

                if "wp_cal_month" not in st.session_state or not (1 <= st.session_state.wp_cal_month <= 12):
                    st.session_state.wp_cal_month = min_date.month if selected_year_wp == min_date.year else 1
                selected_month_wp = st.session_state.wp_cal_month

                min_month_date = date(min_date.year, min_date.month, 1)
                max_month_date = date(max_date.year, max_date.month, 1)

                def _wp_cal_prev():
                    current = date(st.session_state.wp_cal_year, st.session_state.wp_cal_month, 1)
                    if current <= min_month_date:
                        return
                    if st.session_state.wp_cal_month == 1:
                        st.session_state.wp_cal_month = 12
                        st.session_state.wp_cal_year -= 1
                    else:
                        st.session_state.wp_cal_month -= 1

                def _wp_cal_next():
                    current = date(st.session_state.wp_cal_year, st.session_state.wp_cal_month, 1)
                    if current >= max_month_date:
                        return
                    if st.session_state.wp_cal_month == 12:
                        st.session_state.wp_cal_month = 1
                        st.session_state.wp_cal_year += 1
                    else:
                        st.session_state.wp_cal_month += 1

                selected_year_wp = st.session_state.wp_cal_year
                selected_month_wp = st.session_state.wp_cal_month

                kpi_data = get_work_pattern_kpis_cached(
                    wp_source_df, selected_emp_wp, selected_year_wp, selected_month_wp, annotation_items
                )
                st.markdown("### Calendar KPIs")
                kpi_row1 = st.columns(3)
                with kpi_row1[0]:
                    create_metric_card("Expected Working Days", int(kpi_data['expected_days']))
                with kpi_row1[1]:
                    create_metric_card(
                        "Actual Worked Days",
                        int(kpi_data['actual_days']),
                        help_text="Counts attendance on expected workdays only."
                    )
                with kpi_row1[2]:
                    create_metric_card("Days Missed", int(kpi_data['missed_days']))

                kpi_row2 = st.columns(3)
                with kpi_row2[0]:
                    create_metric_card("Expected Working Hours", f"{kpi_data['expected_hours']:.1f}h")
                with kpi_row2[1]:
                    create_metric_card(
                        "Actual Worked Hours",
                        f"{kpi_data['actual_hours']:.1f}h",
                        help_text="Totals expected workdays only."
                    )
                with kpi_row2[2]:
                    create_metric_card("Hours Short / Extra", f"{kpi_data['hours_diff']:+.1f}h")

                kpi_row_extra = st.columns(3)
                with kpi_row_extra[0]:
                    create_metric_card(
                        "Total Actual Hours (All Days)",
                        f"{kpi_data['actual_hours_all_days']:.1f}h",
                        help_text="Includes expected and non-working days."
                    )

                kpi_row3 = st.columns(3)
                with kpi_row3[0]:
                    create_metric_card("Late Arrivals", int(kpi_data['late_arrivals']))
                with kpi_row3[1]:
                    create_metric_card("Early Departures", int(kpi_data['early_departures']))
                with kpi_row3[2]:
                    create_metric_card("On-Time Days", int(kpi_data['on_time_days']))

                kpi_row4 = st.columns(2)
                with kpi_row4[0]:
                    create_metric_card(
                        "High-Risk No-Lunch Days",
                        int(kpi_data.get('high_risk_no_lunch_days', 0)),
                        help_text="Expected workdays only: Working Hours >= 8 and Meal Hours = 0 (Friday included for this high-risk case)."
                    )
                with kpi_row4[1]:
                    create_metric_card(
                        "No-Lunch Working Days",
                        int(kpi_data.get('no_lunch_working_days', 0)),
                        help_text="Expected workdays only: Meal Hours = 0, excluding normal Fridays."
                    )

                if kpi_data['worked_non_working_days'] > 0:
                    st.caption(
                        f"Worked on non-working days: {int(kpi_data['worked_non_working_days'])} "
                        "day(s). Weekend days are hidden in the calendar view."
                    )

                st.markdown("---")

                filter_row = st.columns([2, 1, 1])
                with filter_row[0]:
                    st.selectbox("Select Employee", employee_options, key="wp_cal_emp")
                with filter_row[1]:
                    st.selectbox("Select Year", available_years, key="wp_cal_year")
                with filter_row[2]:
                    st.selectbox("Select Month", range(1, 13), key="wp_cal_month")

                pattern_message = get_work_pattern_context_text(selected_emp_wp)
                if pattern_message:
                    st.markdown(
                        f"""
                        <div style="background-color: #f3f6fb; border-left: 4px solid #2E86AB;
                                    padding: 10px 14px; border-radius: 6px; color: #1a1a1a; margin-top: 8px;">
                            {pattern_message}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                with st.expander("Special Day Annotations", expanded=False):
                    add_col, list_col = st.columns([3, 2])
                    with add_col:
                        min_special_date = min_date.date()
                        max_special_date = max_date.date()
                        default_special_date = date(selected_year_wp, selected_month_wp, 1)
                        if default_special_date < min_special_date:
                            default_special_date = min_special_date
                        if default_special_date > max_special_date:
                            default_special_date = max_special_date
                        if "wp_special_date" in st.session_state:
                            current_special_date = st.session_state.wp_special_date
                            if current_special_date < min_special_date or current_special_date > max_special_date:
                                st.session_state.wp_special_date = default_special_date
                        else:
                            st.session_state.wp_special_date = default_special_date
                        special_date = st.date_input(
                            "Select Date",
                            value=st.session_state.wp_special_date,
                            min_value=min_special_date,
                            max_value=max_special_date,
                            key="wp_special_date"
                        )
                        special_type = st.selectbox(
                            "Operational Note",
                            list(ANNOTATION_TYPES),
                            key="wp_special_type"
                        )
                        special_reason = st.text_input(
                            "Reason (optional)",
                            key="wp_special_reason",
                            placeholder="e.g., Weather closure, Staff meeting"
                        )
                        special_open_time = None
                        special_close_time = None
                        if special_type == "Special Hours":
                            special_time_cols = st.columns(2)
                            with special_time_cols[0]:
                                special_open_time = st.time_input(
                                    "Open Time",
                                    value=Config.STANDARD_START_TIME,
                                    key="wp_special_open_time"
                                )
                            with special_time_cols[1]:
                                default_close_time = (
                                    Config.EARLY_DEPARTURE_TIME_FRI
                                    if special_date.weekday() == 4
                                    else Config.EARLY_DEPARTURE_TIME_MON_THU
                                )
                                special_close_time = st.time_input(
                                    "Close Time",
                                    value=default_close_time,
                                    key="wp_special_close_time"
                                )
                        if st.button("Save Special Day", key="wp_special_save"):
                            payload_reason = special_reason.strip()
                            if special_type == "Special Hours":
                                if special_open_time is None or special_close_time is None:
                                    st.error("Please provide both open and close times for Special Hours.")
                                elif special_open_time >= special_close_time:
                                    st.error("Close time must be later than open time for Special Hours.")
                                else:
                                    payload_reason = format_special_hours_reason(
                                        special_open_time, special_close_time, payload_reason
                                    )
                                    if upsert_annotation(special_date, special_type, payload_reason):
                                        st.success("Annotation saved.")
                                        if hasattr(st, "rerun"):
                                            st.rerun()
                                        else:
                                            st.experimental_rerun()
                                    else:
                                        st.error("Unable to save annotation. Please check database connection.")
                            else:
                                if upsert_annotation(special_date, special_type, payload_reason):
                                    st.success("Annotation saved.")
                                    if hasattr(st, "rerun"):
                                        st.rerun()
                                    else:
                                        st.experimental_rerun()
                                else:
                                    st.error("Unable to save annotation. Please check database connection.")
                    with list_col:
                        month_items = []
                        for date_key, ann_type, ann_reason in annotation_items:
                            try:
                                dval = datetime.strptime(date_key, "%Y-%m-%d").date()
                            except (TypeError, ValueError):
                                continue
                            if dval.year == selected_year_wp and dval.month == selected_month_wp:
                                open_time, close_time, parsed_reason = parse_special_hours_reason(ann_reason)
                                hours_window = ""
                                display_reason = ann_reason
                                if ann_type == "Special Hours":
                                    if open_time and close_time:
                                        hours_window = f"{open_time.strftime('%H:%M')} - {close_time.strftime('%H:%M')}"
                                    display_reason = parsed_reason
                                month_items.append({
                                    "Date": dval,
                                    "Type": ann_type,
                                    "Hours": hours_window,
                                    "Reason": display_reason
                                })
                        month_items = sorted(month_items, key=lambda r: r["Date"])
                        if month_items:
                            month_df = pd.DataFrame(month_items)
                            st.dataframe(month_df, use_container_width=True)
                            remove_options = [row["Date"] for row in month_items]
                            remove_date = st.selectbox(
                                "Remove Date",
                                remove_options,
                                format_func=lambda d: d.strftime("%b %d, %Y"),
                                key="wp_special_remove_date"
                            )
                            if st.button("Remove", key="wp_special_remove_btn"):
                                if delete_annotation(remove_date):
                                    st.success("Annotation removed.")
                                    if hasattr(st, "rerun"):
                                        st.rerun()
                                    else:
                                        st.experimental_rerun()
                                else:
                                    st.error("Unable to remove annotation. Please check database connection.")
                        else:
                            st.caption("No special days noted for this month.")

                special_day_items = []
                for date_key, ann_type, ann_reason in annotation_items:
                    try:
                        dval = datetime.strptime(date_key, "%Y-%m-%d").date()
                    except (TypeError, ValueError):
                        continue
                    if dval.year == selected_year_wp and dval.month == selected_month_wp:
                        special_day_items.append(
                            (date_key, ann_type, ann_reason)
                        )
                special_day_items = tuple(sorted(special_day_items))

                current_month_date = date(selected_year_wp, selected_month_wp, 1)
                prev_disabled = current_month_date <= min_month_date
                next_disabled = current_month_date >= max_month_date

                nav_cols = st.columns([1, 16, 1])
                with nav_cols[0]:
                    st.button("◀", key="wp_cal_prev", help="Previous month", on_click=_wp_cal_prev, disabled=prev_disabled)
                with nav_cols[2]:
                    st.button("▶", key="wp_cal_next", help="Next month", on_click=_wp_cal_next, disabled=next_disabled)
                with nav_cols[1]:
                    # Generate calendar
                    calendar_html = get_work_pattern_calendar_cached(
                        wp_source_df,
                        selected_emp_wp,
                        selected_year_wp,
                        selected_month_wp,
                        kpi_data,
                        special_day_items
                    )
                    
                    # Use Streamlit's HTML component for proper rendering (not markdown)
                    calendar_row_count = len(calendar.Calendar(firstweekday=0).monthdayscalendar(selected_year_wp, selected_month_wp))
                    calendar_height = 480 + (calendar_row_count * 120)
                    components.html(calendar_html, height=calendar_height, scrolling=False)
                
                st.markdown("---")
                st.markdown("### Work Pattern Distribution")
                
                distribution_df = get_work_pattern_distribution_cached(
                    wp_source_df, selected_emp_wp, selected_year_wp, selected_month_wp, annotation_items
                )
                if len(distribution_df) > 0:
                    color_map = {
                        'Full Day': '#2f9e44',
                        'Half Day': '#caa531',
                        'Short Day': '#cf6d21',
                        'Absent': '#b73b3b',
                        'Holiday': '#5c8db8',
                        'Week Off': '#7a8794',
                        'Worked on Non-Working Day': '#607d8b'
                    }
                    
                    fig = px.bar(
                        distribution_df,
                        x='Attendance Type',
                        y='Count',
                        title=f'Distribution - {calendar.month_name[selected_month_wp]} {selected_year_wp}',
                        labels={'Count': 'Number of Days', 'Attendance Type': 'Attendance Type'},
                        color='Attendance Type',
                        color_discrete_map=color_map
                    )
                    
                    fig.update_layout(
                        height=400,
                        showlegend=False,
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        xaxis_title='Attendance Type',
                        yaxis_title='Number of Days'
                    )
                    
                    fig.update_traces(texttemplate='%{y}', textposition='outside')
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption("Distribution of attendance types for the selected month (based on work patterns)")
                
                st.info("Legend is embedded in the calendar. OFF tags mark off-day work; L/E/M/A badges highlight timing and punch issues; Special tags and meal dots flag operational notes and meal-risk days.")
            else:
                st.warning(f"No attendance data found for {selected_emp_wp}")
    
    # ========================================================================
    # FOOTER
    # ========================================================================
    
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p><strong>HR Attendance Analytics Dashboard v1.0</strong></p>
            <p>Powered by Streamlit | Data processed with Pandas & Plotly</p>
            <p>For support or feedback, contact the HR Analytics Team</p>
        </div>
    """, unsafe_allow_html=True)

# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
    
