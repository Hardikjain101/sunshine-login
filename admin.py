"""
Admin controls for managing users in the HR dashboard.
"""

from __future__ import annotations

from typing import List, Dict, Tuple

from mysql.connector import Error

from auth import USER_TABLE, ensure_user_table, get_connection, hash_password


def add_user(username: str, password: str, role: str) -> Tuple[bool, str]:
    """Add a new user with the given role."""
    username = (username or "").strip()
    role = (role or "").strip().lower()
    if not username or not password:
        return False, "Username and password are required."
    if role not in {"admin", "employee"}:
        return False, "Role must be 'admin' or 'employee'."

    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM {USER_TABLE} WHERE username = %s", (username,))
        if cursor.fetchone():
            return False, "User already exists."

        pw_hash = hash_password(password)
        cursor.execute(
            f"""
            INSERT INTO {USER_TABLE} (username, password_hash, role, is_active)
            VALUES (%s, %s, %s, %s)
            """,
            (username, pw_hash, role, 1),
        )
        conn.commit()
        return True, "User created."
    except Error:
        return False, "Database error while creating user."
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def deactivate_user(username: str) -> Tuple[bool, str]:
    """Deactivate a user account."""
    username = (username or "").strip()
    if not username:
        return False, "Username is required."

    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE {USER_TABLE} SET is_active = 0 WHERE username = %s",
            (username,),
        )
        conn.commit()
        if cursor.rowcount == 0:
            return False, "User not found."
        return True, "User deactivated."
    except Error:
        return False, "Database error while deactivating user."
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def reset_password(username: str, new_password: str) -> Tuple[bool, str]:
    """Reset a user's password."""
    username = (username or "").strip()
    if not username or not new_password:
        return False, "Username and new password are required."

    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor()
        pw_hash = hash_password(new_password)
        cursor.execute(
            f"UPDATE {USER_TABLE} SET password_hash = %s WHERE username = %s",
            (pw_hash, username),
        )
        conn.commit()
        if cursor.rowcount == 0:
            return False, "User not found."
        return True, "Password updated."
    except Error:
        return False, "Database error while resetting password."
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def list_users() -> List[Dict[str, object]]:
    """Return a list of users (no passwords)."""
    conn = None
    cursor = None
    try:
        ensure_user_table()
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"""
            SELECT id, username, role, is_active
            FROM {USER_TABLE}
            ORDER BY username ASC
            """
        )
        rows = cursor.fetchall()
        return rows or []
    except Error:
        return []
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def render_admin_panel() -> None:
    """Streamlit UI for admin actions."""
    import streamlit as st

    st.title("Admin Panel")
    st.markdown("Manage users and access.")

    tab_add, tab_reset, tab_deactivate, tab_list = st.tabs(
        ["Add User", "Reset Password", "Deactivate User", "Users"]
    )

    with tab_add:
        with st.form("add_user_form", clear_on_submit=True):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            role = st.selectbox("Role", ["employee", "admin"])
            submitted = st.form_submit_button("Create User")
        if submitted:
            ok, message = add_user(username, password, role)
            if ok:
                st.success(message)
            else:
                st.error(message)

    with tab_reset:
        with st.form("reset_password_form", clear_on_submit=True):
            username = st.text_input("Username")
            new_password = st.text_input("New Password", type="password")
            submitted = st.form_submit_button("Reset Password")
        if submitted:
            ok, message = reset_password(username, new_password)
            if ok:
                st.success(message)
            else:
                st.error(message)

    with tab_deactivate:
        with st.form("deactivate_user_form", clear_on_submit=True):
            username = st.text_input("Username")
            submitted = st.form_submit_button("Deactivate User")
        if submitted:
            ok, message = deactivate_user(username)
            if ok:
                st.success(message)
            else:
                st.error(message)

    with tab_list:
        st.subheader("Current Users")
        users = list_users()
        if users:
            st.dataframe(users, use_container_width=True)
        else:
            st.info("No users found or database unavailable.")


__all__ = [
    "add_user",
    "deactivate_user",
    "reset_password",
    "render_admin_panel",
]
