from __future__ import annotations

import os

import streamlit as st


def inject_global_styles(css_path: str = "static/styles.css") -> None:
    """
    Inject shared CSS into Streamlit app if the style file exists.
    """
    if not css_path or not os.path.exists(css_path):
        return
    try:
        with open(css_path, "r", encoding="utf-8") as handle:
            css = handle.read()
    except OSError:
        return
    if css.strip():
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
