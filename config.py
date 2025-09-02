import streamlit as st

# Database configuration from secrets.toml
DB_CONFIG = {
    "host": st.secrets.DB_CONFIG.host,
    "port": st.secrets.DB_CONFIG.port,
    "database": st.secrets.DB_CONFIG.database,
    "user": st.secrets.DB_CONFIG.user,
    "password": st.secrets.DB_CONFIG.password
}

# Application settings
APP_CONFIG = {
    "page_title": "MemeDD Dashboard",
    "page_icon": "🎯",
    "layout": "wide",
    "initial_sidebar_state": "expanded",
    "cache_ttl": 300  # 5 minutes
}

# Chart settings
CHART_CONFIG = {
    "marker_size": 8,
    "marker_opacity": 0.6,
    "marker_color": "#2E8B57",
    "line_color": "#1F5F3F",
    "line_width": 1
}
