from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from monitor import db
from monitor.config import load_config

st.set_page_config(page_title="SEO Indexing Monitor (Local)", layout="wide")


def _load_config() -> Path:
    default = Path("config.local.json")
    if default.exists():
        return default
    example = Path("config.example.json")
    return example


config_path = _load_config()
cfg = load_config(config_path)
conn = db.connect(cfg.db_path)
db.init_db(conn)

st.title("SEO Indexing Monitor")
st.caption(
    f"DB: `{cfg.db_path}` | Config: `{config_path}` | Cutoff: `{cfg.cutoff_datetime}`"
)

all_rows = db.fetch_all_summary(conn)
properties: List[str] = sorted({row["property_key"] for row in all_rows})
if not properties:
    properties = [p.key for p in cfg.properties]

selected_property = st.selectbox("Property", options=["All"] + properties, index=0)
status_filter = st.selectbox(
    "Status",
    options=["All", "Pending", "Indexed", "Error", "Quota Exceeded", "Blocked by robots.txt", "Blocked by noindex", "Excluded"],
    index=0,
)

property_key = None if selected_property == "All" else selected_property
counts = db.summary_counts(conn, property_key)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total URLs", counts["total"])
c2.metric("Indexed", counts["indexed"])
c3.metric("Pending", counts["pending"])
c4.metric("Errors", counts["errors"])

rows = db.fetch_all_summary(conn, property_key=property_key)
if status_filter != "All":
    rows = [r for r in rows if r.get("current_status") == status_filter]

st.subheader("URL State")
if rows:
    df = pd.DataFrame(rows)
    show_cols = [
        "property_key",
        "date",
        "url",
        "sitemap_published_date",
        "current_status",
        "check_count",
        "first_checked_at",
        "last_checked_at",
        "google_last_crawl_at",
        "indexing_latency_minutes",
        "gsc_coverage_state",
        "gsc_page_fetch_state",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

    urls = [r["url"] for r in rows]
    chosen_url = st.selectbox("Inspect logs for URL", options=["(none)"] + urls, index=0)

    if chosen_url != "(none)":
        logs = db.fetch_logs(conn, property_key=property_key, url=chosen_url, limit=500)
        st.subheader("Check Logs")
        if logs:
            st.dataframe(pd.DataFrame(logs), use_container_width=True, hide_index=True)
        else:
            st.info("No checks logged yet for this URL.")
else:
    st.info("No rows found for the current filter.")

st.subheader("Recent Logs")
recent_logs = db.fetch_logs(conn, property_key=property_key, limit=200)
if recent_logs:
    st.dataframe(pd.DataFrame(recent_logs), use_container_width=True, hide_index=True)
else:
    st.info("No logs yet.")

conn.close()
