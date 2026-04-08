from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from monitor import db
from monitor.config import load_config

st.set_page_config(page_title="SEO Indexing Monitor (Local)", layout="wide")
st.markdown(
    """
    <style>
    div[data-testid="stCheckbox"] {
        padding-top: 0.35rem;
    }
    div[data-testid="stCheckbox"] > label {
        align-items: center;
        gap: 0.55rem;
    }
    div[data-testid="stCheckbox"] p {
        margin: 0;
        line-height: 1.2;
    }
    .filter-caption {
        font-size: 0.95rem;
        font-weight: 600;
        margin: 0 0 0.35rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


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
for row in all_rows:
    parsed_date = pd.to_datetime(row.get("date", ""), errors="coerce")
    row["_date_obj"] = parsed_date.date() if not pd.isna(parsed_date) else None

properties: List[str] = sorted({row["property_key"] for row in all_rows})
if not properties:
    properties = [p.key for p in cfg.properties]

filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns([1.0, 1.0, 1.4, 0.9, 1.0])

with filter_col1:
    selected_property = st.selectbox("Property", options=["All"] + properties, index=0)

with filter_col2:
    status_filter = st.selectbox(
        "Status",
        options=[
            "All",
            "Pending",
            "Indexed",
            "Error",
            "Quota Exceeded",
            "Blocked by robots.txt",
            "Blocked by noindex",
            "Excluded",
        ],
        index=0,
    )

available_dates = sorted({row["_date_obj"] for row in all_rows if row.get("_date_obj")})
date_range = None
if available_dates:
    with filter_col3:
        date_range = st.date_input(
            "Date Range",
            value=(available_dates[0], available_dates[-1]),
            min_value=available_dates[0],
            max_value=available_dates[-1],
        )

with filter_col4:
    min_check_count = st.number_input(
        "Min Check Count",
        min_value=0,
        value=0,
        step=1,
    )

with filter_col5:
    st.markdown("<div class='filter-caption'>Latency Filter</div>", unsafe_allow_html=True)
    latency_gt_five_only = st.checkbox("Latency >5m only", value=False)

property_key = None if selected_property == "All" else selected_property
base_rows = all_rows
if property_key:
    base_rows = [row for row in base_rows if row["property_key"] == property_key]

if available_dates and date_range:
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range
    base_rows = [
        row
        for row in base_rows
        if row.get("_date_obj") and start_date <= row["_date_obj"] <= end_date
    ]

if min_check_count > 0:
    base_rows = [
        row
        for row in base_rows
        if int(row.get("check_count", 0) or 0) >= int(min_check_count)
    ]

if latency_gt_five_only:
    base_rows = [
        row
        for row in base_rows
        if row.get("indexing_latency_minutes") not in (None, "")
        and float(row.get("indexing_latency_minutes") or 0) > 5
    ]

total_count = len(base_rows)
indexed_count = sum(1 for row in base_rows if row.get("current_status") == "Indexed")
error_count = sum(
    1
    for row in base_rows
    if row.get("current_status") in {"Error", "Quota Exceeded"}
)
not_indexed_count = max(total_count - indexed_count, 0)
indexed_pct = (indexed_count / total_count * 100) if total_count else 0.0
late_indexed_count = sum(
    1
    for row in base_rows
    if row.get("indexing_latency_minutes") not in (None, "")
    and float(row.get("indexing_latency_minutes") or 0) > 5
)
late_indexed_pct = (late_indexed_count / indexed_count * 100) if indexed_count else 0.0

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total URLs", total_count)
c2.metric("Indexed", indexed_count)
c3.metric("Not Indexed", not_indexed_count)
c4.metric("Indexed %", f"{indexed_pct:.1f}%")
c5.metric("Errors", error_count)
c6.metric("Latency >5m", late_indexed_count)
c7.metric(">5m %", f"{late_indexed_pct:.1f}%")

rows = list(base_rows)
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
    st.dataframe(df[show_cols], width="stretch", hide_index=True)

    urls = [r["url"] for r in rows]
    chosen_url = st.selectbox("Inspect logs for URL", options=["(none)"] + urls, index=0)

    if chosen_url != "(none)":
        logs = db.fetch_logs(conn, property_key=property_key, url=chosen_url, limit=500)
        st.subheader("Check Logs")
        if logs:
            st.dataframe(pd.DataFrame(logs), width="stretch", hide_index=True)
        else:
            st.info("No checks logged yet for this URL.")
else:
    st.info("No rows found for the current filter.")

st.subheader("Recent Logs")
recent_logs = db.fetch_logs(conn, property_key=property_key, limit=200)
if recent_logs:
    st.dataframe(pd.DataFrame(recent_logs), width="stretch", hide_index=True)
else:
    st.info("No logs yet.")

conn.close()
