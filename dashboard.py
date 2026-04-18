from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

from monitor import db
from monitor.config import load_config
from monitor import sheets as sheet_logger

IST = timezone(timedelta(hours=5, minutes=30))
SESSION_FILE_NAME = ".dashboard_session.json"
PROPERTY_SCHEDULE_MINUTES = {
    "thedailyjagran.com": 0,
    "TDJ_C2C": 5,
    "herzindagi.com_hi": 10,
    "herzindagi.com_en": 15,
    "Herzindagi_C2C_Hindi": 20,
    "Herzindagi_C2C_Engish": 25,
    "jagran.com": 30,
    "Jagran_C2C": 35,
    "jagranjosh.com": 40,
    "jagranreviews.com": 45,
    "onlymyhealth.com_en": 50,
    "onlymyhealth.com_hi": 55,
}

st.set_page_config(page_title="SEO Indexing Monitor (Local)", layout="wide")
st.markdown(
    """
    <style>
    [data-testid="stAppViewContainer"] {
        background: #ffffff;
    }
    [data-testid="stHeader"] {
        background: rgba(255, 255, 255, 0.92);
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
    }
    [data-testid="stSidebar"] {
        display: none;
    }
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
    .session-subtle {
        color: #475467;
        font-size: 0.95rem;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 1rem;
        padding: 0.9rem 1rem;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
    }
    div[data-testid="stMetricLabel"] p {
        color: #475467;
    }
    .topbar-title {
        margin-bottom: 0.25rem;
    }
    .topbar-caption {
        color: #475467;
        font-size: 0.98rem;
        margin-bottom: 1rem;
    }
    .thin-warning {
        padding: 0.75rem 0.9rem;
        border: 1px solid #fecdca;
        border-radius: 0.9rem;
        background: #fff6ed;
        color: #b42318;
        margin-bottom: 1rem;
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


def _secret_value(*keys: str) -> str:
    for key in keys:
        try:
            value = st.secrets.get(key, "")
        except Exception:
            value = ""
        if value not in (None, ""):
            return str(value)
        env_value = os.environ.get(key, "")
        if env_value:
            return env_value
    return ""


def _materialize_service_account_from_secrets() -> str:
    raw_json = _secret_value("GSC_SERVICE_ACCOUNT_JSON", "gsc_service_account_json")
    if raw_json:
        path = Path(tempfile.gettempdir()) / "seo-indexing-monitor-gsc.json"
        path.write_text(raw_json, encoding="utf-8")
        return str(path)

    try:
        payload = st.secrets.get("gsc_service_account", None)
    except Exception:
        payload = None

    if payload:
        path = Path(tempfile.gettempdir()) / "seo-indexing-monitor-gsc.json"
        path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)
    return ""


def _apply_runtime_overrides(cfg):
    db_url = _secret_value("SUPABASE_DB_URL", "supabase_db_url")
    if db_url:
        cfg.db_url = db_url

    db_path = _secret_value("INDEX_MONITOR_DB_PATH", "index_monitor_db_path")
    if db_path:
        cfg.db_path = db_path

    spreadsheet_id = _secret_value("LOGIN_HISTORY_SPREADSHEET_ID", "login_history_spreadsheet_id")
    if spreadsheet_id:
        cfg.login_history_spreadsheet_id = spreadsheet_id

    worksheet_name = _secret_value("LOGIN_HISTORY_WORKSHEET_NAME", "login_history_worksheet_name")
    if worksheet_name:
        cfg.login_history_worksheet_name = worksheet_name

    service_account_path = _secret_value("SERVICE_ACCOUNT_JSON_PATH", "service_account_json_path")
    if service_account_path:
        cfg.service_account_json_path = service_account_path
    elif not cfg.service_account_json_path:
        materialized = _materialize_service_account_from_secrets()
        if materialized:
            cfg.service_account_json_path = materialized

    return cfg


def _session_file_path(cfg) -> Path:
    return Path(cfg.db_path).resolve().parent / SESSION_FILE_NAME


def _ist_now() -> datetime:
    return datetime.now(IST).replace(microsecond=0)


def _format_ist(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed.astimezone(IST).replace(microsecond=0).isoformat()
    except Exception:
        return raw


def _format_timestamp_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    formatted = df.copy()
    for column in columns:
        if column in formatted.columns:
            formatted[column] = formatted[column].map(_format_ist)
    return formatted


def _next_expected_run(property_key: str, reference: datetime | None = None) -> str:
    minute = PROPERTY_SCHEDULE_MINUTES.get(property_key)
    if minute is None:
        return ""
    current = (reference or _ist_now()).astimezone(IST).replace(second=0, microsecond=0)
    candidate = current.replace(minute=minute)
    if candidate <= current:
        candidate = candidate + timedelta(hours=1)
    return candidate.isoformat()


def _normalize_username(username: str) -> tuple[str, str]:
    value = str(username or "").strip().lower().replace(" ", "")
    if not value:
        return "", "Please enter your username."
    if "@" in value:
        return "", "Enter only your username, without the email domain."
    if not re.fullmatch(r"[a-z0-9._-]+", value):
        return "", "Username can only contain letters, numbers, dot, underscore, or hyphen."
    return value, ""


def _restore_persisted_session(session_path: Path) -> None:
    if st.session_state.get("logged_in_username"):
        return
    if not session_path.exists():
        return
    try:
        payload = json.loads(session_path.read_text(encoding="utf-8"))
    except Exception:
        return

    username = str(payload.get("username", "")).strip().lower()
    logged_in_at = str(payload.get("logged_in_at", "")).strip()
    if not username or not logged_in_at:
        return

    st.session_state["logged_in_username"] = username
    st.session_state["logged_in_at"] = logged_in_at


def _persist_session(session_path: Path, username: str, logged_in_at: str) -> None:
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(
        json.dumps(
            {
                "username": username,
                "logged_in_at": logged_in_at,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )


def _clear_persisted_session(session_path: Path) -> None:
    if session_path.exists():
        session_path.unlink()


def _latency_value(row: dict) -> float | None:
    raw = row.get("indexing_latency_minutes")
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _friendly_login_sheet_error(raw_error: str) -> str:
    message = str(raw_error or "").strip()
    lowered = message.lower()
    if not message:
        return ""
    if "404" in lowered or "requested entity was not found" in lowered:
        return "Login sheet not found. Please recheck the spreadsheet ID and ensure the service account has explicit access."
    if "403" in lowered or "permission" in lowered or "access denied" in lowered:
        return "Login sheet access denied. Please share the sheet directly with the service account email."
    return "Login sheet sync failed. Please verify the spreadsheet ID and service account access."


def require_login(conn, cfg) -> tuple[str, str]:
    session_path = _session_file_path(cfg)
    _restore_persisted_session(session_path)

    username = str(st.session_state.get("logged_in_username", "")).strip().lower()
    logged_in_at = str(st.session_state.get("logged_in_at", "")).strip()
    if username and logged_in_at:
        return username, logged_in_at

    st.title("SEO Indexing Monitor")
    st.caption("Use your Jagran username to enter the dashboard.")
    with st.form("login_form", clear_on_submit=False):
        username_input = st.text_input("Username", placeholder="firstname.lastname")
        submitted = st.form_submit_button("Continue", type="primary")

    if submitted:
        username, error = _normalize_username(username_input)
        if error:
            st.error(error)
        else:
            logged_in_at = _ist_now().isoformat()
            st.session_state["logged_in_username"] = username
            st.session_state["logged_in_at"] = logged_in_at
            _persist_session(session_path, username, logged_in_at)
            db.insert_login_event(conn, logged_in_at[:10], username, logged_in_at)
            if cfg.login_history_spreadsheet_id and cfg.service_account_json_path:
                try:
                    sheet_logger.append_login_history_row(
                        cfg.service_account_json_path,
                        cfg.login_history_spreadsheet_id,
                        cfg.login_history_worksheet_name,
                        [logged_in_at[:10], username, logged_in_at],
                    )
                    st.session_state.pop("login_sheet_error", None)
                except Exception as exc:
                    st.session_state["login_sheet_error"] = str(exc)
            else:
                st.session_state.pop("login_sheet_error", None)
            st.rerun()
    st.stop()


def render_account_panel(conn, cfg, username: str, logged_in_at: str) -> None:
    session_path = _session_file_path(cfg)
    title_col, action_col = st.columns([12, 1])
    with title_col:
        st.markdown("<h1 class='topbar-title'>SEO Indexing Monitor</h1>", unsafe_allow_html=True)
    with action_col:
        st.markdown("<div style='height: 0.15rem;'></div>", unsafe_allow_html=True)
        if st.button("⎋", help="Log out", key="logout_icon_button", type="tertiary"):
            _clear_persisted_session(session_path)
            st.session_state.pop("logged_in_username", None)
            st.session_state.pop("logged_in_at", None)
            st.rerun()


config_path = _load_config()
cfg = load_config(config_path)
cfg = _apply_runtime_overrides(cfg)
conn = db.connect(cfg.db_path, cfg.db_url)
db.init_db(conn)

logged_in_username, logged_in_at = require_login(conn, cfg)
render_account_panel(conn, cfg, logged_in_username, logged_in_at)

all_rows = db.fetch_all_summary(conn)
for row in all_rows:
    parsed_date = pd.to_datetime(row.get("date", ""), errors="coerce")
    row["_date_obj"] = parsed_date.date() if not pd.isna(parsed_date) else None

properties: List[str] = sorted({row["property_key"] for row in all_rows})
if not properties:
    properties = [p.key for p in cfg.properties]

known_properties = sorted({p.key for p in cfg.properties} | set(properties))
property_state_rows = {row["property_key"]: row for row in db.fetch_property_states(conn)}
run_status_rows = []
for property_name in known_properties:
    state = property_state_rows.get(property_name, {})
    last_crawled_at = state.get("last_crawled_at", "")
    if not last_crawled_at:
        property_rows = [row for row in all_rows if row.get("property_key") == property_name]
        timestamps = [str(row.get("last_checked_at", "")).strip() for row in property_rows if str(row.get("last_checked_at", "")).strip()]
        last_crawled_at = max(timestamps) if timestamps else ""
    run_status_rows.append(
        {
            "property_key": property_name,
            "current_status": str(state.get("current_status", "idle") or "idle").lower(),
            "last_crawled_at": last_crawled_at,
        }
    )

filter_row1_col1, filter_row1_col2, filter_row1_col3, filter_row1_col4 = st.columns([1.0, 1.0, 1.45, 1.2])
filter_row2_col1, filter_row2_col2, filter_row2_col3 = st.columns([1.0, 1.0, 1.2])

with filter_row1_col1:
    selected_property = st.selectbox("Property", options=["All"] + properties, index=0)

with filter_row1_col2:
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
    today_ist = _ist_now().date()
    if today_ist in available_dates:
        default_start = default_end = today_ist
    else:
        default_start = default_end = available_dates[-1]
    date_range_key = "date_range_filter"
    date_range_anchor_key = "date_range_filter_anchor"
    current_anchor = st.session_state.get(date_range_anchor_key)
    desired_anchor = default_end.isoformat()
    if current_anchor != desired_anchor:
        st.session_state[date_range_key] = (default_start, default_end)
        st.session_state[date_range_anchor_key] = desired_anchor
    with filter_row1_col3:
        date_range = st.date_input(
            "Date Range",
            value=st.session_state.get(date_range_key, (default_start, default_end)),
            min_value=available_dates[0],
            max_value=available_dates[-1],
            key=date_range_key,
        )

with filter_row1_col4:
    url_pattern = st.text_input(
        "URL Pattern",
        value="",
        placeholder="/politics/, election, cricket",
        help="Match URL fragments. Use commas to search multiple patterns.",
    )

with filter_row2_col1:
    min_check_count = st.number_input(
        "Min Check Count",
        min_value=0,
        value=0,
        step=1,
    )

with filter_row2_col2:
    latency_threshold = st.number_input(
        "Latency Threshold (min)",
        min_value=0,
        value=5,
        step=1,
    )

with filter_row2_col3:
    st.markdown("<div class='filter-caption'>Latency Filter</div>", unsafe_allow_html=True)
    latency_over_threshold_only = st.checkbox(
        "Show only URLs above the entered threshold",
        value=False,
        help=f"Uses the current Latency Threshold value: {int(latency_threshold)} minutes",
    )

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

if url_pattern.strip():
    patterns = [part.strip().lower() for part in re.split(r"[\n,]+", url_pattern) if part.strip()]
    if patterns:
        base_rows = [
            row
            for row in base_rows
            if any(pattern in str(row.get("url", "")).lower() for pattern in patterns)
        ]

if min_check_count > 0:
    base_rows = [
        row
        for row in base_rows
        if int(row.get("check_count", 0) or 0) >= int(min_check_count)
    ]

if latency_over_threshold_only:
    base_rows = [
        row
        for row in base_rows
        if (_latency_value(row) is not None and _latency_value(row) > float(latency_threshold))
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
    if (_latency_value(row) is not None and _latency_value(row) > float(latency_threshold))
)
late_indexed_pct = (late_indexed_count / indexed_count * 100) if indexed_count else 0.0

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total URLs", total_count)
c2.metric("Indexed", indexed_count)
c3.metric("Not Indexed", not_indexed_count)
c4.metric("Indexed %", f"{indexed_pct:.1f}%")
c5.metric("Errors", error_count)
c6.metric(f"Latency >{int(latency_threshold)}m", late_indexed_count)
c7.metric(f">{int(latency_threshold)}m %", f"{late_indexed_pct:.1f}%")

rows = list(base_rows)
if status_filter != "All":
    rows = [r for r in rows if r.get("current_status") == status_filter]

url_state_tab, run_status_tab = st.tabs(["URL State", "Run Status"])

with url_state_tab:
    st.subheader("URL State")
    if rows:
        df = pd.DataFrame(rows)
        df = _format_timestamp_columns(
            df,
            [
                "sitemap_published_date",
                "first_checked_at",
                "last_checked_at",
                "google_last_crawl_at",
            ],
        )
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
                logs_df = _format_timestamp_columns(
                    pd.DataFrame(logs),
                    ["checked_at", "last_crawl_time", "created_at"],
                )
                st.dataframe(logs_df, width="stretch", hide_index=True)
            else:
                st.info("No checks logged yet for this URL.")
    else:
        st.info("No rows found for the current filter.")

    st.subheader("Recent Logs")
    recent_logs = db.fetch_logs(conn, property_key=property_key, limit=200)
    if recent_logs:
        recent_logs_df = _format_timestamp_columns(
            pd.DataFrame(recent_logs),
            ["checked_at", "last_crawl_time", "created_at"],
        )
        st.dataframe(recent_logs_df, width="stretch", hide_index=True)
    else:
        st.info("No logs yet.")

with run_status_tab:
    st.subheader("Run Status")
    st.caption("Track which property is running right now and when each property last checked a URL.")
    run_rows = list(run_status_rows)
    if property_key:
        run_rows = [row for row in run_rows if row["property_key"] == property_key]
    for row in run_rows:
        row["last_crawled_at_display"] = _format_ist(row.get("last_crawled_at", ""))
        row["last_run_finished_at_display"] = _format_ist(row.get("last_run_finished_at", ""))
        row["next_expected_run"] = _next_expected_run(row.get("property_key", ""))
        row["next_expected_run_display"] = _format_ist(row.get("next_expected_run", ""))
        row["current_status_display"] = str(row.get("current_status", "idle")).title()
    run_rows.sort(
        key=lambda row: (
            0 if row.get("current_status") == "running" else 1,
            row.get("last_crawled_at", "") or "",
            row.get("property_key", ""),
        ),
        reverse=False,
    )
    run_rows.sort(
        key=lambda row: row.get("last_crawled_at", "") or "",
        reverse=True,
    )
    run_rows.sort(key=lambda row: 0 if row.get("current_status") == "running" else 1)

    if run_rows:
        status_df = pd.DataFrame(run_rows)[
            [
                "property_key",
                "current_status_display",
                "last_crawled_at_display",
                "last_run_finished_at_display",
                "next_expected_run_display",
            ]
        ]
        status_df.columns = [
            "Property",
            "Current Status",
            "Last Crawled At",
            "Last Run Finished At",
            "Next Expected Run",
        ]
        st.dataframe(status_df, width="stretch", hide_index=True)
    else:
        st.info("No property run status available yet.")

conn.close()
