from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency for SQLite-only runs
    psycopg = None
    dict_row = None

SQLITE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS url_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    property_key TEXT NOT NULL,
    url TEXT NOT NULL,
    sitemap_published_date TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    first_checked_at TEXT,
    last_checked_at TEXT,
    check_count INTEGER NOT NULL DEFAULT 0,
    current_status TEXT NOT NULL DEFAULT 'Pending',
    first_indexed_seen_at TEXT,
    last_non_indexed_seen_at TEXT,
    estimated_indexed_at TEXT,
    indexing_latency_minutes INTEGER,
    gsc_coverage_state TEXT,
    gsc_page_fetch_state TEXT,
    google_last_crawl_at TEXT,
    next_check_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(property_key, url)
);

CREATE TABLE IF NOT EXISTS check_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_key TEXT NOT NULL,
    url TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    status TEXT NOT NULL,
    verdict TEXT,
    coverage_state TEXT,
    indexing_state TEXT,
    page_fetch_state TEXT,
    robots_state TEXT,
    last_crawl_time TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS property_state (
    property_key TEXT PRIMARY KEY,
    last_sitemap_check_at TEXT,
    gsc_hour_bucket TEXT,
    gsc_checks_this_hour INTEGER NOT NULL DEFAULT 0,
    gsc_quota_backoff_until TEXT,
    current_status TEXT NOT NULL DEFAULT 'idle',
    current_run_started_at TEXT,
    last_run_finished_at TEXT,
    last_crawled_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS login_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    login_date TEXT NOT NULL,
    username TEXT NOT NULL,
    logged_in_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_url_state_property_status ON url_state(property_key, current_status);
CREATE INDEX IF NOT EXISTS idx_url_state_property_published ON url_state(property_key, sitemap_published_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_url_state_property_url_unique ON url_state(property_key, url);
CREATE INDEX IF NOT EXISTS idx_check_log_property_checked ON check_log(property_key, checked_at);
CREATE INDEX IF NOT EXISTS idx_login_events_logged_in_at ON login_events(logged_in_at DESC);
"""

POSTGRES_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS url_state (
        id BIGSERIAL PRIMARY KEY,
        date DATE NOT NULL,
        property_key TEXT NOT NULL,
        url TEXT NOT NULL,
        sitemap_published_date TIMESTAMPTZ NOT NULL,
        discovered_at TIMESTAMPTZ NOT NULL,
        first_checked_at TIMESTAMPTZ,
        last_checked_at TIMESTAMPTZ,
        check_count INTEGER NOT NULL DEFAULT 0,
        current_status TEXT NOT NULL DEFAULT 'Pending',
        first_indexed_seen_at TIMESTAMPTZ,
        last_non_indexed_seen_at TIMESTAMPTZ,
        estimated_indexed_at TIMESTAMPTZ,
        indexing_latency_minutes INTEGER,
        gsc_coverage_state TEXT,
        gsc_page_fetch_state TEXT,
        google_last_crawl_at TIMESTAMPTZ,
        next_check_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS check_log (
        id BIGSERIAL PRIMARY KEY,
        property_key TEXT NOT NULL,
        url TEXT NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        verdict TEXT,
        coverage_state TEXT,
        indexing_state TEXT,
        page_fetch_state TEXT,
        robots_state TEXT,
        last_crawl_time TIMESTAMPTZ,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS property_state (
        property_key TEXT PRIMARY KEY,
        last_sitemap_check_at TIMESTAMPTZ,
        gsc_hour_bucket TIMESTAMPTZ,
        gsc_checks_this_hour INTEGER NOT NULL DEFAULT 0,
        gsc_quota_backoff_until TIMESTAMPTZ,
        current_status TEXT NOT NULL DEFAULT 'idle',
        current_run_started_at TIMESTAMPTZ,
        last_run_finished_at TIMESTAMPTZ,
        last_crawled_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS login_events (
        id BIGSERIAL PRIMARY KEY,
        login_date DATE NOT NULL,
        username TEXT NOT NULL,
        logged_in_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "ALTER TABLE url_state ADD COLUMN IF NOT EXISTS discovered_at TIMESTAMPTZ",
    "ALTER TABLE url_state ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMPTZ",
    "ALTER TABLE url_state ADD COLUMN IF NOT EXISTS google_last_crawl_at TIMESTAMPTZ",
    "ALTER TABLE property_state ADD COLUMN IF NOT EXISTS current_status TEXT NOT NULL DEFAULT 'idle'",
    "ALTER TABLE property_state ADD COLUMN IF NOT EXISTS current_run_started_at TIMESTAMPTZ",
    "ALTER TABLE property_state ADD COLUMN IF NOT EXISTS last_run_finished_at TIMESTAMPTZ",
    "ALTER TABLE property_state ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMPTZ",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_url_state_property_url_unique ON url_state(property_key, url)",
    "CREATE INDEX IF NOT EXISTS idx_url_state_property_status ON url_state(property_key, current_status)",
    "CREATE INDEX IF NOT EXISTS idx_url_state_property_published ON url_state(property_key, sitemap_published_date)",
    "CREATE INDEX IF NOT EXISTS idx_check_log_property_checked ON check_log(property_key, checked_at)",
    "CREATE INDEX IF NOT EXISTS idx_login_events_logged_in_at ON login_events(logged_in_at DESC)",
]

DBConnection = Any


def _is_postgres(conn: DBConnection) -> bool:
    return psycopg is not None and isinstance(conn, psycopg.Connection)


def _placeholder_query(conn: DBConnection, query: str) -> str:
    if _is_postgres(conn):
        return query.replace("?", "%s")
    return query


def _execute(conn: DBConnection, query: str, params: Iterable[Any] | None = None):
    cursor = conn.execute(_placeholder_query(conn, query), tuple(params or ()))
    return cursor


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def _normalize_record(row: Any) -> Dict[str, Any]:
    raw = row if isinstance(row, dict) else dict(row)
    return {key: _normalize_value(value) for key, value in raw.items()}


def _fetchall_dicts(cursor) -> List[Dict[str, Any]]:
    rows = cursor.fetchall()
    if not rows:
        return []
    return [_normalize_record(row) for row in rows]


def _fetchone_dict(cursor) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if not row:
        return None
    return _normalize_record(row)


def _commit(conn: DBConnection) -> None:
    if not _is_postgres(conn):
        conn.commit()


def connect(db_path: str, db_url: str = "") -> DBConnection:
    if db_url.strip():
        if psycopg is None:
            raise RuntimeError("psycopg is required for Supabase/Postgres connections. Add it to requirements first.")
        conn = psycopg.connect(db_url, row_factory=dict_row, autocommit=True)
        return conn

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _ensure_sqlite_column(conn: DBConnection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db(conn: DBConnection) -> None:
    if _is_postgres(conn):
        with conn.cursor() as cur:
            for statement in POSTGRES_SCHEMA_STATEMENTS:
                cur.execute(statement)
        return
    conn.executescript(SQLITE_SCHEMA_SQL)
    _ensure_sqlite_column(conn, "property_state", "current_status", "TEXT NOT NULL DEFAULT 'idle'")
    _ensure_sqlite_column(conn, "property_state", "current_run_started_at", "TEXT")
    _ensure_sqlite_column(conn, "property_state", "last_run_finished_at", "TEXT")
    _ensure_sqlite_column(conn, "property_state", "last_crawled_at", "TEXT")
    conn.commit()


def reset_db(conn: DBConnection) -> None:
    _execute(conn, "DELETE FROM check_log")
    _execute(conn, "DELETE FROM url_state")
    _execute(conn, "DELETE FROM property_state")
    _execute(conn, "DELETE FROM login_events")
    _commit(conn)


def get_property_state(conn: DBConnection, property_key: str) -> Dict[str, Any]:
    row = _fetchone_dict(
        _execute(
            conn,
            "SELECT * FROM property_state WHERE property_key = ?",
            (property_key,),
        )
    )
    if row:
        return row
    return {
        "property_key": property_key,
        "last_sitemap_check_at": "",
        "gsc_hour_bucket": "",
        "gsc_checks_this_hour": 0,
        "gsc_quota_backoff_until": "",
        "current_status": "idle",
        "current_run_started_at": "",
        "last_run_finished_at": "",
        "last_crawled_at": "",
        "updated_at": "",
    }


def upsert_property_state(conn: DBConnection, state: Dict[str, Any]) -> None:
    _execute(
        conn,
        """
        INSERT INTO property_state (
            property_key,
            last_sitemap_check_at,
            gsc_hour_bucket,
            gsc_checks_this_hour,
            gsc_quota_backoff_until,
            current_status,
            current_run_started_at,
            last_run_finished_at,
            last_crawled_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(property_key) DO UPDATE SET
            last_sitemap_check_at=excluded.last_sitemap_check_at,
            gsc_hour_bucket=excluded.gsc_hour_bucket,
            gsc_checks_this_hour=excluded.gsc_checks_this_hour,
            gsc_quota_backoff_until=excluded.gsc_quota_backoff_until,
            current_status=excluded.current_status,
            current_run_started_at=excluded.current_run_started_at,
            last_run_finished_at=excluded.last_run_finished_at,
            last_crawled_at=excluded.last_crawled_at,
            updated_at=excluded.updated_at
        """,
        (
            state.get("property_key", ""),
            state.get("last_sitemap_check_at", "") or None,
            state.get("gsc_hour_bucket", "") or None,
            int(state.get("gsc_checks_this_hour", 0) or 0),
            state.get("gsc_quota_backoff_until", "") or None,
            state.get("current_status", "idle") or "idle",
            state.get("current_run_started_at", "") or None,
            state.get("last_run_finished_at", "") or None,
            state.get("last_crawled_at", "") or None,
            state.get("updated_at", "") or None,
        ),
    )
    _commit(conn)


def upsert_discovered_urls(
    conn: DBConnection,
    property_key: str,
    rows: Iterable[Tuple[str, str, str, str]],
) -> int:
    """Rows: (url, sitemap_published_date, discovered_at, date)"""
    count = 0
    for url, sitemap_published_date, discovered_at, date_value in rows:
        cursor = _execute(
            conn,
            """
            INSERT INTO url_state (
                date, property_key, url, sitemap_published_date, discovered_at,
                current_status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'Pending', ?, ?)
            ON CONFLICT(property_key, url) DO NOTHING
            """,
            (
                date_value,
                property_key,
                url,
                sitemap_published_date,
                discovered_at,
                discovered_at,
                discovered_at,
            ),
        )
        rowcount = getattr(cursor, "rowcount", 0) or 0
        if rowcount > 0:
            count += 1
    _commit(conn)
    return count


def fetch_property_urls(conn: DBConnection, property_key: str) -> List[Dict[str, Any]]:
    return _fetchall_dicts(
        _execute(
            conn,
            "SELECT * FROM url_state WHERE property_key = ? ORDER BY sitemap_published_date DESC, id DESC",
            (property_key,),
        )
    )


def fetch_due_candidates(conn: DBConnection, property_key: str) -> List[Dict[str, Any]]:
    return _fetchall_dicts(
        _execute(
            conn,
            """
            SELECT *
            FROM url_state
            WHERE property_key = ?
              AND current_status != 'Indexed'
            ORDER BY
              CASE WHEN check_count = 0 THEN 0 ELSE 1 END,
              sitemap_published_date DESC,
              id DESC
            """,
            (property_key,),
        )
    )


def update_url_state(conn: DBConnection, row_id: int, fields: Dict[str, Any]) -> None:
    keys = sorted(fields.keys())
    if not keys:
        return
    assignments = ", ".join([f"{key} = ?" for key in keys])
    values = [fields[key] if fields[key] != "" else None for key in keys]
    values.append(row_id)
    _execute(conn, f"UPDATE url_state SET {assignments} WHERE id = ?", values)
    _commit(conn)


def insert_check_log(conn: DBConnection, payload: Dict[str, Any]) -> None:
    _execute(
        conn,
        """
        INSERT INTO check_log (
            property_key, url, checked_at, status, verdict, coverage_state,
            indexing_state, page_fetch_state, robots_state, last_crawl_time,
            error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("property_key", ""),
            payload.get("url", ""),
            payload.get("checked_at", "") or None,
            payload.get("status", ""),
            payload.get("verdict", ""),
            payload.get("coverage_state", ""),
            payload.get("indexing_state", ""),
            payload.get("page_fetch_state", ""),
            payload.get("robots_state", ""),
            payload.get("last_crawl_time", "") or None,
            payload.get("error", ""),
            payload.get("checked_at", "") or None,
        ),
    )
    _commit(conn)


def fetch_all_summary(conn: DBConnection, property_key: Optional[str] = None) -> List[Dict[str, Any]]:
    if property_key:
        return _fetchall_dicts(
            _execute(
                conn,
                "SELECT * FROM url_state WHERE property_key = ? ORDER BY sitemap_published_date DESC, id DESC",
                (property_key,),
            )
        )
    return _fetchall_dicts(
        _execute(
            conn,
            "SELECT * FROM url_state ORDER BY property_key ASC, sitemap_published_date DESC, id DESC",
        )
    )


def fetch_property_states(conn: DBConnection) -> List[Dict[str, Any]]:
    return _fetchall_dicts(
        _execute(
            conn,
            "SELECT * FROM property_state ORDER BY property_key ASC",
        )
    )


def fetch_logs(
    conn: DBConnection,
    property_key: Optional[str] = None,
    url: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    query = "SELECT * FROM check_log WHERE 1=1"
    params: List[Any] = []
    if property_key:
        query += " AND property_key = ?"
        params.append(property_key)
    if url:
        query += " AND url = ?"
        params.append(url)
    query += " ORDER BY checked_at DESC, id DESC LIMIT ?"
    params.append(limit)
    return _fetchall_dicts(_execute(conn, query, params))


def insert_login_event(
    conn: DBConnection,
    login_date: str,
    username: str,
    logged_in_at: str,
) -> None:
    _execute(
        conn,
        """
        INSERT INTO login_events (
            login_date,
            username,
            logged_in_at,
            created_at
        ) VALUES (?, ?, ?, ?)
        """,
        (
            login_date,
            username,
            logged_in_at,
            logged_in_at,
        ),
    )
    _commit(conn)


def fetch_login_events(conn: DBConnection, limit: int = 200) -> List[Dict[str, Any]]:
    return _fetchall_dicts(
        _execute(
            conn,
            """
            SELECT login_date AS date, username, logged_in_at
            FROM login_events
            ORDER BY logged_in_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
    )


def summary_counts(conn: DBConnection, property_key: Optional[str] = None) -> Dict[str, int]:
    params: List[Any] = []
    where = ""
    if property_key:
        where = "WHERE property_key = ?"
        params.append(property_key)

    total_row = _fetchone_dict(_execute(conn, f"SELECT COUNT(*) AS count FROM url_state {where}", params))
    indexed_row = _fetchone_dict(
        _execute(
            conn,
            f"SELECT COUNT(*) AS count FROM url_state {where + (' AND' if where else ' WHERE')} current_status = 'Indexed'",
            params,
        )
    )
    pending_row = _fetchone_dict(
        _execute(
            conn,
            f"SELECT COUNT(*) AS count FROM url_state {where + (' AND' if where else ' WHERE')} current_status = 'Pending'",
            params,
        )
    )
    errors_row = _fetchone_dict(
        _execute(
            conn,
            f"SELECT COUNT(*) AS count FROM url_state {where + (' AND' if where else ' WHERE')} current_status IN ('Error', 'Quota Exceeded')",
            params,
        )
    )

    return {
        "total": int((total_row or {}).get("count", 0) or 0),
        "indexed": int((indexed_row or {}).get("count", 0) or 0),
        "pending": int((pending_row or {}).get("count", 0) or 0),
        "errors": int((errors_row or {}).get("count", 0) or 0),
    }
